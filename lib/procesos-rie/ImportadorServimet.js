import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import fetch from "node-fetch";
import moment from "moment-timezone";
import {DateTime} from "luxon";

const tz = "America/Santiago";
const varMapping = {
    "rie.temp":3, "rie.humedad":4, "rie.punto_rocio":5, "rie.vel_media_viento":6, "rie.dir_viento":8,
    // rie.precip_dia:10
    "rie.presion_atm":11,
    // rie.precip_ano:21
    "rie.sens_termica":25, "rie.indice_calor":42, "rie.indice_uv":44
}

class ImportadorServimet extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-servimet"}
    get name() {return "Importar datos de Estaciones SERVIMET"}
    get description() {return "Importar datos de Estaciones SERVIMET"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación");
            // Chequear otras instancias en ejecución
            let pis = await this.getRunningInstances();
            let elimino = false;
            for (let pi of pis) {
                // Marcar como finalizados los procesos con más de 5 minutos de inactividad
                let idleMinutes = pi.idleTime / 1000 / 60;
                await this.addLog("W", "Se encontró otra instancia en ejecución sin actividad por " + idleMinutes.toFixed(2) + " [min]");
                if (idleMinutes > 5) {
                    let lx = DateTime.fromMillis(pi.started, {zone: tz});
                    await this.addLog("W", "Se marca como finalizado proceso iniciado " + lx.toFormat("yyyy-LL-dd hh:mm:ss") + " por inactividad > 5 minutos");
                    await this.finishProcessInstance(pi.instanceId, "Se marca como finalizado por inactividad > 5 minutos [" + idleMinutes.toFixed(2) + "]");
                    elimino = true;
                }
            }
            if (elimino) {
                pis = await this.getRunningInstances();
            }
            if (pis.length) throw "Ya hay otra instancia en ejecución. Se descarta esta búsqueda";
            // Chequear que exista el proveedor "servimet"
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "servimet");
            if (!row) {
                await this.addLog("I", "Creando Proveedor Servimet");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"servimet", name:"Servimet"});
            }

            let estaciones = await ZRepoClient.getValores("rie.estacion", null, {proveedor:"servimet"});
            estaciones = estaciones.filter(e => {
                if (e.activa === false) return false;
                return true;
            })
            if (!estaciones.length) {
                await this.addLog("E", "No se encontraron estaciones para el proveedor 'servimet");
                await this.finishProcess();
                return;
            }
            await this.addLog("I", "---- Buscando datos para " + estaciones.length + " estaciones de SERVIMET");
            let estado = await this.getData();
            for (let e of estaciones) {
                await this.addLog("I", "[" + e.code + "] " + e.name);
                if (!estado[e.code] || !isNaN(estado[e.code])) estado[e.code] = {tiempo:0, pptAcum:0};
                let tiempoEstacion = estado[e.code].tiempo || 0;
                let url = "http://web.directemar.cl/met/jturno/estaciones/" + e.code + "/realtime.txt";
                let data;
                try {
                    data = await this.downloadFile(url);
                } catch(error) {
                    //console.error("Error descargando:", url);
                    await this.addLog("E", "Error descargando desde " + url);
                    await this.addLog("E", error.toString());
                    data = null;
                }
                if (data) {
                    let fields = data.split(" ");
                    let time = moment.tz(fields[0] + " " + fields[1], "DD-MM-YY HH:mm:ss", tz);
                    if (time.isValid() && time.valueOf() > tiempoEstacion) {
                        await this.addLog("I", "    => Hay datos nuevos para " + fields[0] + " " + fields[1]);
                        estado[e.code].tiempo = time.valueOf();
                        let dsRow = {time:time.valueOf(), codigo_estacion:e.code}
                        for (let v of e.variables) {
                            let idx = varMapping[v];
                            if (idx === undefined) {
                                //logs.warn("Variable " + v + " has no index in Servimet Downloader");
                            } else {
                                let value = parseFloat(fields[idx - 1]);
                                if (!isNaN(value)) {
                                    if (v == "vel_media_viento") value = value * 1.94384; // m/s => kts
                                    // quitar "rie." del nombre de la variable para usar como nombre de la columna del dataSet
                                    dsRow[v.substr(4)] = value;
                                }                            
                            }
                        }
                        // Agregar precipitaciones
                        let pptAcumDia = parseFloat(fields[9]);
                        let pptAcumMes = parseFloat(fields[19]);
                        let pptAcumAnual = parseFloat(fields[20]);
                        let pptAyer = parseFloat(fields[21]);
                        dsRow.ppt_acum_dia = pptAcumDia;
                        dsRow.ppt_acum_mes = pptAcumMes;
                        dsRow.ppt_acum_ano = pptAcumAnual;
                        dsRow.ppt_acum_ayer = pptAyer;
                        try {
                            await ZRepoClient.postDataSet("rie.servimet", dsRow);
                        } catch(error) {
                            console.error("Error posting dataset", error);
                            await this.addLog("E", "Error posting to DataSet. " + error.toString());
                        }
                    } else {
                        try {
                            let ultimo = moment.tz(tiempoEstacion, tz);
                            await this.addLog("I", "    => No hay datos nuevos para " + fields[0] + " " + fields[1]);
                            await this.addLog("I", "    => Ultimo registro para " + ultimo.format("DD-MM-YY HH:mm:ss"));
                        } catch(error) {
                            logs.error(error.toString())
                        }
                    }
                }
            }

            await this.setData(estado);
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    downloadFile(url) {
        return new Promise((resolve, reject) => {
            fetch(url)
                .then(res => {
                    if (res.status != 200) {
                        res.text()
                            .then(txt => reject(txt))
                            .catch(_ => reject(res.statusText))
                        return;
                    }
                    res.text()
                        .then(txt => {resolve(txt)})
                        .catch(err => {reject(err)})
                })
                .catch(err => {
                    reject(err.name == "AbortError"?"aborted":err)
                });
        })        
    }
}
export {ImportadorServimet};