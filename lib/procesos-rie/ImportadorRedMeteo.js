import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import fetch from "node-fetch";
import moment from "moment-timezone";
import {DateTime} from "luxon";

const angulosPorDireccion = {
    N:0, NNE:25, NE:45, ENE:70, E:90, ESE:110, SE:135, SSE:160, S:180, SSO:200, SO:225, OSO:250, O:270, ONO:290, NO:315, NNO:340
}
const tz = "America/Santiago";

class ImportadorRedMeteo extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-redmeteo"}
    get name() {return "Importar datos de Estaciones Red Meteo-Aficionados"}
    get description() {return "Importar datos de Estaciones Red Meteo-Aficionados"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación");
            let pis = await this.getRunningInstances();
            let i=0;
            while (i < pis.length) {
                let pi = pis[i];
                let idleMinutes = pi.idleTime / 1000 / 60;
                await this.addLog("W", "Se encontró otra instancia en ejecución sin actividad por " + idleMinutes.toFixed(2) + " [min]");
                // Marcar como finalizados los procesos con más de 30 minutos de inactividad
                if (idleMinutes > 30) {
                    let lx = DateTime.fromMillis(pi.started, tz);
                    await this.addLog("W", "Se marca como finalizado proceso iniciado " + lx.toFormat("yyyy-LL-dd hh:mm:ss") + " por inactividad > 30 minutos");
                    await this.finishProcessInstance(pi.instanceId, "Se marca como finalizado por inactividad > 30 minutos");
                    pis.splice(i,1);
                } else {
                    i++;
                }
            }
            if (pis.length) throw "Se encontró otra instancia de este proceso activa (inactividad menor a 30 minutos). Se cancela esta ejecución";

            // Chequear que exista el proveedor "ceaza"
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "redmeteo");
            if (!row) {
                await this.addLog("I", "Creando Proveedor RedMeteo");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"redmeteo", name:"Red Metereológica Aficionados"});
            }

            let estaciones = await ZRepoClient.getValores("rie.estacion", null, {proveedor:"redmeteo"});
            estaciones = estaciones.filter(e => {
                if (e.activa === false) return false;
                return true;
            })
            if (!estaciones.length) {
                await this.addLog("E", "No se encontraron estaciones para el proveedor 'redmeteo");
                await this.finishProcess();
                return;
            }
            let estado = await this.getData();
            
            let telemetria = await this.downloadFile("http://redmeteo.cl/networktelemetry.txt");
            let filas = telemetria.split("\n");
            await this.addLog("I", "---- Procesando datos para " + filas.length + " estaciones Reportadas");
            for (let i=0; i<filas.length; i++) {
                let cols = filas[i].split(",");
                if (cols.length < 18 || i < 2) continue;
                let codigoEstacion = cols[0];
                codigoEstacion = codigoEstacion.substr(1);
                codigoEstacion = codigoEstacion.substr(0, codigoEstacion.length - 1);
                await this.addLog("I", "Estación " + codigoEstacion);
                let e = estaciones.find(x => x.code == codigoEstacion);
                if (!e) {
                    await this.addLog("E", "  => La estación no está en la lista de ZRepo");
                    continue;
                }
                e.reportada = true;
                let time = this.parseTime(cols[1], cols[2], e.ZonaHoraria);
                if (!time) continue;
                if (estado[codigoEstacion] == time) continue;                
                let unidades = cols[18].split("|");
                if (unidades.length != 4) continue;
                let [uTemp, uViento, uPresion, uPrecipitacion] = unidades;
                let temp = parseFloat(cols[3]);
                let dsRow = {codigo_estacion:codigoEstacion, time:time}
                if (!isNaN(temp)) {
                    temp = this.normalizaTemp(temp, uTemp);
                    if (!isNaN(temp)) dsRow.temp = temp;
                }
                let presion = parseFloat(cols[8]);
                if (!isNaN(presion)) {
                    presion = this.normalizaPresion(presion, uPresion);
                    if (!isNaN(presion)) dsRow.presion_atm = presion;
                }
                let sens = parseFloat(cols[5]);
                if (!isNaN(sens)) {
                    sens = this.normalizaTemp(sens, uTemp);
                    if (!isNaN(sens)) dsRow.sens_termica = sens;
                }
                let humedad = parseFloat(cols[6]);
                if (!isNaN(humedad)) dsRow.humedad = humedad;
                let rocio = parseFloat(cols[7]);
                if (!isNaN(rocio)) {
                    rocio = this.normalizaTemp(rocio, uTemp);
                    if (!isNaN(rocio)) dsRow.punto_rocio = rocio;
                }
                let velViento = parseFloat(cols[10]);
                if (!isNaN(velViento)) {
                    velViento = this.normalizaViento(velViento, uViento);
                    if (!isNaN(velViento)) dsRow.vel_media_viento = velViento;
                }
                let rachaViento = parseFloat(cols[17]);
                if (!isNaN(rachaViento)) {
                    rachaViento = this.normalizaViento(rachaViento, uViento);
                    if (!isNaN(rachaViento)) dsRow.racha_viento = rachaViento;
                }
                let dirViento = cols[11];
                dirViento = angulosPorDireccion[dirViento];
                if (!isNaN(dirViento)) dsRow.dir_viento = dirViento;
                let precip = parseFloat(cols[12]);
                if (!isNaN(precip)) dsRow.ppt_acum_dia = precip;

                await ZRepoClient.postDataSet("rie.redmeteo", dsRow);
                //console.log("post rie.redmeteo", dsRow)
                estado[codigoEstacion] = time;
            }

            await this.setData(estado);
            let noReportadas = estaciones.filter(e => !e.reportada);
            if (noReportadas.length) {
                await this.addLog("W", "No hay reporte para las siguientes estaciones:" + JSON.stringify(noReportadas.map(e => (e.code)), null, 4));
            }
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

    parseTime(stHH, stDia, timeZone) {
        let fields = stHH.split(":");
        if (fields.length != 2) return null;
        let hh = parseInt(fields[0]);
        let mm = parseInt(fields[1]);
        if (isNaN(hh) || isNaN(mm)) return null;
        fields = stDia.split("/");
        if (fields.length != 3) return null;
        let dd = parseInt(fields[0]);
        let MM = parseInt(fields[1]);
        let yyyy = parseInt(fields[2]);
        if (isNaN(dd) || isNaN(MM) || isNaN(yyyy)) return null;
        let t = moment.tz(timeZone);
        t.year(yyyy);t.month(MM - 1);t.date(dd);
        t.hour(hh);t.minute(MM);t.second(0);t.millisecond(0);
        return t.valueOf();
    }

    normalizaTemp(v, u) {
        switch(u) {
            case "C": return v;
            case "F": return (v - 32) * 5/9;
            case "K": return v - 273.15;
            default: {
                console.warn("Unidad de Temperatura '" + u + "' no manejada");
                return null;
            }
        }
    }
    normalizaPresion(v, u) {
        switch(u) {
            case "hPa": return v;
            case "mb": return v;
            case "in": return v * 33.8639;
            default: {
                console.warn("Unidad de Presión '" + u + "' no manejada");
                return null;
            }
        }
    }
    normalizaViento(v, u) {
        switch(u) {
            case "kts": return v;
            case "km/h": return v / 1.852;
            case "m/s": return v * 1.94384;
            case "mph": return v / 1.151;
            default: {
                console.warn("Unidad de Velocidad '" + u + "' no manejada");
                return null;
            }
        }
    }
}
export {ImportadorRedMeteo};