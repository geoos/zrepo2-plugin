import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import fetch from "node-fetch";
import moment from "moment-timezone";
import * as turf from "@turf/turf";
import {DateTime} from "luxon";

const tz = "America/Santiago";
const email = process.env.EMA_EMAIL || "otrojotaj@gmail.com"
const apiKey = process.env.EMA_EMAIL || "599ce47c4163276bf35173c9"

const variablesRIE = {
    temperatura:"rie.temp",
    puntoDeRocio:"rie.punto_rocio",
    humedadRelativa:"rie.humedad",
    presionNivelDelMar:"rie.presion_atm",
    direccionDelViento:"rie.dir_viento",
    fuerzaDelViento:"rie.vel_media_viento"
}

class ImportadorEMA extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-redema"}
    get name() {return "Importar datos de Estaciones Red EMA"}
    get description() {return "Importar datos de Estaciones Red EMA"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación");
            // Chequear otras instancias en ejecución
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
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "redema");
            if (!row) {
                await this.addLog("I", "Creando Proveedor Red EMA");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"redema", name:"Red EMA"});
            }

            let estaciones = await ZRepoClient.getValores("rie.estacion", null, {proveedor:"redema"});
            estaciones = estaciones.filter(e => {
                if (e.activa === false) return false;
                return true;
            })
            if (!estaciones.length) {
                await this.addLog("E", "No se encontraron estaciones para el proveedor 'redema");
                await this.finishProcess();
                return;
            }
            await this.addLog("I", "---- Buscando datos para " + estaciones.length + " estaciones de la Red EMA");
            let estado = await this.getData();
            if (!estado.estaciones) estado.estaciones = {};
            //let txtDatosRecientes = await this.downloadFile("https://climatologia.meteochile.gob.cl/application/productos/datosRecientesRedEma");
            let txtDatosRecientes = await this.downloadFile(`https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesRedEma?usuario=${email}&token=${apiKey}`);
            let datosRecientes;
            try {
                datosRecientes = JSON.parse(txtDatosRecientes);
            } catch(error) {
                await this.addLog("E", "Descarga inválida. No es JSON")
                await this.finishProcess();
                return;
            }
            //console.log("datosRecientes", JSON.stringify(datosRecientes, null, 4));
            let msCreacion = moment.utc(datosRecientes.fechaCreacion, "DD-MM-YYYY HH:mm:ss").valueOf();
            if (estado.time && msCreacion <= estado.time) {
                await this.addLog("I", "No hay datos nuevos");
                await this.finishProcess();
                return;
            }
            estado.time = msCreacion;
            let jEstaciones = datosRecientes.datosEstaciones;
            let nuevasEstaciones = [];

            await this.addLog("I", "Buscando datos para " + jEstaciones.length + " estaciones de la Red EMA");

            for (let i=0; i<jEstaciones.length; i++) {
                let jEstacion = jEstaciones[i].estacion;
                let codigoEstacion = "EMA-" + jEstacion.codigoNacional;
                let estacionExiste = false;
                for (let e of estaciones) {
                    if (e.code == codigoEstacion) {
                        estacionExiste = true;
                        break;
                    }
                }
                await this.addLog("I", codigoEstacion);
                let jDatos = jEstaciones[i].datos || [];
                //console.log(JSON.stringify(jDatos, null, 4))
                let ultimoTiempoEstacion = estado.estaciones[codigoEstacion];
                if (!ultimoTiempoEstacion) ultimoTiempoEstacion = 0;
                let nuevoTiempoEstacion = 0;
                for (let jMuestra of jDatos) {
                    let tiempoMuestra = moment.utc(jMuestra.momento, "YYYY-MM-DD HH:mm:ss").valueOf(); //  this.parseFechaHoraUTC(jMuestra.momento);
                    if (!ultimoTiempoEstacion || tiempoMuestra > ultimoTiempoEstacion) {
                        if (!nuevoTiempoEstacion || tiempoMuestra > nuevoTiempoEstacion) nuevoTiempoEstacion = tiempoMuestra;
                        let variables = {}, varsRie = [];
                        let dataSetRow = {tiempo:tiempoMuestra, estacion:codigoEstacion}
                        Object.keys(jMuestra).forEach(v => {
                            let value = jMuestra[v];
                            if (variablesRIE[v]) {
                                varsRie.push(variablesRIE[v]);
                                if (value != null) {
                                    let parsed = this.parseVariable(v, value);
                                    if (parsed !== null) {
                                        variables[v] = parsed;
                                        dataSetRow[v] = parsed;
                                    }
                                }
                            }
                        });
                        // Agregar precipitaciones
                        if (jMuestra.aguaCaidaDelMinuto) dataSetRow.ppt_minuto = this.parseVariable("aguaCaidaDelMinuto", jMuestra.aguaCaidaDelMinuto);
                        if (jMuestra.aguaCaida6Horas) dataSetRow.ppt_6_horas = this.parseVariable("aguaCaida6Horas", jMuestra.aguaCaida6Horas);
                        if (jMuestra.aguaCaida24Horas) dataSetRow.ppt_24_horas = this.parseVariable("aguaCaida24Horas", jMuestra.aguaCaida24Horas);
                        // Insertar fila en dataset
                        if (Object.keys(dataSetRow).length > 2) {
                            await ZRepoClient.postDataSet("rie.datosRecientesEMA", dataSetRow);
                            //console.log("post", dataSetRow);
                        }
                        if (!estacionExiste) {
                            await this.addLog("W", "La estación '" + codigoEstacion + "' no existe. Se creará automáticamente");
                            // Crear estación como dimension ZRepo - Se graba en archivo json para importar
                            // {code:"", name:"" }
                            let e = {
                                code:codigoEstacion, name:jEstacion.nombreEstacion,
                                lat:parseFloat(jEstacion.latitud), lng:parseFloat(jEstacion.longitud),
                                proveedor:"redema", tipo:"meteo",
                                variables:varsRie
                            };
                            nuevasEstaciones.push(e);
                            estacionExiste = true;
                        }
                    }
                }
                if (nuevoTiempoEstacion) estado.estaciones[codigoEstacion] = nuevoTiempoEstacion;                
            }
            await this.setData(estado);

            if (nuevasEstaciones && nuevasEstaciones.length) {
                await this.addLog("W", "Nuevas estaciones para agregar: " + nuevasEstaciones.length);
                let stComunas = await this.downloadFile("https://geoserver.geoos.org/ine-regional/comunas/geoJson");
                let comunas = JSON.parse(stComunas).geoJson.features;
                for (let e of nuevasEstaciones) {
                    await this.addLog("I", "Buscando comuna de " + e.code);
                    let found = "000";
                    for (let comuna of comunas) {
                        if (turf.inside([e.lng, e.lat], comuna)) {
                            console.log(e.name + " esta en la comuma " + comuna.properties.name);                            
                            found = comuna.properties.id;
                            break;
                        } 
                    }
                    e.comuna = found;
                    await this.addLog("I", "  => " + found);
                    await ZRepoClient.setFilaDimension("rie.estacion", e);
                }
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

    parseVariable(name, stValue) {
        // stValue = "999.9 unit"
        let idx = stValue.indexOf(" ");
        if (idx <= 0) return null;
        let v = parseFloat(stValue.substr(0,idx));
        if (isNaN(v)) return null;
        //console.log(name + " = " + v + " [" + unit + "]");
        return v;
    }
}
export {ImportadorEMA};