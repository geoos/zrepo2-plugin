import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import fetch from "node-fetch";
import {DateTime} from "luxon";

const URLDescarga = "https://rt.ambientweather.net/v1/devices?apiKey=7712e7e6c7464bee8add6bbc83d493d1b999ed19c684474d8f2f9316325225fd&applicationKey=8ecae3ed4c4e407d80535d2dee6ac671a7502d92bd9b4b57b4dde2e6a303719f&lastData=true";

const varMapping = {
    //"tempinf": 92.3,
    //"humidityin": 49,
    //"baromrelin": 29.888,
    //"baromabsin": 29.814,
    "tempf": {target: "rie.temp", transform: x => ((x -32) * 5/9)},
    //"battout": 1,
    "humidity": {target: "rie.humedad"},
    "winddir": {target: "rie.dir_viento"},
    "windspeedmph": {target: "rie.vel_media_viento", transform: x => (x * 0.868976)},
    //"windgustmph": 11.4,
    //"maxdailygust": 12.5,
    //"hourlyrainin": 0,
    //"eventrainin": 0,
    //"dailyrainin": 0,
    //"weeklyrainin": 0,
    //"monthlyrainin": 0.165,
    //"totalrainin": 236.22,
    //"solarradiation": 504.51,
    //"uv": 4,
    //"batt_co2": 1,
    //"feelsLike": 82.77,
    //"dewPoint": 67.79,
    //"feelsLikein": 98.6,
    //"dewPointin": 70.3,
    //"lastRain": "2024-06-19T06:27:00.000Z"
}

class ImportadorUFPR extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-ufpr"}
    get name() {return "Importar datos de Estaciones UFPR"}
    get description() {return "Importar datos de Estaciones UFPR"}

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
            
            // Chequear que exista el proveedor "ufpr"
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "ufpr");
            if (!row) {
                await this.addLog("I", "Creando Proveedor UFPR");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"ufpr", name:"UFPR"});
            }

            // Chequear que existan region: BR, provincia: BR, comuna: BR para asociar las estaciones
            row = await ZRepoClient.getValorDimension("ine.region", "BR");
            if (!row) {
                await ZRepoClient.setFilaDimension("ine.region", {code:"BR", name:"Brasil"});
            }
            row = await ZRepoClient.getValorDimension("ine.provincia", "BR");
            if (!row) {
                await ZRepoClient.setFilaDimension("ine.provincia", {code:"BR", name:"Brasil", region:"BR"});
            }
            row = await ZRepoClient.getValorDimension("ine.comuna", "BR");
            if (!row) {
                await ZRepoClient.setFilaDimension("ine.comuna", {code:"BR", name:"Brasil", provincia: "BR"});
            }

            let estacionesActuales = await ZRepoClient.getValores("rie.estacion", null, {proveedor:"ufpr"});
            estacionesActuales = estacionesActuales.filter(e => {
                if (e.activa === false) return false;
                return true;
            })            
            let estado = await this.getData();
            if (!estado) estado = {};

            // Descargar json
            let estaciones;
            try {
                let ret = await fetch(URLDescarga);
                console.log("ret", ret.status, ret.statusText);                
                if (ret.status != 200) {
                    let msg = "";
                    try {
                        msg = await ret.text();
                    } catch (error) {                    
                    }
                    throw "Error descargando datos: [" + ret.status + "]: " + ret.statusText + ": " + msg;
                }
                estaciones = await ret.json();
            } catch (error) {
                throw "Error en descarga: " + error.toString();
            }

            for (let estacion of estaciones) {
                let code = estacion.info.name
                let found = estacionesActuales.find(e => e.code == code);
                if (!found) {
                    let name = estacion.info.name;
                    let lat = estacion.info.coords.coords.lat, lng = estacion.info.coords.coords.lon;
                    await ZRepoClient.setFilaDimension("rie.estacion", {
                        code, name, comuna: "BR", proveedor: "ufpr", tipo:"meteo",
                        lat, lng,
                        variables:[ "rie.temp", "rie.humedad", "rie.punto_rocio", "rie.vel_media_viento", "rie.dir_viento",
                                    "rie.presion_atm", "rie.sens_termica", "rie.indice_calor", "rie.indice_uv", "rie.ppt_hora",
                                    "rie.ppt_dia_utc"
                        ]
                    });
                    await this.addLog("I", "Creada la estación " + code);
                }
                let data = estacion.lastData;
                let time = data.dateutc;
                let ultimaCapturaEstacion = estado[code] || 0;
                if (time > ultimaCapturaEstacion) {
                    let n = 0;
                    estado[code] = time;
                    for (let varName of Object.keys(data)) {
                        let zrepoVar = varMapping[varName];
                        if (zrepoVar) {
                            let value = data[varName];
                            if (zrepoVar.transform) value = zrepoVar.transform(value);
                            await ZRepoClient.postVar(zrepoVar.target, time, value, {estacion:code}, {});
                            n++;
                        }
                    }
                    await this.addLog("I", code + ": Importados valores para " + n + " variables");
                } else {
                    await this.addLog("I", code + ": No hay datos nuevos");
                }
            }
            
            await this.setData(estado);
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

}
export {ImportadorUFPR};