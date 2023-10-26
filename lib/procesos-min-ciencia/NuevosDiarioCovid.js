import { ZRepoProcess } from "../ZRepoPluginClient.js";
import zRepo from "../ZRepoClient.js";
import fetch from "node-fetch";
import TimeUtils from "../TimeUtils.js";
import { DateTime } from "luxon";

const tz = "America/Santiago";

class NuevosDiarioCovid extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "nuevos-diario-covid"}
    get name() {return "Carga N° Casos Nuevos Diarios COVID19"}
    get description() {return "Carga N° Casos Nuevos Nacionales Diarios COVID19"}

    async exec(params) {
        try {
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

            let data = await this.getData();
            if (!data.fromTime) {
                data.fromTime = 0;
                await this.addLog("W", "Comenzando importación desde el inicio de los tiempos");
            } else {
                let lx = TimeUtils.msToLx(data.fromTime, tz);
                await this.addLog("I", "Buscando datos posteriores a " + lx.toFormat("dd/LL/yyyy"));
            }
            //data.fromTime = 0;
            await this.addLog("I", "Eliminando datos anteriores ...");
            await zRepo.deletePeriod("cie.casos_nuevos", data.fromTime + 24 * 60 * 60 * 1000, Date.now() + 24 * 60 * 60 * 1000, true, true);
            let maxTime = data.fromTime;
            await this.addLog("I", "Buscando datos publicados ...");
            let csv = await (await fetch("https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto13/CasosNuevosCumulativo_T.csv")).text();
            // Region,Codigo region,Comuna,Codigo comuna,Poblacion,Fecha,Casos confirmados
            let filas = csv.split("\n");
            await this.addLog("I", "Procesando datos [" + (filas.length - 1) + " filas] ...");
            let n = 0;
            let lastFecha = 0;
            const regiones = ["15", "1", "2", "3", "4", "5", "13", "6", "7", "16", "8", "9", "14", "10", "11", "12"];
            for (let i=1; i<filas.length; i++) {
                let campos = filas[i].split(",");
                if (campos.length < 18 ) continue;
                let stFecha = campos[0];
                let msFecha = TimeUtils.stToLx(stFecha, "yyyy-LL-dd", tz).valueOf();
                if (msFecha != lastFecha) {
                    await this.addLog("I", "[" + i + "/" + (filas.length - 1) + "] " + stFecha);
                    lastFecha = msFecha;
                }                
                if (msFecha > data.fromTime) {
                    if (msFecha > maxTime) maxTime = msFecha;
                    for (let reg=1; reg <= 16; reg++) {
                        let casos = parseInt(campos[reg]);
                        if (!isNaN(casos)) {
                            n++;
                            if (!(n % 1000)) {
                                await this.addLog("I", "[" + n + ": " + i + "/" + (filas.length - 1) + "]" + stFecha);
                            }
                            await zRepo.postVar("cie.casos_nuevos", msFecha, casos, {region:regiones[reg-1]});
                        }
                    }
                }
                if (!(i % 100)) await this.checkCanceled();
            }
            data.fromTime = maxTime;
            await this.setData(data);
            await this.addLog("I", "Agregados " + n + " registros");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }
}

export {NuevosDiarioCovid};