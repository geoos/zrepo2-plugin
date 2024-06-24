import { ZRepoProcess } from "../ZRepoPluginClient.js";
import fs from "fs";
import geoserver from "../GEOServerUtils.js";
import * as  Hjson from "hjson";
import moment from "moment-timezone";
import { DateTime } from "luxon";

class ImportadorSentinel extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "sentinel"}
    get name() {return "Importador Productos Sentinel"}
    get description() {return "Importador Productos Sentinel"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación de Productos de Sentinel");
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

            // Leer configuración
            let downloaderConfig = fs.readFileSync(geoserver.configPath + "/sentinel-downloader.hjson").toString("utf8");
            try {
                downloaderConfig = Hjson.parse(downloaderConfig);
            } catch (error) {
                await this.addLog("E", "Error interpretando configuración de productos sentinel: " + error.toString());
                await this.finishProcess();
            }

            console.log("config", downloaderConfig);

            this.state = await this.getData();
            let now = moment.tz("UTC");
            
            for (let i=0; i<downloaderConfig.products.length; i++) {
                let nTries = 0, success = false;
                do {
                    try {                        
                        await this.downloadProduct(downloaderConfig.products[i], downloaderConfig, now);
                        success = true;
                        await this.setData(this.state);
                    } catch(error) {
                        // probar con un dia anterior antes de descartar
                        let yesterday = now.clone();
                        yesterday.date(now.date() - 1);
                        try {
                            await this.addLog("W", "  => El producto no se encontró para el día solicitado. Intentando para el día anterior");
                            await this.downloadProduct(downloaderConfig.products[i], downloaderConfig, yesterday);
                            success = true;
                            await this.setData(this.state);
                        } catch(error) {
                            console.error(error);
                            await this.addLog("E", error.toString());
                            success = false;    
                        }
                        await this.checkCanceled();
                    }
                } while (!success && ++nTries < 1);
            }

            await this.addLog("I", "Importación Finalizada");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    async downloadProduct(product, config, time) {
        try {
            let downloadDate = time.clone().subtract(1, "days").startOf("day");
            let fmtDownloadDate = downloadDate.format(product.publishDateFormat);
            let outName =  product.dataSetCode + "_" + downloadDate.format("YYYY-MM-DD");
            if (this.state[product.dataSetCode] == fmtDownloadDate) return;
            await this.addLog("I", "Buscando producto: " + product.dataSetCode + " [" + fmtDownloadDate + "]");
            let outDir = geoserver.downloadPath;
            try {
                // Descargar
                await this.addLog("I", "    => Descargado");
                //fs.renameSync(outDir + "/" + outName + ".nc", geoserver.importPath + "/" + outName + ".nc")
                //this.state[product.dataSetCode] = fmtDownloadDate;                
            } catch(error) {
                //await this.addLog("E", error.toString());
                console.error(error);
                throw error;
            }
        } catch(error) {
            throw error;
        }
    }
}
export {ImportadorSentinel};