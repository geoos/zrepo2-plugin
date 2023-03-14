import { ZRepoProcess } from "../ZRepoPluginClient.js";
import fs from "fs";
import geoserver from "../GEOServerUtils.js";
import * as  Hjson from "hjson";
import moment from "moment-timezone";
import motuClient from "../MOTUClient.js";

class ImportadorCopernicus extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "copernicus"}
    get name() {return "Importador Productos Copernicus"}
    get description() {return "Importador Productos Copernicus"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación de Productos de Copernicus");
            let pis = await this.getRunningInstances();
            if (pis.length) {
                let p0 = pis[0];
                let m = moment.tz(p0.started, tz);
                await this.setDescription("Otra Instancia en ejecución. Se Descarta");
                await this.addLog("W", "Se encontró instancia en ejecución desde " + m.format("YYYY-MM-DD HH:mm"));
                await this.finishProcess();
                return;
            }

            // Leer configuración
            let downloaderConfig = fs.readFileSync(geoserver.configPath + "/copernicus-downloader.hjson").toString("utf8");
            try {
                downloaderConfig = Hjson.parse(downloaderConfig);
            } catch (error) {
                await this.addLog("E", "Error interpretando configuración de productos copernicus: " + error.toString());
                await this.finishProcess();
            }

            //console.log("config", downloaderConfig);

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
                await motuClient.download(product, fmtDownloadDate, outDir, outName + ".nc", config);
                await this.addLog("I", "    => Descargado");
                fs.renameSync(outDir + "/" + outName + ".nc", geoserver.importPath + "/" + outName + ".nc")
                this.state[product.dataSetCode] = fmtDownloadDate;                
            } catch(error) {
                //await this.addLog("E", error.toString());
                //console.error(error);
                throw error;
            }
        } catch(error) {
            throw error;
        }
    }
}
export {ImportadorCopernicus};