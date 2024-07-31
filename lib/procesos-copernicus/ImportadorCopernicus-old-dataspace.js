import { ZRepoProcess } from "../ZRepoPluginClient.js";
import fs from "fs";
import geoserver from "../GEOServerUtils.js";
import geoServerUtils from "../GEOServerUtils.js"
import * as  Hjson from "hjson";
import { DateTime } from "luxon";
import fetch from "node-fetch";


// Crear OAuth Client en
// https://shapps.dataspace.copernicus.eu/dashboard/#/account/settings
const TokenURL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
const CatalogURL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products";

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
            let downloaderConfig = fs.readFileSync(geoserver.configPath + "/copernicus-downloader.hjson").toString("utf8");
            try {
                downloaderConfig = Hjson.parse(downloaderConfig).dataSpaceConfig;
                console.log("config", downloaderConfig);
            } catch (error) {
                await this.addLog("E", "Error interpretando configuración de productos copernicus: " + error.toString());
                await this.finishProcess();
            }

            await this.addLog("I", "Obteniendo token para DataSpace de Copernicus ...");
            this.token = await this.getToken(downloaderConfig.userName, downloaderConfig.password);
            await this.addLog("I", "Token obtenido");

            // Consultar catálogo
            let desde = Date.now() - 1000 * 60 * 60 * 24 * 15;
            let catalogo = await this.queryODataCatalog(downloaderConfig, desde, Date.now());
            catalogo = catalogo.value.map(c => ({
                id: c.Id, name: c.Name, 
                date: parseInt((DateTime.fromISO(c.ContentDate.Start).valueOf() + DateTime.fromISO(c.ContentDate.End).valueOf()) / 2)
            }))
            await this.addLog("I", "Catálogo encontrado:\n" + JSON.stringify(catalogo, null, 4));
            this.state = await this.getData();
            for (let c of catalogo) {
                await this.checkCanceled();
                let outFileName = await this.downloadFile(c);
                await this.addLog("I", "Descargado " + outFileName);
            }

            await this.addLog("I", "Importación Finalizada");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    async getToken(username, password) {
        let params = new URLSearchParams(username, password);
        params.append("client_id", "cdse-public");
        params.append("grant_type", "password");
        params.append("username", username);
        params.append("password", password);
        try {
            const response = await fetch(TokenURL, {
                method: "POST",
                headers: {"Content-Type": "application/x-www-form-urlencoded"},
                body: params
            });
            if (!response.ok) throw response.statusText;
            let data = await response.json();
            return data.access_token;
        } catch (error) {
            console.error(error);
            throw error;
        }
    }

    async queryODataCatalog(config, desde, hasta) {
        //let headers = {"Authorization": "Bearer " + this.token};
        let headers = {};
        let inicio = DateTime.fromMillis(desde, {zone: "UTC"});
        let fin = DateTime.fromMillis(hasta, {zone: "UTC"});
        let filter = `ContentDate/Start gt ${inicio.toISO()} and ContentDate/End lt ${fin.toISO()} and (`;
        let firstCollection = true;
        for (let p of config.products) {
            if (firstCollection) {
                firstCollection = false;
            } else {
                filter += " or ";                
            }
            filter += `(Collection/Name eq '${p.collection}' and (`;
            let firstProduct = true;
            for (let p2 of p.products) {
                if (firstProduct) {
                    firstProduct = false;
                } else {
                    filter += " or ";
                }
                filter += `startswith(Name, '${p2.startsWith}')`;
            }
            filter += "))";
        }
        filter += ")";
        if (config.intersects) filter += " and " + config.intersects;
        console.log("filter", filter);

        let url = CatalogURL + "?$filter=" + encodeURIComponent(filter);
        console.log("url", url);
        try {
            let response = await fetch(url, {headers});   
            if (!response.ok) {
                let text = await response.text();
                throw response.statusText + ": " + text;
            }
            let data = await response.json();
            return data;
        } catch (error) {
            console.error(error);
            throw error;
        }        
    }

    async downloadFile(catalogEntry) {
        let outPath = `${geoServerUtils.downloadPath}/temp_${parseInt(Math.random() * 1000000)}.zip`;
        if (fs.existsSync(outPath)) fs.unlinkSync(outPath);
        let url = `https://zipper.dataspace.copernicus.eu/odata/v1/Products(${catalogEntry.id})/$value`;
        let headers = {"Authorization": "Bearer " + this.token};
        let response = await fetch(url, {headers});
        if (!response.ok) {
            let text = await response.text();
            throw response.statusText + ": " + text;
        }
        const fileStream = fs.createWriteStream(outPath);
        response.body.pipe(fileStream);
        return new Promise((resolve, reject) => {
            fileStream.on('finish', () => {
                console.log('Download completed.');
                resolve(outPath);
            });
    
            fileStream.on('error', (err) => {
                console.error('Error writing to file', err);
                reject(err);
            });
        });
    }

    async processZIP(filePath) {

    }
}
export {ImportadorCopernicus};