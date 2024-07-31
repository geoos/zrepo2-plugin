import { ZRepoProcess } from "../ZRepoPluginClient.js";
import zRepo from "../ZRepoClient.js";
import TimeUtils from "../TimeUtils.js";
import AWS from "aws-sdk";
import {DateTime} from "luxon";
import geoServerUtils from "../GEOServerUtils.js"
import S3Download from "s3-download";
import fs from "fs";
import { exec } from "child_process";

const tz = "America/Santiago";

class ABI_L2_CMI extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "goes-16-abi-l2-cmi"}
    get name() {return "Carga Multibandas"}
    get description() {return "Carga Multibandas"}

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
            if (!data.lastTime) {
                data.lastTime = 0;
            }            
            await this.addLog("I", "Buscando últimos datos de CMI GOES-16 ...");
            // Buscar el ultimo archivo dentro de la hora de hace 5 minutos            
            let lx = TimeUtils.nowLx("UTC").minus({minutes: 15});
            let bucketName = `noaa-goes16`;
            let stOrdinal = "" + lx.ordinal; 
            if (stOrdinal.length < 3) stOrdinal = "0" + stOrdinal;
            if (stOrdinal.length < 3) stOrdinal = "0" + stOrdinal;
            let prefix = `ABI-L2-MCMIPF/${lx.year}/${stOrdinal}/${(lx.hour<10?"0":"") + lx.hour}/`;
            await this.addLog("I", "Buscando en " + bucketName + prefix);

            let s3 = new AWS.S3();
            const params = {
                Bucket: bucketName,
                Delimiter: "",
                Prefix: prefix
            };

            const files = await s3.listObjectsV2(params).promise();
            if (!files.Contents) throw "No se retornaron archivos";
            let lastFile = null, maxTime = data.lastTime;
            for (let f of files.Contents) {
                let fileTime = f.LastModified.getTime();
                if (fileTime > maxTime) {
                    maxTime = fileTime;
                    lastFile = f;
                }                
            }

            if (lastFile) {
                let p0 = lastFile.Key.lastIndexOf("_c");
                let st = lastFile.Key.substring(p0+2, p0+15);
                let lx = DateTime.fromFormat(st, "yyyyoooHHmmss", {zone:"UTC"});
                if (!lx.isValid) {
                    throw "No se puede interpretar el string '" + st + "' del archivo '" + lastFile.Key + "' como fecha válida";
                }
                await this.addLog("I", "Descargando [" + lastFile.Size +" bytes] " + lastFile.Key);
                let filePath = await this.downloadFile(lastFile, bucketName, s3);
                let dstPath = geoServerUtils.workingPath + "/goes16-multibandas.nc";
                if (fs.existsSync(dstPath)) fs.unlinkSync(dstPath);
                fs.renameSync(filePath, dstPath);
                await this.processFile(dstPath, lx);
            } else {
                await this.addLog("I", "No hay nuevos archivos Multibanda");
            }

            data.lastTime = maxTime;
            await this.setData(data);            
            
            // Eliminar archivos usados
            let wp = geoServerUtils.workingPath;
            if (fs.existsSync(wp + "/goes16-multibandas.nc")) fs.unlinkSync(wp + "/goes16-multibandas.nc");
            for (let b=1; b<=16; b++) {                
                let band = "" + b;
                if (band.length < 2) band = "0" + band;
                if (fs.existsSync(wp + "//goes16-cmi" + band + ".nc")) fs.unlinkSync(wp + "/goes16-cmi" + band + ".nc");
                if (fs.existsSync(wp + "//goes16-cmi" + band + "_geo.nc")) fs.unlinkSync(wp + "/goes16-cmi" + band + "_geo.nc");                
            }
                        
            await this.addLog("I", "Carga de Multibandas Finalizada");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    downloadFile(file, bucketName, s3) {
        return new Promise((resolve, reject) => {
            let params = {
                Bucket:bucketName,
                Key:file.Key
            };
            let sessionParams = {
                concurrentStreams: 5,
                maxRetries: 3, 
                totalObjectSize: file.Size
            };
            try {
                let outPath = geoServerUtils.downloadPath + "/goes16-multibandas.nc";
                if (fs.existsSync(outPath)) fs.unlinkSync(outPath);
                let downloader = S3Download(s3);
                let d = downloader.download(params, sessionParams);
                d.on("error", err => reject(err));
                d.on("downloaded", dat => {
                    // console.log("dat", dat);
                    resolve(outPath);
                });
                let w = fs.createWriteStream(outPath);
                d.pipe(w);
            } catch(error) {
                reject(error);
            }
        });       
    }

    run(cmd) {  
        const ignorar1 = "ERROR 1: X axis unit";
        let bufferSize = 1024 * 1024;
        return new Promise((resolve, reject) => {
            try {
                let p = exec(cmd, {maxBuffer:bufferSize}, (err, stdout, stderr) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (stderr && stderr.toLowerCase().indexOf("error") >= 0 && stderr.indexOf(ignorar1) < 0) {
                        reject(stderr);
                        return;
                    }
                    resolve(stdout);
                });
                p.on("error", error => {
                    reject(error);
                })
            } catch(error) {
                reject(error);
            }
        })
    }

    async processFile(path, fileTime) {
        try {
            await this.addLog("I", "Procesando archivo " + path);            
            let outTime = fileTime.toFormat("yyyy-LL-dd_HH-mm");
            // Eliminar archivos intermedios anteriores si existen
            let wp = geoServerUtils.workingPath;
            let ip = geoServerUtils.importPath;
            for (let b=1; b<=16; b++) {                
                let band = "" + b;
                if (band.length < 2) band = "0" + band;
                await this.addLog("I", "--- CMI Band" + band);
                if (fs.existsSync(wp + "//goes16-cmi" + band + "_geo.nc")) fs.unlinkSync(wp + "/goes16-cmi" + band + "_geo.nc");                
                let cmd = `
                    gdalwarp -t_srs EPSG:4326 NETCDF:"${path}":CMI_C${band} ${wp}/goes16-cmi${band}_geo.nc
                `;
                await this.addLog("I", "[Band " + band + "] Ejecutando " + cmd);
                let res = await this.run(cmd);
                await this.addLog("I", "[Band " + band + "]   ==> " + res);
                await this.checkCanceled();
                // Mover a carpeta de importación
                // Mover archivo final a import
                let src = `${wp}/goes16-cmi${band}_geo.nc`;
                let dst = `${ip}/goes16-abi-l2-cmi_[CMI-${band}]${outTime}.nc`;
                await this.addLog("I", "[Band " + band + "]  ==> Generando archvo: " + dst);
                fs.renameSync(src, dst);
                await this.checkCanceled();
            }
        } catch (error) {
            throw error;
        }
    }
}

export {ABI_L2_CMI};