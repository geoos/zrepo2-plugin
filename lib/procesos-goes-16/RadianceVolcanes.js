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

class RadianceVolcanes extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "goes-16-radiance-volcanes"}
    get name() {return "Carga Radiance Volcanes"}
    get description() {return "Carga Radiance Volcanes"}

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
            await this.descargaRad07();                        
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
                let outPath = geoServerUtils.downloadPath + "/goes16-rad.nc";
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

    async descargaRad07() {
        try {
            let data = await this.getData();
            if (!data.lastTimeRad07) {
                data.lastTimeRad07 = 0;
            }            
            await this.addLog("I", "Buscando últimos datos de RadF-07 GOES-16 ...");
            // Buscar el ultimo archivo dentro de la hora de hace 5 minutos            
            let lx = TimeUtils.nowLx("UTC").minus({minutes: 15});
            let bucketName = `noaa-goes16`;
            let stOrdinal = "" + lx.ordinal; 
            if (stOrdinal.length < 3) stOrdinal = "0" + stOrdinal;
            if (stOrdinal.length < 3) stOrdinal = "0" + stOrdinal;
            let prefix = `ABI-L1b-RadF/${lx.year}/${stOrdinal}/${(lx.hour<10?"0":"") + lx.hour}/OR_ABI-L1b-RadF-M6C07_`;
            await this.addLog("I", "Buscando en " + bucketName + prefix);

            let s3 = new AWS.S3();
            const params = {
                Bucket: bucketName,
                Delimiter: "",
                Prefix: prefix
            };

            const files = await s3.listObjectsV2(params).promise();
            if (!files.Contents) throw "No se retornaron archivos";
            let lastFile = null, maxTime = data.lastTimeRad07;
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
                let dstPath = geoServerUtils.workingPath + "/goes16-rad-07.nc";
                if (fs.existsSync(dstPath)) fs.unlinkSync(dstPath);
                fs.renameSync(filePath, dstPath);
                let dstGeoPath = geoServerUtils.workingPath + "/goes16-rad-07_geo.nc";
                if (fs.existsSync(dstGeoPath)) fs.unlinkSync(dstGeoPath);
                let cmd = `
                    gdalwarp -t_srs EPSG:4326 NETCDF:"${dstPath}":Rad ${dstGeoPath}
                `;
                await this.addLog("I", "Ejecutando " + cmd);
                let res = await this.run(cmd);
                await this.addLog("I", "   ==> " + res);
                await this.checkCanceled();
                let dstUnscaledPath = geoServerUtils.workingPath + "/goes16-rad-07_uns.nc";
                if (fs.existsSync(dstUnscaledPath)) fs.unlinkSync(dstUnscaledPath);
                cmd = `
                    gdal_translate -unscale -ot float32 ${dstGeoPath} ${dstUnscaledPath}
                `;
                await this.addLog("I", "Ejecutando " + cmd);
                res = await this.run(cmd);
                await this.addLog("I", "   ==> " + res);
                await this.checkCanceled();
                await this.calculaRadiaciones(dstUnscaledPath, lx.valueOf());
                if (fs.existsSync(dstPath)) fs.unlinkSync(dstPath);
                if (fs.existsSync(dstGeoPath)) fs.unlinkSync(dstGeoPath);
                if (fs.existsSync(dstUnscaledPath)) fs.unlinkSync(dstUnscaledPath);
            } else {
                await this.addLog("I", "No hay nuevos archivos Radiance Banda 07");
            }

            data.lastTimeRad07 = maxTime;
            await this.setData(data);         
            await this.addLog("I", "Finaliza descarga de Radiance para Volcanes");               
        } catch (error) {
            throw error;
        }
    }

    async calculaRadiaciones(bandFile, time) {
        try {
            let agregoFilas = false;
            await this.addLog("I", "Recorriendo volcanes");
            const resolucion = 0.0032085636527; // 0.000056 radianes
            let volcanes = await zRepo.getValores("sng.volcan");
            for (let volcan of volcanes) {
                await this.addLog("I", "-- " + volcan.name + " (" + volcan.lat + ", " + volcan.lng + ")");
                let sum=0, n=0;
                for (let iLng=-2; iLng<=2; iLng++) {
                    let lng = volcan.lng + iLng * resolucion;
                    for (let iLat=-2; iLat<=2; iLat++) {
                        let lat = volcan.lat + iLat * resolucion;
                        let radiacion = await this.run(`
                            gdallocationinfo -geoloc -valonly ${bandFile} ${lng} ${lat}
                        `);
                        radiacion = parseFloat(radiacion);
                        if (!isNaN(radiacion)) {
                            n++; sum += radiacion;
                        }
                    }
                }
                if (n) {
                    let radiacion = sum/n;
                    await this.addLog("I", "    => Radiación media: " + radiacion.toLocaleString());
                    let dsRow = {time, codigoVolcan: volcan.code, radiacion}
                    await zRepo.accumDataSet("sng.reporte_radiacion_volcan", dsRow);
                    agregoFilas = true;
                }
            }            
            if (agregoFilas) await zRepo.flushDataSet("sng.reporte_radiacion_volcan");
        } catch (error) {
            throw error;
        }
    }
}

export {RadianceVolcanes};