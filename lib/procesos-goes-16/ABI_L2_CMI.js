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
        let bufferSize = 1024 * 1024;
        return new Promise((resolve, reject) => {
            try {
                let p = exec(cmd, {maxBuffer:bufferSize}, (err, stdout, stderr) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    if (stderr) {
                        reject(stderr);
                        return;
                    }
                    resolve(stdout);
                });
                p.on("error", error => {
                    console.group("child_process error", error);
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
            let promises = [];
            for (let b=1; b<=16; b++) {                
                let band = "" + b;
                if (band.length < 2) band = "0" + band;
                await this.addLog("I", "--- CMI Band" + band);
                if (fs.existsSync(wp + "//goes16-cmi" + band + ".nc")) fs.unlinkSync(wp + "/goes16-cmi" + band + ".nc");
                if (fs.existsSync(wp + "//goes16-cmi" + band + "_geo.nc")) fs.unlinkSync(wp + "/goes16-cmi" + band + "_geo.nc");                
                let cmd = `
                    gdal_translate -ot float32 -unscale NETCDF:"${path}":CMI_C${band} ${wp}/goes16-cmi${band}.nc
                `;
                await this.addLog("I", "[Band " + band + "] Ejecutando " + cmd);
                let res = await this.run(cmd);
                await this.addLog("I", "[Band " + band + "]   ==> " + res);
                await this.checkCanceled();
                let prom = new Promise(async (resolve, reject) => {
                    try {
                        cmd = `
                            gdalwarp -t_srs EPSG:4326 ${wp}/goes16-cmi${band}.nc ${wp}/goes16-cmi${band}_geo.nc
                        `;
                        await this.addLog("I", "[Band " + band + "] Ejecutando " + cmd);
                        res = await this.run(cmd);
                        await this.addLog("I", "[Band " + band + "]   ==> " + res);
                        // Mover a carpeta de importación
                        // Mover archivo final a import
                        let src = `${wp}/goes16-cmi${band}_geo.nc`;
                        // Si la banda es la 07, calcular radiación de volcanes
                        if (b == 7) {
                            await this.calculaRadiaciones(src, fileTime.valueOf());
                        }
                        let dst = `${ip}/goes16-abi-l2-cmi_[CMI-${band}]${outTime}.nc`;
                        await this.addLog("I", "[Band " + band + "]  ==> Generando archvo: " + dst);
                        fs.renameSync(src, dst);
                        await this.checkCanceled();
                        resolve();
                    } catch(error) {
                        reject(error);
                    }
                })
                promises.push(prom);
            }
            await this.addLog("I", "Esperando finalización de comandos ...");
            await Promise.all(promises);            
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

export {ABI_L2_CMI};