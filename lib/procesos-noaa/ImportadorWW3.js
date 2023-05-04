import { ZRepoProcess } from "../ZRepoPluginClient.js";
import { WW34ModelExecution } from "./WW3ModelExecution.js";
import { DateTime } from "luxon";
import fs from "fs";
import geoserver from "../GEOServerUtils.js";
import moment from "moment-timezone";
import https from "https";

const tz = process.env.LOCAL_TZ || "UTC";

class ImportadorWW3 extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "noaa-ww3"}
    get name() {return "Importador NOAA - WW3"}
    get description() {return "Importador NOAA - WW3"}

    get nParallelDownloads() {return parseInt(process.env.WW3_N_PARALLEL_DOWNLOADS || "5")}
    get nRetries() {return parseInt(process.env.N_RETRIES || "5")}
    get maxForecastHours() {return parseInt(process.env.WW3_MAX_FORECAST_HOURS || "180")}


    async exec(params) {
        try {
            await this.addLog("I", "Iniciando proceso NOAA - WW3");
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

            // Cancel pending downloads from last run
            this.state = await this.getData();
            if (this.state.files) {
                this.state.files.forEach(f => {
                    if (f.status == "downloading") {
                        f.status = "pending";
                    }
                })
                await this.setData(this.state);
            }

            let files = fs.readdirSync(geoserver.downloadPath);
            files.forEach(f => {
                try {
                    if (f.startsWith(this.code + "_") && f.endsWith(".grb2")) {
                        fs.unlinkSync(geoserver.downloadPath + "/" + f);
                    }
                } catch(err) {}
            })

            // Buscar nuevo modelo o continuar anterior descarga
            let mExec, continuaImportacion = false;
            if (this.state.currentModel) {
                let modelTime = moment.tz(this.state.currentModel, "UTC");
                await this.addLog("W", "Se encontró una descarga interrumpida para el modelo UTC:" + modelTime.format("YYYY-MM-DD HH:mm"));
                mExec = new WW34ModelExecution(modelTime);
                let stillPublished = await mExec.isPublished();
                if (stillPublished) {
                    continuaImportacion = true;
                    await this.addLog("W", "El modelo aún está publicado. Completando la descarga parcial.");
                } else {
                    await this.addLog("W", "El Modelo ya no está publicado. Se descarta");
                    delete this.state.currentModel;
                    delete this.state.files;
                    await this.setData(this.state);
                    mExec = null;
                }
            }
            if (!mExec) {
                mExec = new WW34ModelExecution(moment.tz("UTC").startOf("hour"));
                let n = 0,published = false;
                do {
                    published = await mExec.isPublished();
                    if (!published) mExec.dec();
                    n++;
                } while(n < 50 && !published);
                if (!published) {
                    await this.addLog("I", "No se encontró un nuevo modelo en NOAA");
                    await this.finishProcess();
                    return;
                }
                let isNewModel = !this.state.lastModel || mExec.time.valueOf() > this.state.lastModel;
                if (!isNewModel) {
                    await this.finishProcess();
                    return;
                }
                this.state.currentModel = mExec.time.valueOf();
                this.state.files = [];
                await this.addLog("I", "  => Buscando archivos hasta " + this.maxForecastHours + " horas de pronóstico");                 
                let hh = 0, max = parseInt(this.maxForecastHours);
                while (hh <= max) {
                    let url = mExec.getNOAAUrl(hh, ".grib2");
                    let forecastTime = mExec.time.clone().add(hh, "hours");
                    let fileName = this.code + "_" + forecastTime.format("YYYY-MM-DD_HH-mm") + ".grb2"
                    this.state.files.push({url, fileName, status:"pending", retries:0});
                    if (hh < 120) hh ++;
                    else hh += 3;
                }
                this.setData(this.state);
            }
            await this.addLog("I", `------ ${continuaImportacion?"Continuando":"Iniciando"} descarga del modelo: ${mExec.time.format("YYYY-MM-DD HH:mm")}`);

            this.activeRequests = {}; // fileURTL:httpRequest
            this.canceled = false;
            await this.startInitialDownloads(); 

            let nActive;
            do {
                // Esperar 5 segundos
                await (new Promise(resolve => {setTimeout(_ => resolve(), 5000)}));
                try {
                    await this.checkCanceled();
                } catch (error) {
                    this.canceled = true;
                    for (let url of Object.keys(this.activeRequests)) {
                        if (this.activeRequests[url]) this.activeRequests[url].destroy();
                    }
                }
                nActive = this.state.files?this.state.files.filter(f => f.status == "downloading").length:0;     
                //console.log("nActive", nActive);           
            } while(nActive);

            let  nPending = this.state.files.filter(f => f.status == "pending").length;
            if (!nPending) delete this.state.currentModel;

            await this.setData(this.state);
            if (this.canceled) {
                await this.addLog("E", "------ Importación Finalizada por Solicitud del Usuario");
            } else {
                await this.addLog("I", "------ Importación Finalizada");
            }
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    async startInitialDownloads() {
        try {            
            if (!this.state.files) {                
                throw "No hay archivos en 'state'";
            }        
            let nActive = this.state.files.filter(f => f.status == "downloading").length;
            let nPending = this.state.files.filter(f => f.status == "pending").length;
            if (!nPending) {
                await this.addLog("W", "No hay pendientes al iniciar descarga.");
                return;
            }
            while (nActive < this.nParallelDownloads && nPending) {
                await this.startNextDownload();
                // Sleep 5 sec
                await (new Promise(resolve => {setTimeout(_ => resolve(), 5000)}));
                nActive = this.state.files?this.state.files.filter(f => f.status == "downloading").length:0
                nPending = this.state.files.filter(f => f.status == "pending").length;
            }
        } catch (error) {
            throw error;
        }
    }

    async startNextDownload() {
        if (this.canceled) return;        
        try {
            if (!this.state.files) return;
            let file = this.state.files.find(f => f.status == "pending");
            if (!file) return;            
            file.status = "downloading";
            file.startTime = Date.now();
            await this.setData(this.state);
            this.downloadFile(file)
                .then(async _ => {
                    let f2 = this.state.files.find(f => f.url == file.url);
                    f2.status = "ok";
                    await this.setData(this.state);
                    this.startNextDownload();
                })
                .catch(async error => {
                    if (this.canceled) {
                        let f2 = this.state.files.find(f => f.url == file.url);
                        f2.status = "pending";
                        await this.setData(this.state);
                        return;
                    };
                    let f2 = this.state.files.find(f => f.url == file.url);
                    f2.retries++;
                    if (f2.retries > this.nRetries) {
                        await this.addLog ("E", "Error downloading file '" + f2.url + "': " + error.toString() + ". Max retries (" + this.nRetries + ") reached. File discarted");
                        f2.status = "error";
                    } else {
                        this.addLog("W", "Error downloading file '" + f2.url + "': " + error.toString() + ". Retry " + f2.retries + "/" + this.nRetries + ". Waiting 60 sec.");
                        await (new Promise(resolve => setTimeout(_ => resolve(), 60000)));
                        f2 = this.state.files.find(f => f.url == file.url);
                        f2.retries++;
                        f2.status = "pending";
                    }
                    await this.setData(this.state);
                    this.startNextDownload();
                });            
        } catch(error) {
            console.error("Error starting next download");
            throw error;
        }
    }

    downloadFile(file) {        
        return new Promise(async (resolve, reject) => {
            try {
                let p = file.url.lastIndexOf("/");
                let fileName = file.url.substring(p+1);
                await this.addLog("I", fileName + ": Iniciando Descarga desde " + file.url);
                let dstFile = geoserver.downloadPath + "/" + file.fileName;
                let t0 = Date.now();
                let fileStream = fs.createWriteStream(dstFile);
                this.activeRequests[file.url] = https.get(file.url, async response => {
                    try {
                        if (response && response.statusCode == 200) {
                            try {
                                response.pipe(fileStream);
                            } catch(error) {
                                await this.addLog("E", "Error [1] preparing download:" + error.toString());
                                delete this.activeRequests[file.url];
                                reject(error);
                            }
                            fileStream.on('finish', _ => {
                                delete this.activeRequests[file.url];
                                if (response.timeoutTimer) {clearTimeout(response.timeoutTimer); response.timeoutTimer = null;}
                                fileStream.close(async _ => {
                                    try {
                                        let mm = parseInt((Date.now() - t0) / 1000 / 60);
                                        let ss = parseInt((Date.now() - t0) / 1000) - mm * 60;
                                        mm = (mm<10?"0":"") + mm;
                                        ss = (ss<10?"0":"") + ss;
                                        await this.addLog("I", fileName + ": Descargado en " + mm + ":" + ss);
                                        try {
                                            fs.renameSync(dstFile, geoserver.importPath + "/" + file.fileName)
                                        } catch(error) {
                                            console.error(`Error [2] moviendo archivo ${dstFile} to ${geoserver.importPath + "/" + file.fileName}: ${error.toString()}`);
                                            reject(error);
                                            return;
                                        }
                                    } catch(error) {
                                        reject(error);
                                    }
                                    resolve();
                                });
                            });
                            fileStream.on('error', async err => {
                                delete this.activeRequests[file.url];
                                try {
                                    if (response.timeoutTimer) {clearTimeout(response.timeoutTimer); response.timeoutTimer = null;}
                                    response.resume();                                    
                                    try {
                                        try {
                                            fs.unlinkSync(dstFile);
                                        } catch(error) {}
                                        await this.addLog("E", `Error [3] descargando archivo ${file.url}: ${err.toString()}`)
                                    } catch(err2) {}
                                    reject(err);
                                } catch(error) {
                                    reject(error);
                                }
                            });
                            response.on("error", async err => {        
                                delete this.activeRequests[file.url];                        
                                try {
                                    if (response.timeoutTimer) {clearTimeout(response.timeoutTimer); response.timeoutTimer = null;}                                      
                                    response.resume();
                                    await this.addLog("E", `Error [4] descargando archivo ${file.url}: ${err.toString()}`)
                                    reject(err);
                                } catch(error) {
                                    reject(error);
                                }
                            });
                            response.timeoutTimer = setTimeout(async _ => {
                                delete this.activeRequests[file.url];
                                try {
                                    await this.addLog("E", "Timeout (30mn) para descarha de archivo. Destruyendo request");
                                    await this.addLog("E", "  => File: " + file.url);
                                    request.destroy(Error("Timeout descargando archivo"));
                                    reject("Timeout 30m");
                                } catch(error) {
                                    reject(error);
                                }
                            }, 30 * 60 * 1000);
                        } else {
                            delete this.activeRequests[file.url];
                            reject("Response [5] Status Code:" + response.statusCode);
                        }
                    } catch(error) {
                        delete this.activeRequests[file.url];
                        reject(error);
                    }
                }).on("error", async err => {
                    try {
                        await this.addLog("E", "Error descargando archivo:" + err.toString());
                        reject(err);
                    } catch(error) {
                        reject(error);
                    }
                })     
            } catch(error) {
                delete this.activeRequests[file.url];
                reject(error);
            }       
        });
    }
}

export {ImportadorWW3};