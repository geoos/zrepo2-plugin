import fetch from "node-fetch";
import httpProt from "http";
import httpsProt from "https";

class ZRepoPluginClient {
    constructor() {
        this.processClasses = [];
    }

    async init(app, zRepoURL, pluginCode) {
        this.zRepoURL = zRepoURL;
        this.pluginCode = pluginCode;
        try {
            await this.registerEndPoints(app);
        } catch(error) {
            throw error;
        }
    }

    registerProcess(procClass) {this.processClasses.push(procClass)};

    post(url, headers, data) {
        return new Promise((resolve, reject) => {
            headers = headers || {};
            headers["Content-Type"] = "application/json";
            let stData = (typeof data == "object")?JSON.stringify(data):data;
            //headers["Content-Length"] = stData.length;
            if (this.token) headers["Authorization"] = "Bearer " + this.token;

            let http = url.startsWith("https")?httpsProt:httpProt;
            let options = {
                headers:headers, method:"POST"
            }     
            let buffer = "", responseError = null;   
            try {
                let req = http.request(new URL(url), options, res => {
                    if (res.statusCode != 200) {
                        responseError = "[" + res.statusCode + "] " + res.statusMessage + (buffer.length?". " + buffer:"");
                        //reject("[" + res.statusCode + "] " + res.statusMessage + (buffer.length?". " + buffer:""));
                        //return;                    
                    }
                    res.setEncoding('utf8');
                    res.on("error", err => {
                        reject(err)
                    });
                    res.on('data', d => {
                        buffer += d
                    });
                    res.on("end", _ => {
                        if (responseError) {
                            reject(responseError + ". " + buffer);
                        } else {
                            if (res.headers && res.headers["content-type"] && res.headers["content-type"].startsWith("application/json")) {
                                try {
                                    let json = JSON.parse(buffer);
                                    resolve(json);
                                    return;
                                } catch(error) {
                                    console.error("Invalid json from plugin", buffer, error);
                                }
                            }
                            resolve(buffer);
                        }
                    })
                })
                req.setTimeout(0);
                req.on("error", err => reject(err));
                req.write(stData);
                req.end();
            } catch(error) {
                reject(error);
            }
        })        
    }

    async registerEndPoints(app) {
        try {
            app.get("/zcb/" + this.pluginCode + "/metadata/:processCode", (req, res) => this.resolveProcMetadata(req, res));
            app.post("/zcb/" + this.pluginCode + "/exec", (req, res) => this.resolveExec(req, res));
            app.post("/zcb/" + this.pluginCode + "/execSync", (req, res) => this.resolveExecSync(req, res));
        } catch (error) {
            throw error;
        }
    }

    returnError(res, error) {
        if (typeof error == "string") {
            res.status(400).send(error);
        } else { 
            console.error("Error Interno:", error);        
            console.trace(error);
            res.status(500).send("Error Interno");
        }
    }
    returnOK(res, ret) {
        res.setHeader('Content-Type', 'application/json');
        res.status(200);
        if (typeof ret == "number") res.send("" + ret);
        else res.send(ret?ret:null);    
    }

    /*
    getProcMetadata() {
        this.procClasses = {}
        let procs = [];
        for (let procClass of this.processClasses) {
            let pi = new procClass();            
            procs.push({code:pi.code, name:pi.name, description:pi.description, params:pi.paramsDec})
            this.procClasses[pi.code] = procClass;
        }
        return procs;
    }
    */

    getMetadata(processCode) {
        if (!this.procMetadatas) this.procMetadatas = {};
        let pm = this.procMetadatas[processCode];
        if (pm) return pm;
        for (let procClass of this.processClasses) {
            let pi = new procClass();
            if (pi.code == processCode) {
                pm = {code:pi.code, name:pi.name, description:pi.description, params:pi.paramsDec};
                this.procMetadatas[processCode] = pm;
                break;
            }
        }
        return pm;
    }
    getProcClass(processCode) {
        if (!this.procClasses) this.procClasses = {};
        let pc = this.procClasses[processCode];
        if (pc) return pc;
        for (let procClass of this.processClasses) {
            let pi = new procClass();
            if (pi.code == processCode) {
                pc = procClass;
                this.procClasses[processCode] = pc;
                break;
            }
        }
        return pc;
    }    

    resolveProcMetadata(req, res) {
        let processCode = req.params.processCode;
        let pm = this.getMetadata(processCode);
        if (!pm) {
            this.returnError(res, "No se encontró la Metadata del proceso '" + processCode + "'");
            return;
        }
        this.returnOK(res, pm);
    }

    async getProcessData(process) {
        try {
            let r = await fetch(this.zRepoURL + "/pluginAPI/processData/" + process);
            let data = r.json();
            return data;
        } catch (error) {
            throw error;
        }
    }
    async setProcessData(process, data) {
        try {
            await this.post(this.zRepoURL + "/pluginAPI/processData/" + process, null, data);
        } catch (error) {
            throw error;
        }
    }

    async addLog(instanceId, type, text) {
        try {
            if (instanceId == "-1") return;
            await this.post(this.zRepoURL + "/pluginAPI/addLog", null, {instanceId, type, text});
        } catch (error) {
            throw error;
        }
    }
    async finishProcess(instanceId) {
        try {
            if (instanceId == "-1") return;
            await this.post(this.zRepoURL + "/pluginAPI/finishProcess", null, {instanceId});
        } catch (error) {
            throw error;
        }
    }
    async getStatus(instanceId) {
        try {
            if (instanceId == "-1") return {status:"ok"};
            let r = await fetch(this.zRepoURL + "/pluginAPI/status/" + instanceId);
            let status = r.json();
            return status;
        } catch (error) {
            throw error;
        }
    }
    async getRunningInstances(instanceId) {
        try {
            let r = await fetch(this.zRepoURL + "/pluginAPI/runningInstances/" + instanceId);
            try {
                let rows = await r.json();
                return rows;
            } catch(error) {
                throw await r.text();
            }
        } catch (error) {
            throw error;
        }
    }
    async setDescription(instanceId, description) {
        try {
            if (instanceId == "-1") return;
            await this.post(this.zRepoURL + "/pluginAPI/setProcessInstanceDescription", null, {instanceId, description});
        } catch (error) {
            throw error;
        }
    }
    async finishRunningInstance(instanceId, description) {
        try {
            await this.post(this.zRepoURL + "/pluginAPI/finishProcess", null, {instanceId, description});
        } catch (error) {
            throw error;
        }
    }

    /*
    resolvePing(req, res) {
        this.returnOK(res, {token:this.token, pong:true});
    }
    */

    resolveExec(req, res) {
        let code = req.body.process;
        // console.log("exec", code);
        let procClass = this.getProcClass(code);
        if (!procClass) {
            console.error("Se recibe solicitud de ejecución de proceso '" + code + "' que no se encuentra registrado en este plugin:" + this.pluginCode)
            this.returnError(res, "No se encontró el proceso " + code);
            return;
        }
        let instanceId = req.body.instanceId;
        let params = req.body.params;
        let trigger = req.body.trigger;
        let procInstance = new procClass();
        procInstance.pluginClient = this;
        procInstance.instanceId = instanceId;
        procInstance.trigger = trigger;
        this.returnOK(res, {});        
        procInstance.exec(params)
            .then(_ => {
                if (!procInstance._wasFinished) {
                    console.log("Proceso no finalizado explícitamente: " + procInstance.code + ". Finalizando ...");
                    procInstance.finishProcess()
                        .then(_ => console.log("Finalizado."))
                        .catch(err => console.error(err));
                }
            })
            .catch(error => {
                console.error(error);
                procInstance.addLog("E", error.toString()).then(_ => procInstance.finishProcess());
            })
    }   
    
    resolveExecSync(req, res) {
        let code = req.body.process;
        // console.log("exec", code);
        let procClass = this.procClasses[code];
        if (!procClass) {
            console.error("Se recibe solicitud de ejecución de proceso '" + code + "' que no se encuentra registrado en este plugin:" + this.pluginCode)
            this.returnError(res, "No se encontró el proceso " + code);
            return;
        }
        let instanceId = req.body.instanceId;
        let params = req.body.params;
        let trigger = req.body.trigger;
        let procInstance = new procClass();
        procInstance.pluginClient = this;
        procInstance.instanceId = instanceId;
        procInstance.trigger = trigger;
        //this.returnOK(res, {});        
        procInstance.exec(params)
            .then(ret => {
                if (instanceId != "-1" && !procInstance._wasFinished) {
                    console.log("Proceso no finalizado explícitamente: " + procInstance.code + ". Finalizando ...");
                    procInstance.finishProcess()
                        .then(_ => {
                            console.log("Finalizado.");
                            this.returnOK(res, ret);
                        })
                        .catch(err => {
                            console.error(err);
                            this.returnError(res, err);
                        });
                } else {
                    this.returnOK(res, ret);
                }
            })
            .catch(error => {
                console.error(error);
                procInstance.addLog("E", error.toString()).then(_ => procInstance.finishProcess());
                this.returnError(error);
            })
    }   
}

class ZRepoProcess {
    get code() {return "no-code"}
    get name() {return "no-name"}
    get description() {return "No Description"}

    declareParams(params) {
        this.paramsDec = params;
    }

    async getData() {
        try {
            return await this.pluginClient.getProcessData(this.code);
        } catch (error) {
            console.error(error);
        }
    }
    async setData(data) {
        try {
            await this.pluginClient.setProcessData(this.code, data);
        } catch (error) {
            console.error(error);
        }
    }

    async addLog(type, text) {
        try {
            if (this.instanceId == "-1") return;
            await this.pluginClient.addLog(this.instanceId, type, text);
        } catch (error) {
            console.error(error);
        }
    }
    async finishProcess() {
        try {
            if (this.instanceId != "-1") {
                await this.pluginClient.finishProcess(this.instanceId);
            }
            this._wasFinished = true;
        } catch (error) {
            console.error(error);
        }
    }
    async finishProcessInstance(instanceId, description) {
        try {
            await this.pluginClient.finishRunningInstance(instanceId, description);
        } catch (error) {
            console.error(error);
        }
    }
    async getStatus() {
        try {
            return await this.pluginClient.getStatus(this.instanceId);
        } catch (error) {
            console.error(error);
            return {running:true, canceling:false}
        }
    }
    async getRunningInstances() {
        try {
            return await this.pluginClient.getRunningInstances(this.instanceId);
        } catch (error) {
            console.error(error);
            return [];
        }
    }
    async getExternalRunningInstances(processCode) {
        try {
            return await this.pluginClient.getRunningInstances(processCode || "_all_");
        } catch (error) {
            console.error(error);
            return [];
        }
    }
    async setDescription(description) {
        try {
            await this.pluginClient.setDescription(this.instanceId, description);
        } catch (error) {
            console.error(error);
        }
    }
    async checkCanceled() {
        try {
            let s = await this.getStatus();
            if (!s.running) throw "Finalización Forzada por usuario";
            if (s.canceling) throw "Finaliza por Cancelación solicitada por usuario";
        } catch (error) {
            throw error;
        }
    }
    async exec(params) {
        throw "exec No implementado en " + this.code;
    }
}
export {ZRepoPluginClient, ZRepoProcess};
