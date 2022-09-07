import fetch from "node-fetch";
import httpProt from "http";
import httpsProt from "https";

class ZRepoClient {
    static get instance() {
        if (!ZRepoClient._instance) ZRepoClient._instance = new ZRepoClient();
        return ZRepoClient._instance;
    }

    async init(zRepoURL, token) {
        this.url = zRepoURL;
        this.token = token;
        try {
            await this.readMetadata();
        } catch(error) {
            throw error;
        }
    }

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
                            resolve(buffer);
                        }
                    })
                })
                req.on("error", err => reject(err));
                req.write(stData);
                req.end();
            } catch(error) {
                reject(error);
            }
        })        
    }

    deleteRequest(url) {
        return new Promise((resolve, reject) => {
            let headers = {};
            headers["Content-Type"] = "application/json";
            if (this.token) headers["Authorization"] = "Bearer " + this.token;

            let http = url.startsWith("https")?httpsProt:httpProt;
            let options = {
                headers:headers, method:"DELETE"
            }     
            let buffer = "", responseError = null;   
            try {
                let req = http.request(new URL(url), options, res => {
                    if (res.statusCode != 200) {
                        responseError = "[" + res.statusCode + "] " + res.statusMessage + (buffer.length?". " + buffer:"");
                    }
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
                            resolve(buffer);
                        }
                    })
                })
                req.on("error", err => reject(err));
                //req.write(stData);
                req.end();
            } catch(error) {
                reject(error);
            }
        })        
    }

    async readMetadata() {
        await this.getDimensiones();
        await this.getVariables();
        return {dimensiones:this.dimensiones, variables:this.variables}
    }
    async getDimensiones() {
        if (this.dimensiones) return this.dimensiones;
        try {
            this.dimensiones = (await (await fetch(this.url + "/dim/dimensions?token=" + this.token)).json());
            return this.dimensiones;
        } catch(error) {
            console.log("Usando URL:" + this.url + "/dim/dimensions?token=" + this.token)
            throw error;
        }
    }
    async getDimension(code) {
        let dims = await this.getDimensiones();
        if (!dims) return null;
        return dims.find(d => d.code == code);
    }
    async getVariables() {
        if (this.variables) return this.variables;
        try {
            let cache = Math.random() * 9999999999;
            this.variables = (await (await fetch(this.url + "/var/variables?token=" + this.token + "&cache=" + cache)).json());
            return this.variables;
        } catch(error) {
            throw error;
        }
    }
    async getVariable(code) {
        let variables = await this.getVariables();
        return variables.find(v => (v.code == code));
    }

    // Filas Dimensiones
    async getValorDimension(codigoDimension, codigoFila) {
        try {
            let f = await fetch(this.url + "/dim/" + codigoDimension + "/rows/" + codigoFila + "?token=" + this.token);
            if (f.status != 200) throw await f.text();
            else return await f.json();
        } catch(error) {
            throw error;
        }
    }
    async countValores(codigoDimension, textFilter, filter) {
        try {
            let url = this.url + "/dim/" + codigoDimension + "/rows?token=" + this.token;
            if (textFilter) url += "&textFilter=" + encodeURIComponent(textFilter);
            if (filter) url += "&filter=" + encodeURIComponent(JSON.stringify(filter));
            url += "&count=true";
            let f = await fetch(url);
            if (f.status != 200) throw await f.text();
            let r = await f.json();
            return r.n;
        } catch(error) {
            throw error;
        }
    }
    async getValores(codigoDimension, textFilter, filter, startRow, nRows, includeNames) {
        try {
            let url = this.url + "/dim/" + codigoDimension + "/rows?token=" + this.token;
            if (textFilter) url += "&textFilter=" + encodeURIComponent(textFilter);
            if (filter) url += "&filter=" + encodeURIComponent(JSON.stringify(filter));
            if (startRow !== undefined && nRows !== undefined) url += "&startRow=" + startRow + "&nRows=" + nRows;
            if (includeNames) url += "&includeNames=true";
            let f = await fetch(url);
            if (f.status != 200) throw await f.text();
            else return await f.json();
        } catch(error) {
            throw error;
        }
    }
    async getAllValores(codigoDimension) {
        try {
            let url = this.url + "/dim/" + codigoDimension + "/all-rows?token=" + this.token;
            let f = await fetch(url);
            if (f.status != 200) throw await f.text();
            else return await f.json();
        } catch(error) {
            throw error;
        }
    }
    async setFilaDimension(codigoDimension, fila) {
        try {
            let url = this.url + "/dim/" + codigoDimension;
            await this.post(url, null, fila);
        } catch(error) {
            throw error;
        }
    }

    // DataSets
    postDataSet(dsCode, row) {
        let url = this.url + "/dataSet/" + dsCode;
        return this.post(url, null, row);
    }

    // Variables
    deletePeriod(varCode, startTime, endTime, varData, details) {
        let url = this.url + "/data/" + varCode + "/period?startTime=" + startTime + "&endTime=" + endTime;
        if (varData) url += "&varData=true";
        if (details) url += "&details=true";
        return this.deleteRequest(url);
    }
    postVar(varCode, time, value, data, options) {
        let url = this.url + "/data/" + varCode;
        return this.post(url, null, {time, data, value, options});
    }
}
export default ZRepoClient.instance;