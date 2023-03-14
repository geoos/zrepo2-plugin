import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import fetch from "node-fetch";
import moment from "moment-timezone";

const tz = "America/Santiago";

class ImportadorSHOA extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-shoa"}
    get name() {return "Importar datos de Mareógrafos SHOA"}
    get description() {return "Importar datos de Mareógrafos SHOA"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación");
            // Chequear otras instancias en ejecución
            let pis = await this.getRunningInstances();
            if (pis.length) {
                let p0 = pis[0];
                let m = moment.tz(p0.started, tz);
                await this.setDescription("Otra Instancia en ejecución. Se Descarta");
                await this.addLog("W", "Se encontró instancia en ejecución desde " + m.format("YYYY-MM-DD HH:mm"));
                await this.finishProcess();
                return;
            }
            // Chequear que exista el proveedor "ceaza"
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "shoa");
            if (!row) {
                await this.addLog("I", "Creando Proveedor SHOA");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"shoa", name:"SHOA - Servicio Hidrográfico de la Armada"});
            }

            let estaciones = await ZRepoClient.getValores("rie.estacion", null, {proveedor:"shoa"});
            estaciones = estaciones.filter(e => {
                if (e.activa === false) return false;
                return true;
            })
            if (!estaciones.length) {
                await this.addLog("E", "No se encontraron mareógrafos para el proveedor 'shoa");
                await this.finishProcess();
                return;
            }
            await this.addLog("I", "---- Buscando datos para " + estaciones.length + " mareógrafos de SHOA");
            this.estado = await this.getData();
            for (let e of estaciones) {
                await this.addLog("I", "[" + e.code + "] " + e.name);
                await this.downloadMareografo(e.code);
            }

            await this.setData(this.estado);
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    async downloadSensor(codigoSensor, codigoMareografo) {
        try {
            
            let dt = 12;
            let estado = this.estado;
            if (estado[codigoMareografo + "-" + codigoSensor] && (Date.now() - estado[codigoMareografo + "-" + codigoSensor]) < 1000 * 60 * 60) dt = 1;
            let url = `
                http://wsprovimar.mitelemetria.cl/apps/src/ws/wsexterno.php?wsname=getData&idsensor=${codigoSensor}&idestacion=${codigoMareografo}&period=${dt}&fmt=json&tipo=tecmar&orden=ASC
            `;
            let datos = await this.downloadFile(url);
            try {
                datos = JSON.parse(datos);
            } catch(error) {
                throw "Resultado no es JSON:" + datos.toString();
            }
            if (!Array.isArray(datos)) {
                if (datos.result && datos.result.descripcion) throw datos.result.descripcion;
                throw datos;
            }
            if (!datos || !datos.length) return null;            
            // console.log(url);
            // console.log("  => " + datos.length);
            let tiempoMayor = null;
            for (let i=0; i<datos.length; i++) {
                let d = datos[i];
                if (d.DATO !== null) {
                    let time = moment.tz(d.FECHA, "UTC");
                    if (!estado[codigoMareografo + "-" + codigoSensor] || time.valueOf() > estado[codigoMareografo + "-" + codigoSensor]) {
                        tiempoMayor = time.valueOf();
                        if (d.DATO) {
                            let dsRow = {codigo_mareografo:codigoMareografo, time:time.valueOf(), prs:null, rad:null};
                            if (codigoSensor == "PRS") dsRow.prs = d.DATO / 1000;
                            else dsRow.rad = d.DATO / 1000;
                            await zrepo.postDataSet("rie.shoa", dsRow);
                        } else {
                            //console.log("Se desacta nivel ", d);
                        }
                        //console.log("Registra nivel:", d.DATO / 1000, codigoMareografo, codigoSensor);
                        estado[codigoMareografo + "-" + codigoSensor] = time.valueOf();
                    }
                }
            }
            if (tiempoMayor) {
                let m = moment.tz(tiempoMayor, config.timeZone);
                //console.log("  => Actualizado a " + m.format("YYYY-MM-DD HH:mm"));
            } else {
                //console.log("  => No actualiza");
            }
            return tiempoMayor;
        } catch(error) {
            throw error;
        }
    }
    async downloadMareografo(codigo) {
        try {
            let tiempoMayor1 = await this.downloadSensor("PRS", codigo);
            let tiempoMayor2 = await this.downloadSensor("RAD", codigo);
            return tiempoMayor1 > tiempoMayor2?tiempoMayor1:tiempoMayor2;
        } catch(error) {
            console.error("Error descargando datos de mareografo " + codigo + ":", error);
            await this.addLog("E", error);
        }
    }

    downloadFile(url) {
        return new Promise((resolve, reject) => {
            fetch(url)
                .then(res => {
                    if (res.status != 200) {
                        res.text()
                            .then(txt => reject(txt))
                            .catch(_ => reject(res.statusText))
                        return;
                    }
                    res.text()
                        .then(txt => {resolve(txt)})
                        .catch(err => {reject(err)})
                })
                .catch(err => {
                    reject(err.name == "AbortError"?"aborted":err)
                });
        })        
    }
}
export {ImportadorSHOA};