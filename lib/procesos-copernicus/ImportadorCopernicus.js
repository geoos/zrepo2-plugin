import { ZRepoProcess } from "../ZRepoPluginClient.js";
import fs from "fs";
import path from "path";
import geoserver from "../GEOServerUtils.js";
import geoServerUtils from "../GEOServerUtils.js"
import * as  Hjson from "hjson";
import { DateTime } from "luxon";
import { exec } from "child_process";
import { resolve } from "path";

const tz = "America/Santiago";

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
            let config = fs.readFileSync(geoserver.configPath + "/copernicus-downloader.hjson").toString("utf8");
            try {
                config = Hjson.parse(config).copernicusmarineConfig;
            } catch (error) {
                await this.addLog("E", "Error interpretando configuración de productos copernicus: " + error.toString());
                await this.finishProcess();
            }
            
            for (let product of config.products) {
                await this.checkCanceled();
                await this.importaProducto(config, product);
            }

            await this.addLog("I", "Importación Finalizada");
            await this.finishProcess();
        } catch (unhandledError) {
            console.trace(unhandledError);
            throw unhandledError;
        }
    }

    normalizaFecha(fecha, temporalidad) {
        let hh = 0;
        if (temporalidad == "1d") hh = 0;
        else if (temporalidad == "6h") hh = parseInt(fecha.hour / 6) * 6;
        else if (temporalidad == "3h") hh = parseInt(fecha.hour / 3) * 3;
        else if (temporalidad == "1h") hh = fecha.hour;
        else throw "Temporalidad " + temporalidad + " no manejada"
        return fecha.startOf("day").plus({hours: hh});
    }

    incPeriodo(fecha, temporalidad, incremento) {
        let hh = 0;
        if (temporalidad == "1d") hh = 24;
        else if (temporalidad == "6h") hh = 6
        else if (temporalidad == "3h") hh = 3
        else if (temporalidad == "1h") hh = 1
        else throw "Temporalidad " + temporalidad + " no manejada"
        return fecha.plus({hours: incremento * hh});
    }

    creaYLimpiaDirectorio(ruta, limpiar) {
        if (!fs.existsSync(ruta)) {
            fs.mkdirSync(ruta);
        }
        if (limpiar) {
            const files = fs.readdirSync(ruta);
            files.forEach(file => {
                const filePath = path.join(ruta, file);
                if (fs.statSync(filePath).isFile()) {
                    fs.unlinkSync(filePath);
                }
            });
        }
    }

    async importaProducto(config, p) {
        let ahora = this.normalizaFecha(DateTime.fromMillis(Date.now(), {zone:"UTC"}), p.temporality);
        let state = (await this.getData()) || {};
        let ultima = state[p.geoosDataSetCode]?DateTime.fromISO(state[p.geoosDataSetCode]):null;
        let inicio, fin;
        if (ultima) {
            inicio = this.incPeriodo(ultima, p.temporality, 1);
        } else {
            inicio = this.incPeriodo(ahora, p.temporality, p.searchPeriod[0]);
        }
        fin = this.incPeriodo(ahora, p.temporality, p.searchPeriod[1]);
        this.creaYLimpiaDirectorio(geoServerUtils.downloadPath + "/cope");
        for (let f = inicio; f <= fin; f = this.incPeriodo(f, p.temporality, 1)) {
            await this.checkCanceled();
            let descargo = await this.descargaYProcesaTiempo(config, p, f);
            if (descargo) {
                state[p.geoosDataSetCode] = f.toISO();
                await this.setData(state);
            }
        }
    }

    formateaFechaParaImport(fecha, temporalidad) {
        if (temporalidad == "1d") return fecha.toFormat("yyyy-MM-dd");
        else if (temporalidad == "6h") return fecha.toFormat("yyyy-MM-dd_HH'_00_00'");
        else if (temporalidad == "3h") return fecha.toFormat("yyyy-MM-dd_HH'_00_00'");
        else if (temporalidad == "1h") return fecha.toFormat("yyyy-MM-dd_HH'_00_00'");
        else throw "Tempooralidad " + temporalidad + " no reconocida";
    }

    async descargaYProcesaTiempo(config, p, fecha) {
        let filePath = geoServerUtils.downloadPath + "/cope/" + p.geoosDataSetCode;
        this.creaYLimpiaDirectorio(filePath, true);
        await this.addLog("I", "Buscando " + p.geoosDataSetCode + " para " + fecha.toFormat("dd/MM/yyyy HH:mm"));
        let cmd = ` copernicusmarine subset `;
            cmd += `--username ${config.userName} --password ${config.password} `;
            cmd += `-x ${config.limits.w} -X ${config.limits.e} `;
            cmd += `-y ${config.limits.s} -Y ${config.limits.n} `;
            cmd += `-t ${fecha.toFormat("yyyy-MM-dd'T'HH:mm:ss")} -T ${fecha.toFormat("yyyy-MM-dd'T'HH:mm:ss")} `;
        if (p.levelLimits) {
            cmd += `-z ${p.levelLimits[0]} -Z ${p.levelLimits[1]} `;
        }
        cmd += `-i ${p.copernicusDataSetId} --force-download -o ${filePath}`

        try {
            await this.ejecutaComando(cmd);
            // Buscar el archivo descargado
            const files = fs.readdirSync(filePath);
            if (!files.length) {
                await this.addLog("W", "No se descargó archivo");
                return false;                    
            }
            let importName = geoServerUtils.importPath + "/" + p.geoosDataSetCode + "_" + this.formateaFechaParaImport(fecha, p.temporality) + ".nc";
            if (fs.existsSync(importName)) fs.unlinkSync(importName);
            fs.renameSync(filePath + "/" + files[0], importName);
            await this.addLog("I", "Archivo movido a " + importName);
            return true;
        } catch (error) {
            await this.addLog("W", error.toString());
            return false;
        }
    }

    ejecutaComando(cmd) {
        return new Promise((resolve, reject) => {
            exec(cmd, {maxBuffer:1024 * 1024}, (err, stdout, stderr) => {
                if (err) {
                    reject(err);
                    return;
                } else {
                    if (stderr) resolve(stderr);
                    else resolve(stdout);
                }
            });
        })
    }
}
export {ImportadorCopernicus};