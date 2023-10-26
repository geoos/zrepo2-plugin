import { ZRepoProcess } from "../ZRepoPluginClient.js";
import { google } from "googleapis";
import { GoogleSpreadsheet } from "google-spreadsheet";
import ZRepoClient from "../ZRepoClient.js";
import * as fs from 'fs';
import TimeUtils from "../TimeUtils.js";
import fetch from "node-fetch";
import { DateTime } from "luxon";
import mongo from "../MongoDB.js";

const tz = "America/Santiago";
const credentialFilename = "geoos-monitoreo-huinay-055989a4acee.json";
const scopes = ["https://www.googleapis.com/auth/drive.metadata.readonly", "https://www.googleapis.com/auth/drive"];
const auth = new google.auth.GoogleAuth({keyFile: credentialFilename, scopes: scopes});
const drive = google.drive({ version: "v3", auth });

const fechaMinima = TimeUtils.lxFromString1("2000-01-01 00:00", tz);
const fechaMaxima = TimeUtils.lxFromString1("2100-01-01 00:00", tz);

class ImportadorMonitoreoHiuinay extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "monitoreo-importador-huinay"}
    get name() {return "Buscar e Importar Nuevas Planillas de Monitoreo de Huinay"}
    get description() {return "Buscar e Importar Nuevas Planillas de Monitoreo de Huinay"}

    async exec(params) {
        try {
            await this.addLog("I", "Iniciando Importación");
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

            // Checkear que existan las dimensiones necesarias
            let profundidades = await ZRepoClient.getAllValores("huinay.profundidad");
            if (!profundidades.length) {
                await this.addLog("W", "No existen profundidades, se crearán los datos iniciales");
            }
            let profMap = profundidades.reduce((map, p) => {
                map[p.code] = true;
                return map;
            }, {});            
            for (let p=0; p<=500; p++) {
                let code = "" + p;
                if (profMap[code] === undefined) {
                    await ZRepoClient.setFilaDimension("huinay.profundidad", {code, name:p + " [m]"});
                }
            }
            let portalURL = process.env.PORTAL_URL;            
            let config = await (await fetch(portalURL + "/getPortalConfig.geoos", {
                method: 'POST',
                body: '{"includeX_privateX":true}',
                headers: { 'Content-Type': 'application/json' }
            })).json();
            
            let capasM = config.capasMonitoreo || [];
            this.capaM = capasM.find(c => c.code == "monitoreo-huinay");
            if (!this.capaM) throw "No se encontró la capa 'monitoreo-huinay' en portal.hjson/capasMonitoreo";

            await this.addLog("I", "Autenticando en Google Drive");
            let files;
            try {
                let res = await drive.files.list({
                    pageSize: 500, fields: "nextPageToken, files(id, name, mimeType)",
                    q: `'${this.capaM.parentFolderId}' in parents and trashed = false`
                });
                files = res.data.files.filter(f => f.mimeType != "application/vnd.google-apps.folder");
            } catch (error) {
                console.error(error);
                await this.addLog("E", error.toString());
                await this.finishProcess();
                return;
            }
            if (!files.length) {
                this.addLog("I", "No hay nuevos archivos para procesar");
            } else {
                this.addLog("I", "Se encontraron " + files.length + " nuevos archivos para procesar");
            }
            for (let f of files) {
                if (f.mimeType != "application/vnd.google-apps.spreadsheet") {
                    await this.mueveACarpeta(f, this.capaM.discardedFolderId, "Carpeta de Descartados", true);
                } else {
                   let moverA = await this.importaPlanilla(f);
                   if (moverA == "errores") await this.mueveACarpeta(f, this.capaM.withErrorsFolderId, "Carpeta de archivos Con Errores", true);
                   else if (moverA == "importados") await this.mueveACarpeta(f, this.capaM.importedFolderId, "Carpeta de Importados", false);
                }
            }
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

    async mueveACarpeta(f, folderId, nombreCarpeta, warning) {
        try {
            this.addLog(warning?"W":"", "Moviendo archivo " + f.name + " de tipo " + f.mimeType + " a " + nombreCarpeta);
            let currentFolderId = this.capaM.parentFolderId;
            let files = await drive.files.update({
                fileId: f.id, addParents: folderId, removeParents: currentFolderId
            });
        } catch (error) {
            console.error(error);
            throw error;
        }
    }

    async importaPlanilla(f) {
        let conErrores = false;
        try {            
            let json = JSON.parse(fs.readFileSync(credentialFilename));            
            const doc = new GoogleSpreadsheet(f.id);
            await doc.useServiceAccountAuth({
                client_email: json.client_email,
                private_key: json.private_key
            });
            await doc.loadInfo();
            let nSheets = doc.sheetCount;
            await this.addLog("I", "Importando " + nSheets + " hojas desde " + doc.title);
            // Buscar Sheet de control
            let idxControl = -1;
            for (let iSheet=0; iSheet<nSheets; iSheet++) {
                let s = doc.sheetsByIndex[iSheet];
                if (s.title.toLowerCase() == "control") {
                    idxControl = iSheet;
                    break;
                }
            }
            if (idxControl < 0) throw "No se encontró la hoja de control en la planilla";
            this.control = doc.sheetsByIndex[idxControl];
            await this.control.loadCells();
            let cellEstado = this.control.getCell(3, 1);
            if (!cellEstado || !cellEstado.value) throw "No se encontró la celda con estado en la hoja de control";
            let estado = cellEstado.value.toLowerCase();
            await this.addLog("I", "Estado Actual: " + estado);
            if (estado != "esperando importación") {
                await this.addLog("I", "  => Se descarta por estado");
                return "mantener";
            }
            // Borrar posibles mensajes desde alguna importación anterior
            let idxMensaje = 6;
            while (idxMensaje < 1000) {
                try {
                    let cell = this.control.getCell(idxMensaje, 0);
                    if (!cell || !cell.value) break;
                    cell.value = "";
                    idxMensaje++;
                } catch (error) {
                    console.error(error);
                    break;
                }
            }
            this.idxMensaje = 6;

            for (let iSheet=0; iSheet<nSheets; iSheet++) {
                let sheet = doc.sheetsByIndex[iSheet];
                if (sheet.title.toLowerCase() == "control") continue;
                await sheet.loadCells();                
                await this.addLog("I", "  -> Procesando estación " + sheet.title);
                this.agregaMensaje("-> Procesando estación " + sheet.title);
                try {
                    // Checkear que el nombre del Sheet sea el mismo código de la estación declarado en B1
                    let cell = sheet.getCell(0, 1);
                    if (cell.valueType != "stringValue" || cell.value != sheet.title) throw "El titulo del Sheet es diferentet al código de estación de monitoreo en la celda B1. Se descarta el Sheet";
                    // Asegurar que es un código de estación y que existe
                    let codigoEstacion = cell.value.trim();
                    if (codigoEstacion.length < 3 || codigoEstacion.length > 20) throw "El código de estación '" + codigoEstacion + "' es inválido. Se descarta";
                    let latCell = sheet.getCell(0, 3), lngCell = sheet.getCell(0, 5);
                    let lat = latCell?parseFloat(latCell.value):null, lng = lngCell?parseFloat(lngCell.value):null;
                    if (isNaN(lat) || isNaN(lng)) throw "Latitud o Longitud Inválidas. Se descarta";
                    let row = await ZRepoClient.getValorDimension("huinay.estacion", codigoEstacion);
                    if (row) {
                        // Actualizar lat/lng
                        row.lat = lat; row.lng = lng;                    
                    } else {
                        // Crear
                        row = {code: codigoEstacion, name: "Estación Monitoreo " + codigoEstacion, lat, lng}
                    }
                    await ZRepoClient.setFilaDimension("huinay.estacion", row);
                    // Checkear fecha
                    let cellFecha;
                    try {
                        cellFecha = sheet.getCell(1,1);
                    } catch (error) {                        
                    }
                    if (!cellFecha || !cellFecha.value) throw "No se encontró la celda con la fecha";
                    let stFecha = cellFecha.formattedValue;
                    let lxFecha = TimeUtils.lxFromString1(stFecha, tz);
                    if (lxFecha < fechaMinima || lxFecha > fechaMaxima) throw "La fecha de muestreo es inválida: " + stFecha;
                    await this.addLog("I", "  => Importando para fecha " + stFecha);
                    await this.addLog("I", "Eliminando valores para el mismo día");
                    this.eliminaDatosEstacion(codigoEstacion, lxFecha);
                    let filaMuestra = 4, n=0;
                    while (filaMuestra < 10000) {
                        let hayDatos = false, temperatura, salinidad, densidad, oxigeno, fluorescencia, par, ph;
                        let m = this.getCellValue(sheet, filaMuestra, 0);
                        if (m === null) break;
                        temperatura = this.getCellValue(sheet, filaMuestra, 1);
                        salinidad = this.getCellValue(sheet, filaMuestra, 2);
                        densidad = this.getCellValue(sheet, filaMuestra, 3);
                        oxigeno = this.getCellValue(sheet, filaMuestra, 4);
                        fluorescencia = this.getCellValue(sheet, filaMuestra, 5);
                        par = this.getCellValue(sheet, filaMuestra, 6);
                        ph = this.getCellValue(sheet, filaMuestra, 7);
                        hayDatos = temperatura != null || salinidad != null || densidad != null || oxigeno != null || fluorescencia != null || par != null || ph != null;
                        if (hayDatos) {
                            n++;
                            await ZRepoClient.accumDataSet("huinay.monitoreo", {
                                time: lxFecha.valueOf(), timestamp: lxFecha.valueOf(),
                                codigoEstacion,
                                profundidad:"" + m,
                                temperatura, salinidad, densidad, oxigeno, fluorescencia, par, ph
                            })
                            if (n % 100) {
                                await ZRepoClient.flushDataSet("huinay.monitoreo");
                            }
                        }

                        filaMuestra++;
                    }
                    await ZRepoClient.flushDataSet("huinay.monitoreo");
                    await this.addLog("I", "Importadas " + n + " filas de datos de muestreo para la estación " + codigoEstacion);
                    this.agregaMensaje("  => Importadas " + n + " filas de datos de muestreo para la estación " + codigoEstacion)
                } catch(error) {
                    console.error(error);
                    await this.addLog("E", error.toString());
                    this.agregaMensaje("Error: " + error.toString());
                    conErrores = true;
                }
            }
            if (conErrores) this.control.getCell(3, 1).value = "Con Errores";
            else this.control.getCell(3, 1).value = "Importado";
            await this.control.saveUpdatedCells();
            return conErrores?"errores":"importados";
        } catch (error) {
            console.error(error);
            throw error;
        }
    }

    getCellValue(sheet, row, col) {
        try {
            let cell = sheet.getCell(row, col);
            if (!cell) return null;
            let v = parseFloat(cell.value);
            if (isNaN(v)) return null;
            return v;
        } catch (error) {
            return null;
        }
    }
    async agregaMensaje(msg) {
        try {
            let cell;
            try {
                cell = this.control.getCell(this.idxMensaje, 0);
            } catch (error) {
                cell = null;
            }
            if (!cell) cell = this.control.addRow([msg]);
            else cell.value = msg;
            this.idxMensaje++;
        } catch (error) {
            console.error(error);
            throw error;
        }
    }

    async eliminaDatosEstacion(codigoEstacion, dia) {
        try {            
            await ZRepoClient.deleteDataSetRows("huinay.monitoreo", dia.startOf("day").valueOf(), dia.endOf("day").valueOf(), {codigoEstacion});
            await ZRepoClient.deletePeriod("huinay.temp_profundidad", dia.startOf("day").valueOf(), dia.endOf("day").valueOf(), true, true, {estacion:codigoEstacion});
        } catch (error) {
            console.error(error);
        }
    }
}
export {ImportadorMonitoreoHiuinay};