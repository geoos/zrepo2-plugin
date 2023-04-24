// Propiedades de las celdas:
// https://theoephraim.github.io/node-google-spreadsheet/#/classes/google-spreadsheet-cell?id=methods

import { ZRepoProcess } from "../ZRepoPluginClient.js";
import TimeUtils from "../TimeUtils.js";
import fetch from "node-fetch";
import { GoogleSpreadsheet } from "google-spreadsheet";
import mongo from "../MongoDB.js";

const tz = "America/Santiago";

class ImportadorGoogleSpreadsheets extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "mm-importador-google-spreadsheets"}
    get name() {return "Importar Items de Capas Multimedia desde Planillas de Google"}
    get description() {return "Importar Items de Capas Multimedia desde Planillas de Google"}

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

            let multimediaItemsCol;
            // Inicializar data
            let data = await this.getData();
            if (!data) data = {};

            // Obtenerr lista de capas multimedia del tipo "drive-spreadsheet"
            let portalURL = process.env.PORTAL_URL;            
            let config = await (await fetch(portalURL + "/getPortalConfig.geoos", {
                method: 'POST',
                body: '{"includeX_privateX":true}',
                headers: { 'Content-Type': 'application/json' }
            })).json();
            
            let capasMM = config.capasMultimedia || [];
            capasMM = capasMM.filter(c => c.source == "google-spreadsheet");
            for (let capa of capasMM) {
                // await this.addLog("I", "Actualizando capa " + capa.name);
                const doc = new GoogleSpreadsheet(capa.docID);
                await doc.useServiceAccountAuth({
                    client_email: capa.client_email,
                    private_key: capa.private_key
                });
                await doc.loadInfo(); // loads document properties and worksheets
                // await this.addLog("I", "Planilla Abierta: " + doc.title);
                // console.log(doc.title);
                const sheet = doc.sheetsByIndex[0]; 
                await sheet.loadCells("B2:B2");
                const version = sheet.getCell(1,1).value;
                if (!data[capa.code] || data[capa.code] != version) {                    
                    await this.addLog("I", "Encontrada Nueva versión de: " + capa.name + " -> " + version);
                    await sheet.loadCells();
                    if (!multimediaItemsCol) {
                        multimediaItemsCol = await mongo.collection("multimedia_items");
                    }
                    await multimediaItemsCol.deleteMany({capa:capa.code});
                    let row = 4, n=0;
                    let cell, value;
                    cell = sheet.getCell(row, 0);
                    value = cell?cell.value:null;
                    while(cell && value) {
                        try {                            
                            // Fecha
                            let ms;
                            if (capa.tolerancia) {
                                if (cell.valueType != "numberValue") throw "El valor de la celda fecha no es numérico: " + cell.valueType;
                                if (!cell.effectiveFormat.numberFormat || cell.effectiveFormat.numberFormat.type != "DATE" || cell.effectiveFormat.numberFormat.pattern != "yyyy-mm-dd") {
                                    throw "El formato de la fecha no es 'yyyy-mm-dd': " + JSON.stringify(cell.effectiveFormat);
                                }
                                let date = cell.formattedValue;
                                // Hora
                                cell = sheet.getCell(row, 1);
                                if (cell.valueType != "numberValue") throw "El valor de la celda hora no es numérico: " + cell.valueType;
                                if (!cell.effectiveFormat.numberFormat || cell.effectiveFormat.numberFormat.type != "TIME" || cell.effectiveFormat.numberFormat.pattern != "h:mm") {
                                    throw "El formato de la hora no es 'hh:mm': " + JSON.stringify(cell.effectiveFormat);
                                }
                                date += " " + cell.formattedValue;                                
                                let lx = TimeUtils.stToLx(date, "yyyy-MM-dd H:mm", tz);
                                ms = lx.valueOf();
                                // console.log("fecha-original:", date, "lx:", lx.toFormat("yyyy-MM-dd HH:mm"), "ms:", ms);
                                if (isNaN(ms) || ms < 0 || ms > 7258118400000) throw "La fecha es inválida: " + date;
                            }
                            // Tipo
                            cell = sheet.getCell(row, 2);
                            if (!cell || cell.valueType != "stringValue") throw "El tipo es nulo o no es string";
                            let tipo = cell.formattedValue;
                            if (tipo != "audio" && tipo != "video" && tipo != "imagen") throw "Tipo de contenido '" + tipo + "' no soportado";
                            // Titulo
                            cell = sheet.getCell(row, 3);
                            if (!cell || cell.valueType != "stringValue") throw "El título es nulo o no es string";
                            let titulo = cell.formattedValue;
                            if (!titulo) throw "Debe ingresar un título";
                            // Latitud
                            cell = sheet.getCell(row, 4);
                            if (!cell || cell.valueType != "numberValue") throw "La latitud no es numérica";
                            let latitud = cell.value;
                            if (latitud == null || latitud == undefined) throw "Debe ingresar una latitud";
                            if (latitud < -70 || latitud > -10) throw "El valor de la latitud es inválido: " + latitud;
                            // Longitud
                            cell = sheet.getCell(row, 5);
                            if (!cell || cell.valueType != "numberValue") throw "La longitud no es numérica";
                            let longitud = cell.value;
                            if (longitud == null || longitud == undefined) throw "Debe ingresar una longitud";
                            if (longitud < -120 || longitud > -60) throw "El valor de la longitud es inválido: " + longitud;
                            // Enlace
                            cell = sheet.getCell(row, 6);
                            if (!cell || cell.valueType != "stringValue") throw "El enlace no es un string";
                            let enlace = cell.value;
                            if (!enlace) throw "Debe ingresar un enlace";
                            if (!enlace.startsWith("http")) throw "El enlace es inválido: " + enlace;
                            // Descripcion
                            let descripcion = "";
                            cell = sheet.getCell(row, 7);
                            if (cell && cell.valueType == "stringValue") {
                                descripcion = cell.formattedValue;
                            }
                            // Logo Proveedor
                            let logoProveedor = null;
                            cell = sheet.getCell(row, 8);
                            if (cell && cell.valueType == "stringValue") {
                                logoProveedor = cell.formattedValue;
                            }
                            // URL Proveedor
                            let urlProveedor = null;
                            cell = sheet.getCell(row, 9);
                            if (cell && cell.valueType == "stringValue") {
                                urlProveedor = cell.formattedValue;
                            }
                            
                            // Construir documento
                            let doc = {capa: capa.code, lat:latitud, lng:longitud, tipo, link:enlace, titulo, descripcion};
                            if (capa.tolerancia) doc.tiempo = ms;
                            if (logoProveedor && urlProveedor) {
                                doc.proveedor = {logo:logoProveedor, url:urlProveedor}
                            }
                            await multimediaItemsCol.insertOne(doc);                                
                            n++;
                            sheet.getCell(row, 10).value = "Importada Ok";
                        } catch (error) {
                            console.error(error);
                            await this.addLog("W", "Error en fila: " + (row + 1) + ": " + error.toString());
                            sheet.getCell(row, 10).value = "Error: " + error;
                        }
                        row++;
                        cell = sheet.getCell(row, 0);
                        value = cell?cell.value:null;
                    }
                    // Grabar versión importada
                    data[capa.code] = version;
                    await this.setData(data);
                    // Actualizar versión importada en la planmilla
                    sheet.getCell(1, 7).value = version;
                    await sheet.saveUpdatedCells();
                    await this.addLog("I", "Se agregaron: " + n + " items a la capa");
                } else {
                    await this.addLog("I", "La capa " + capa.name + " mantiene su versión -> " + version);
                }                
            }            
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }
}

export {ImportadorGoogleSpreadsheets};