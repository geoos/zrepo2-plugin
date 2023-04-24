import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import TimeUtils from "../TimeUtils.js";
import fetch from "node-fetch";
import {DateTime} from "luxon";

const tz = "America/Santiago";

// Valor de tm_cod de la lista de sensores de una estación (sólo en el json)
const variablesPorSensor = {
    ta_c:{variable:"rie.temp", columna:"temperatura"},
    hr:{variable:"rie.humedad", columna:"hum_relativa"},
    vv_ms:{variable:"rie.vel_media_viento", columna:"vel_viento"},
    dv:{variable:"rie.dir_viento", columna:"dir_viento"},
    rs_w:{variable:"rie.indice_uv", columna:"indice_uv"},
    pa_hpa:{variable:"rie.presion_atm", columna:"presion_atm"},
    pp_mm:{variable:"rie.ppt_dia_utc", columna:"ppt_dia"},

}

class ImportadorCEAZA extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "rie-ceaza"}
    get name() {return "Importar datos de Estaciones RIE-CEAZA"}
    get description() {return "Importar datos de Estaciones RIE-CEAZA"}

    async exec(params) {
        try {
            let data = await this.getData();
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

            // Chequear que exista el proveedor "ceaza"
            let row = await ZRepoClient.getValorDimension("rie.proveedor", "ceaza");
            if (!row) {
                await this.addLog("I", "Creando Proveedor CEAZA");
                await ZRepoClient.setFilaDimension("rie.proveedor", {code:"ceaza", name:"CEAZA"});
            }
            // Chequear que existan las estaciones
            // owner = ceaza
            //let rows = await (await fetch("http://www.ceazamet.cl/ws/pop_ws.php?fn=GetListaEstaciones&p_cod=ceazamet&e_owner=ceaza&user=miguel.fernandez.d@pucv.cl&tipo_resp=json")).json();
            let rows = await (await fetch("http://www.ceazamet.cl/ws/pop_ws.php?fn=GetListaEstaciones&p_cod=ceazamet&user=miguel.fernandez.d@pucv.cl&tipo_resp=json")).json();
            let fechaCorte = TimeUtils.lxFromString2("2022-01-01 00:00:00");
            for (let row of rows) {
                // Filtrar por fecha 
                if (!row.e_ultima_lectura || row.e_ultima_lectura.length < 4) continue;
                let f = TimeUtils.lxFromString2(row.e_ultima_lectura);
                if (!f) {
                    await this.addLog("W", "No se pudo interpretar como fecha hora:" + row.e_ultima_lectura + " para estación " + row.e_cod);
                    continue;
                }
                if (f < fechaCorte) continue;
                let codigo = row.e_cod, nombre = row.e_nombre, lat = parseFloat(row.e_lat), lng = parseFloat(row.e_lon),
                    altitud = parseFloat(row.e_altitud), provincia = row.e_cod_provincia;
                codigo = "CZ_" + codigo;
                let e = await ZRepoClient.getValorDimension("rie.estacion", codigo);
                if (!e) {
                    // Buscar sensores de la estación
                    let url = `http://www.ceazamet.cl/ws/pop_ws.php?fn=GetListaSensores&p_cod=ceazamet&e_cod=${row.e_cod}&user=miguel.fernandez.d@pucv.cl&tipo_resp=json`;
                    let sensores = null;
                    try {
                        sensores = await (await fetch(url)).json();
                    } catch(error) {
                        await this.addLog("W", "No se puede obtener la lista de sensores para la estacion " + row.e_cod);
                        continue;
                    }
                    let variables = [];
                    let sensoresImportar = {};
                    for (let s of sensores) {
                        if (variablesPorSensor[s.tm_cod]) {
                            if (variables.indexOf(variablesPorSensor[s.tm_cod].variable) < 0) {
                                variables.push(variablesPorSensor[s.tm_cod].variable);
                                sensoresImportar[s.s_cod] = variablesPorSensor[s.tm_cod];
                                sensoresImportar[s.s_cod].tm_cod = s.tm_cod;
                            }
                        }
                    }

                    if (variables.length) {
                        await this.addLog("I", "Creando Estación: " + codigo + ":" + nombre);
                        await ZRepoClient.setFilaDimension("rie.estacion", {
                            code:codigo, name:nombre, proveedor:"ceaza", tipo:"meteo", comuna:"00",
                            altitud, provincia,
                            lat:lat, lng:lng,
                            variables, sensoresImportar
                        });
                        // Recargar variable 'e'
                        e = await ZRepoClient.getValorDimension("rie.estacion", codigo);
                    }
                }
                if (!e) continue; // estación sin variables
                
                // Buscar valores para sensores
                // Granularidad horaria (mínimo CEAZA)
                let inicio = TimeUtils.nowLx(tz, false).minus({days:1});
                let stInicio = inicio.toFormat("yyyy-LL-dd")
                let fin = TimeUtils.nowLx(tz, false);
                let stFin = fin.endOf("hour").toFormat("yyyy-LL-dd")
                let ultimoDatoEstacion = data[codigo] || inicio.valueOf();
                let mayorFechaLeida = 0;
                let rowsMap = {}; // mapa con key en milisegundos del tiempo (hora)
                for (let codSensor of Object.keys(e.sensoresImportar)) {
                    let url = `http://www.ceazamet.cl/ws/pop_ws.php?fn=GetSerieSensor&p_cod=ceazamet&s_cod=${codSensor}&fecha_inicio=${stInicio}&fecha_fin=${stFin}&user=miguel.fernandez.d@pucv.cl&tipo_resp=json`;
                    let datos = null;
                    try {
                        datos = await (await fetch(url)).json();
                        if (!datos || !datos.serie) throw "No hay datos";
                    } catch(error) {
                        await this.addLog("W", "No se puede obtener los valores del sensor " + codSensor + " para la estacion " + e.code);
                        continue;
                    }
                    for (let row of datos.serie) {
                        let fecha = TimeUtils.lxFromString2(row.fecha);
                        let prom = row.prom;
                        if (fecha.valueOf() > ultimoDatoEstacion && prom !== null) {
                            if (fecha.valueOf() > mayorFechaLeida) mayorFechaLeida = fecha.valueOf();
                            let retRow = rowsMap[fecha.valueOf()];
                            if (!retRow) {
                                retRow = {time:fecha.valueOf(), estacion:codigo};
                                rowsMap[fecha.valueOf()] = retRow;
                            }
                            retRow[e.sensoresImportar[codSensor].columna] = row.prom;
                        }
                    }
                } 
                let rowsList = [];
                for (let time of Object.keys(rowsMap)) {
                    rowsList.push(rowsMap[time]);
                }
                rowsList.sort((a, b) => (a.time - b.time));
                if (rowsList.length) {
                    await this.addLog("I", "Estación " + e.name + ": " + rowsList.length + " nuevos registros de mediciones");
                } else {
                    await this.addLog("I", "Estación " + e.name + ": No hay nuevos datos");
                }

                if (mayorFechaLeida) {
                    data[codigo] = mayorFechaLeida;
                    await this.setData(data);
                    for (let r of rowsList) {
                        await ZRepoClient.accumDataSet("rie.ceaza", r);
                    }
                }
            }
            await ZRepoClient.flushDataSet("rie.ceaza");
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }
}
export {ImportadorCEAZA};