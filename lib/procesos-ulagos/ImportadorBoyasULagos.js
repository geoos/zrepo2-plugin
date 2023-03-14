import { ZRepoProcess } from "../ZRepoPluginClient.js";
import ZRepoClient from "../ZRepoClient.js";
import TimeUtils from "../TimeUtils.js";
import fetch from "node-fetch";

const tz = "America/Santiago";

const boyas = {
    RLCV: {
        sensores:[{
            codigo: "RLCVMETTA", columna:"temp_aire"
        }, {
            codigo: "RLCVMETHR", columna:"hum_relativa"
        }, {
            codigo: "RLCVMETVV", columna:"vel_viento"
        }, {
            codigo: "RLCVMETDV", columna:"dir_viento"
        }, {
            codigo: "RLCVMETPA", columna:"pres_atm"
        }, {
            codigo: "RLCVMETPP", columna:"ppt_dia"
        }, {
            codigo: "RLCVCTDPH", columna:"ph"
        }, {
            codigo: "RLCVCTDSAL", columna:"salinidad"
        }, {
            codigo: "RLCVCTDCND", columna:"conductividad"
        }, {
            codigo: "RLCVCTDTAG", columna:"temp_agua"
        }, {
            codigo: "RLCVCTDTUR", columna:"turvidez"
        }, {
            codigo: "RLCVCTDPS", columna:"presion"
        }, {
            codigo: "RLCVCTDCLF", columna:"fluorcescencia"
        }, {
            codigo: "RLCVCTDOXD", columna:"oxigeno"
        }, {
            prefijo: "RLCVDC", columna:"dir_corriente", campoProfundidad: "s_altura", max:60, min:1
        }, {
            prefijo: "RLCVVC", columna:"vel_corriente", campoProfundidad: "s_altura", max:60, min:1
        }]
    }
}


class ImportadorBoyasULagos extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "ulagos-boyas"}
    get name() {return "Importar datos de Boyas U.Lagos"}
    get description() {return "Importar datos de Boyas U.Lagos"}


    async exec(params) {
        try {
            let data = await this.getData();
            await this.addLog("I", "Iniciando Importación");

            // Checkear que existan las dimensiones necesarias
            let profundidades = await ZRepoClient.getAllValores("ulagos.profundidad");
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
                    await ZRepoClient.setFilaDimension("ulagos.profundidad", {code, name:p + " [m]"});
                }
            }
            // Chequear que ecistan las boyas (estaciones)
            let codigos = Object.keys(boyas);
            let estacionesCEAZA = null;
            for (let codBoya of codigos) {
                let estacion = await ZRepoClient.getValorDimension("ulagos.estacion", codBoya);
                if (!estacion) {
                    if (!estacionesCEAZA) {
                        estacionesCEAZA = await (await fetch("http://www.ceazamet.cl/ws/pop_ws.php?fn=GetListaEstaciones&p_cod=ceazamet&user=miguel.fernandez.d@pucv.cl&tipo_resp=json")).json();
                    }
                    let e = estacionesCEAZA.find(e => (e.e_cod == codBoya));
                    let row = {code:codBoya, name:e.e_nombre, lat:parseFloat(e.e_lat), lng:parseFloat(e.e_lon)};                    
                    await ZRepoClient.setFilaDimension("ulagos.estacion", row);
                    await this.addLog("I", "Creada boya [" + codBoya + "] " + row.name);
                    estacion = await ZRepoClient.getValorDimension("ulagos.estacion", codBoya);
                }
                // Recorrer sensores y armar filas de dataSets
                let sensores = boyas[codBoya].sensores;
                let inicio = TimeUtils.nowLx(tz, false).minus({days:1});
                let stInicio = inicio.toFormat("yyyy-LL-dd")
                let fin = TimeUtils.nowLx(tz, false);
                let stFin = fin.endOf("hour").toFormat("yyyy-LL-dd")
                let ultimoDatoEstacion = data[codBoya] || inicio.valueOf();
                let mayorFechaLeida = 0;
                // dataSet de valores a -7m
                let rowsMap7m = {}; //time:{row}
                let rowsMapProf = {}; // time-prof:{}
                for (let sensor of sensores) {
                    let codSensor = sensor.codigo;
                    if (codSensor) {
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
                                let retRow = rowsMap7m[fecha.valueOf()];
                                if (!retRow) {
                                    retRow = {time:fecha.valueOf(), estacion:codBoya};
                                    rowsMap7m[fecha.valueOf()] = retRow;
                                }
                                retRow[sensor.columna] = row.prom;
                            }
                        }
                    } else {
                        // es por profundidad
                        for (let idx=sensor.min; idx <= sensor.max; idx++) {
                            codSensor = sensor.prefijo + (idx < 10?"0":"") + idx;
                            let url = `http://www.ceazamet.cl/ws/pop_ws.php?fn=GetSerieSensor&p_cod=ceazamet&s_cod=${codSensor}&fecha_inicio=${stInicio}&fecha_fin=${stFin}&user=miguel.fernandez.d@pucv.cl&tipo_resp=json`;
                            let datos = null;
                            try {
                                datos = await (await fetch(url)).json();
                                if (!datos || !datos.serie) throw "No hay datos";
                            } catch(error) {
                                await this.addLog("W", "No se puede obtener los valores del sensor " + codSensor + " para la estacion " + e.code);
                                continue;
                            }
                            let profundidad = Math.abs(parseFloat(datos[sensor.campoProfundidad]));
                            for (let row of datos.serie) {
                                let fecha = TimeUtils.lxFromString2(row.fecha);
                                let prom = row.prom;
                                if (fecha.valueOf() > ultimoDatoEstacion && prom !== null) {
                                    if (fecha.valueOf() > mayorFechaLeida) mayorFechaLeida = fecha.valueOf();
                                    let key = fecha.valueOf() + "-" + profundidad;
                                    let retRow = rowsMapProf[key];
                                    if (!retRow) {
                                        retRow = {time:fecha.valueOf(), estacion:codBoya, profundidad};
                                        rowsMapProf[fecha.valueOf()] = retRow;
                                    }
                                    retRow[sensor.columna] = row.prom;
                                }
                            }
                        }
                    }
                }
                let rows7mList = [];
                for (let key of Object.keys(rowsMap7m)) {
                    rows7mList.push(rowsMap7m[key]);
                }
                rows7mList.sort((a, b) => (a.time - b.time));
                if (rows7mList.length) {
                    await this.addLog("I", "Boya " + codBoya + ": " + rows7mList.length + " nuevos registros de mediciones en sensores superficiales y -7m");
                } else {
                    await this.addLog("I", "Boya " + codBoya + ": No hay nuevos datos en sensores superficiales y -7m");
                }
                let rowsProfList = [];
                for (let key of Object.keys(rowsMapProf)) {
                    rowsProfList.push(rowsMapProf[key]);
                }
                rowsProfList.sort((a, b) => (a.time == b.time?(a.profundidad - b.profundidad):(a.time - b.time)));
                if (rowsProfList.length) {
                    await this.addLog("I", "Boya " + codBoya + ": " + rows7mList.length + " nuevos registros de mediciones en sensores por profundidad");
                } else {
                    await this.addLog("I", "Boya " + codBoya + ": No hay nuevos datos en sensores por profundidad");
                }
                if (mayorFechaLeida) {
                    data[codBoya] = mayorFechaLeida;
                    await this.setData(data);
                    for (let r of rows7mList) {
                        await ZRepoClient.accumDataSet("ulagos.boyas_sin_profundidad", r);
                    }
                    for (let r of rowsProfList) {
                        await ZRepoClient.accumDataSet("ulagos.boyas_con_profundidad", r);
                    }
                }
            }


            await ZRepoClient.flushDataSet("ulagos.boyas_sin_profundidad");
            await ZRepoClient.flushDataSet("ulagos.boyas_con_profundidad");
            await this.addLog("I", "Finalizando Importación");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }
}
export {ImportadorBoyasULagos};