import {ZRepoPluginClient} from "./ZRepoPluginClient.js";
import { Ping } from "./procesos-health/Ping.js";
import {ConfirmadosDiarioCovid} from "./procesos-min-ciencia/ConfirmadosDiarioCovid.js";
import {NuevosDiarioCovid} from "./procesos-min-ciencia/NuevosDiarioCovid.js";
import {ImportadorGoogleSpreadsheets} from "./procesos-multimedia/ImportadorGoogleSpreadsheets.js";
import {ImportadorMonitoreoHiuinay} from "./procesos-huinay/ImportadorMonitoreoHuinay.js";
import { ImportadorCEAZA } from "./procesos-rie/ImportadorCEAZA.js";
import { ImportadorServimet } from "./procesos-rie/ImportadorServimet.js";
import { ImportadorEMA } from "./procesos-rie/ImportadorEMA.js";
import { ImportadorRedMeteo } from "./procesos-rie/ImportadorRedMeteo.js";
import { ImportadorSHOA } from "./procesos-rie/ImportadorSHOA.js";
//import { ImportadorBoyasULagos } from "./procesos-ulagos/ImportadorBoyasULagos.js";
import { ImportadorGFS4 } from "./procesos-noaa/ImportadorGFS4.js";
import { ImportadorWW3 } from "./procesos-noaa/ImportadorWW3.js";
import { ImportadorCopernicus } from "./procesos-copernicus/ImportadorCopernicus.js";
import { ABI_L2_CMI } from "./procesos-goes-16/ABI_L2_CMI.js";
import { RadianceVolcanes } from "./procesos-goes-16/RadianceVolcanes.js";

class ZRepo2Plugin extends ZRepoPluginClient {
    static get instance() {
        if (!ZRepo2Plugin._instance) ZRepo2Plugin._instance = new ZRepo2Plugin();
        return ZRepo2Plugin._instance;
    }

    constructor() {
        super();
        this.registerProcess(Ping);
        this.registerProcess(ConfirmadosDiarioCovid);
        this.registerProcess(NuevosDiarioCovid);
        this.registerProcess(ImportadorGoogleSpreadsheets);
        this.registerProcess(ImportadorMonitoreoHiuinay);
        this.registerProcess(ImportadorCEAZA);
        this.registerProcess(ImportadorServimet);
        this.registerProcess(ImportadorEMA);
        this.registerProcess(ImportadorRedMeteo);
        this.registerProcess(ImportadorSHOA);
        //this.registerProcess(ImportadorBoyasULagos);
        this.registerProcess(ImportadorGFS4);
        this.registerProcess(ImportadorWW3);
        this.registerProcess(ImportadorCopernicus);
        this.registerProcess(ABI_L2_CMI);
        this.registerProcess(RadianceVolcanes);
    }
}

export default ZRepo2Plugin.instance;