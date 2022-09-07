import {ZRepoPluginClient} from "./ZRepoPluginClient.js";
import {ConfirmadosDiarioCovid} from "./procesos-min-ciencia/ConfirmadosDiarioCovid.js";
import {NuevosDiarioCovid} from "./procesos-min-ciencia/NuevosDiarioCovid.js";

class ZRepo2Plugin extends ZRepoPluginClient {
    static get instance() {
        if (!ZRepo2Plugin._instance) ZRepo2Plugin._instance = new ZRepo2Plugin();
        return ZRepo2Plugin._instance;
    }

    constructor() {
        super();
        this.registerProcess(ConfirmadosDiarioCovid);
        this.registerProcess(NuevosDiarioCovid);
    }
}

export default ZRepo2Plugin.instance;