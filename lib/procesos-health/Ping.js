import { ZRepoProcess } from "../ZRepoPluginClient.js";

class Ping extends ZRepoProcess {
    constructor() {
        super();
        this.declareParams([]);
    }

    get code() {return "geoos-ping"}
    get name() {return "Ping Server"}
    get description() {return "Ping Server"}

    async exec(params) {
        try {
            await this.addLog("I", "Pong");
            await this.finishProcess();
        } catch (unhandledError) {
            throw unhandledError;
        }
    }

}
export {Ping};