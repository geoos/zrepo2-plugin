import fs from "fs";

class GEOServerUtils {
    static get instance() {
        if (GEOServerUtils.singleton) return GEOServerUtils.singleton;
        GEOServerUtils.singleton = new GEOServerUtils();
        return GEOServerUtils.singleton;
    }

    constructor() {
        if (!fs.existsSync(this.downloadPath)) fs.mkdirSync(this.downloadPath);
        if (!fs.existsSync(this.importPath)) fs.mkdirSync(this.importPath);
        if (!fs.existsSync(this.workingPath)) fs.mkdirSync(this.workingPath);

    }

    get timeZone() {return process.env.TIME_ZONE || "America/Santiago"}
    get dataPath() {return "/home/data"}
    get configPath() {return "/home/config"}
    get logPath() {return "/home/log"}
    get downloadPath() {return this.dataPath + "/download"}
    get importPath() {return this.dataPath + "/import"}
    get workingPath() {return this.dataPath + "/working"}

}

export default GEOServerUtils.instance;