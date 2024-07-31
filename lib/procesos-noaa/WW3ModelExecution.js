import url from "url";
import https from "https";
const http = https;

class WW34ModelExecution {
    constructor(time) {
        this.time = time;
        let hours = this.time.hours();
        if (hours % 6) {
            hours = 6 * parseInt(hours / 6);
            this.time.hours(hours);
        }
    }

    get NOAAWW34Url() {
        let url = process.env.NOAA_WW3_URL || "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/";        
        if (!url.endsWith("/")) url += "/";
        return url;
    }


    getNOAAUrl(forecastHour, extension) {  
        if (extension === undefined) extension = "";      
        let hh = "" + forecastHour;
        if (hh.length < 3) hh = "0" + hh;
        if (hh.length < 3) hh = "0" + hh;
        //return this.NOAAWW34Url + "gfs." + this.time.format("YYYYMMDD") + "/" + this.time.format("HH") + "/atmos/gfs.t" + this.time.format("HH") + "z.pgrb2.0p25.f" + hh + extension;        
        return this.NOAAWW34Url + "gfs." + this.time.format("YYYYMMDD") + "/" + this.time.format("HH") + "/wave/gridded/gfswave.t" + this.time.format("HH") + "z.global.0p25.f" + hh + extension;        
    }

    isPublished() {
        return new Promise((resolve, reject) => {            
            // Search file ".inv" to forecast hour "0000"
            let testUrl = this.getNOAAUrl(0, ".grib2.idx");
            // Test file existance (model was run for hat hour)
            let parsed = url.parse(testUrl);
            let options = {
                method:"HEAD",
                host:parsed.host,
                path: parsed.pathname,
                protocol:parsed.protocol
            };
            try {
                let req = http.request(options, r => {
                    if (r.statusCode == 200) {
                        resolve(true);
                    } else if (r.statusCode == 404) {
                        resolve(false);
                    } else if (r.statusCode == 403) {
                        resolve(false);
                    } else {
                        reject("[" + r.statusCode + "] " + r.statusMessage);
                    }
                }).on("error", err => {
                    reject(err);
                })
                req.end();
            } catch(error) {
                reject(error);
            }
        });        
    }

    dec() {
        this.time.subtract(6, "hours");
    }
}

export {WW34ModelExecution};