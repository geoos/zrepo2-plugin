import { exec } from "child_process";

class MOTUClient {
    static get instance() {
        if (MOTUClient.singleton) return MOTUClient.singleton;
        MOTUClient.singleton = new MOTUClient();
        return MOTUClient.singleton;
    }

    download(product, date, outDir, outName, config) {
        return new Promise((resolve, reject) => {            
            let cmd = "";
            //cmd += `python3  -m motuclient --motu ${product.url} --service-id ${product.serviceId} --product-id ${product.productId}`;
            cmd += `motuclient --motu ${product.url} --service-id ${product.serviceId} --product-id ${product.productId}`;
            cmd += `        --longitude-min ${config.limits.w} --longitude-max ${config.limits.e}`;
            cmd += `        --latitude-min ${config.limits.s} --latitude-max ${config.limits.n}`;
            cmd += `        --date-min "${date}" --date-max "${date}"`;
            if (product.depths) {
                cmd += `       --depth-min ${product.depths[0]} --depth-max ${product.depths[1]}`;
            }
            cmd += `        ${product.variables.reduce((st, v) => (st + (st?" ":"") + "--variable " + v), "")}`;
            cmd += `        --out-dir ${outDir} --out-name ${outName}`;
            cmd += `        --user ${config.copernicusAuth.user} --pwd ${config.copernicusAuth.pwd}`;
            cmd += `        --auth-mode basic`;
            //console.log("tying ", product);
            //console.log("cmd", cmd);
            exec(cmd, {maxBuffer:1024 * 1024}, (err, stdout, stderr) => {
                if (err) {
                    reject(err);
                    return;
                } else {
                    if (stderr) {
                        reject(stderr);
                        return;
                    }
                    let lines = stdout.split("\n");
                    let errores = lines.reduce((st, line) => {
                        if (line.indexOf("[ERROR]") >= 0) {
                            if (st.length) st += "\n";
                            st += line;
                        }
                        return st;
                    }, "");
                    if (errores.length) reject(errores);
                    else resolve(stdout);
                }
            });
        });
    }
}

export default MOTUClient.instance;