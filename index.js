import express from "express"
import bodyParser from "body-parser";
import http from "http";
import fs from "fs";
import zrepo2Plugin from "./lib/ZRepo2Plugin.js";
import zRepoClient from "./lib/ZRepoClient.js";
import mongoDB from "./lib/MongoDB.js";

async function startHTTPServer() {
    try {
        try {
            await mongoDB.init();
        } catch(error) {
            console.error("Error conectando a MongoDB", error);
            throw error;
        }

        let version = "?";
        try {
            let txt = fs.readFileSync("./build.sh").toString();
            txt = txt.split("\n")[0];
            let p = txt.indexOf("=");
            version = txt.substring(p+1);
        } catch(error) {
            console.error(error);
        }
        const app = express();
        app.use("/", express.static("www"));
        app.use(bodyParser.urlencoded({limit: '50mb', extended:true}));
        app.use(bodyParser.json({limit: '50mb', extended: true}));

        app.use((req, res, next) => {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");
            next();
        });
        try {
            await zRepoClient.init(process.env.ZREPO_URL, process.env.ZREPO_TOKEN);
            await zrepo2Plugin.init(app, process.env.ZREPO_URL, "zr2pi");
        } catch(error) {
            console.error("Error conectando a ZRepo", error);
            throw error;
        }

        const port = process.env.HTTP_PORT || 8090;
        const httpServer = http.createServer(app);
        httpServer.listen(port, "::", async _ => {
            console.log("[ZRepo2 PlugIn - Procesos de Carga - " + version + "] HTTP Web Server listenning at port " + port);
        });
    } catch(error) {
        console.error("Can't start web server", error);
        console.error("Exit (-1)")
        process.exit(-1);
    }
}

startHTTPServer();

