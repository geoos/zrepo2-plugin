import {DateTime} from "luxon";

function msToLx(ms, zone) {
    // Convierte un tiempo en ms UTC a DateTime de Luxon
    return DateTime.fromMillis(ms, {zone});
}
function msTojsDate(ms, zone, onlyDate) {
    // Convierte un tiempo en ms UTC a Date de javascript considerando las correcciones
    // de la zona.
    let lx = msToLx(ms, zone);
    if (onlyDate) return new Date(lx.year, lx.month-1, lx.day);
    else return new Date(lx.year, lx.month-1, lx.day, lx.hour, lx.minute, lx.second, 0);
}

function dateToLx(date, zone, onlyDate) {
    // Convierte un javascript Date a Luxon DateTime asegurando que cada campo (y,m,d,h,s,ms) 
    // se mantiene seg
    if (onlyDate) 
        return DateTime.fromObject(
            {year:date.getFullYear(), month:date.getMonth()+1, day:date.getDate(), zone:zone}
        );
    else
        return DateTime.fromObject(
            {year:date.getFullYear(), month:date.getMonth()+1, day:date.getDate(), hour:date.getHours(), minute:date.getMinutes(), second:date.getSeconds(), millisecond:0, zone:zone}
        );
}
function lxToDate(lx,onlyDate){
    if(onlyDate){
        return new Date(lx.year,lx.month-1,lx.day)
    }else{
        return new Date(lx.year,lx.month-1,lx.day,lx.hour,lx.minute,lx.second)
    }
    
}
function dateToMs(date, zone, onlyDate) {
    return dateToLx(date, zone, onlyDate).valueOf();
} 
function nowLx(zone, onlyDate) {    
    let lx = DateTime.fromMillis(Date.now(), {zone});
    if (onlyDate) return lx.startOf("day");
    return lx;
}
function cloneLx(lx) {
    return msToLx(lx.valueOf(), lx.zone.name, false);
}
function stToLx(st, format, zone) {
    let lx = DateTime.fromFormat(st, format, {zone});
    return lx;
}

function lxFromString1(st, tz) {
    // formato esperado "YYYY-MM-DD HH:MM"
    let p0 = st.indexOf("-");
    let ano = parseInt(st.substring(0, p0));
    p0++;
    let p1 = st.indexOf("-", p0);
    let mes = parseInt(st.substring(p0, p1));
    p0 = p1 + 1;
    p1 = st.indexOf(" ", p0);
    let dia = parseInt(st.substring(p0, p1));
    p0 = p1 + 1;
    p1 = st.indexOf(":", p0);
    let hora = parseInt(st.substring(p0, p1));
    let minuto = parseInt(st.substring(p1 + 1));

    let lx = DateTime.fromObject(
        {year:ano, month:mes, day:dia, hour:hora, minute:minuto, second:0, millisecond:0}, {zone:tz}
    );
    return lx;
}

function lxFromString2(st, tz) {
    // formato esperado "YYYY-MM-DD HH:MM:SS"
    let p0 = st.indexOf("-");
    let ano = parseInt(st.substring(0, p0));
    p0++;
    let p1 = st.indexOf("-", p0);
    let mes = parseInt(st.substring(p0, p1));
    p0 = p1 + 1;
    p1 = st.indexOf(" ", p0);
    let dia = parseInt(st.substring(p0, p1));
    p0 = p1 + 1;
    p1 = st.indexOf(":", p0);
    let hora = parseInt(st.substring(p0, p1));
    p0 = p1 + 1;
    p1 = st.indexOf(":", p0);
    let minuto = parseInt(st.substring(p0, p1));
    let segundo = parseInt(st.substring(p1 + 1));

    try {
        return DateTime.fromObject(
            {year:ano, month:mes, day:dia, hour:hora, minute:minuto, second:segundo, millisecond:0}, {zone:tz}
        );
    } catch(error) {
        console.error("No se puede convertir a fecha", st, ano, mes, dia, hora, minuto, segundo);
        return null;
    }
}
export default {msToLx, msTojsDate, dateToLx, dateToMs, nowLx, cloneLx, lxToDate, stToLx, lxFromString1, lxFromString2};