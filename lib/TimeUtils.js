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
    let lx = DateTime.local().setZone(zone)
    if (onlyDate) return lx.startOf("day");
    return lx;
}
function cloneLx(lx) {
    return msToLx(lx.valueOf(), lx.zone.name, false);
}
function stToLx(st, format, zone) {
    return DateTime.fromFormat(st, format, {zone});
}
export default {msToLx, msTojsDate, dateToLx, dateToMs, nowLx, cloneLx, lxToDate, stToLx};