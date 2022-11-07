var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('./connection/mysql_connection');
var service_controller = require('./controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');
const {userLogger,RKEYVatLogger,LockRegistryLogger,ProdmastLogger} = require('./controller/logger');


const jsonString = fs.readFileSync("appconfig.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = student.CLIENT_ID+"_VERSI_PROGRAM";
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
//const topic_bc = student.TOPIC_BC;
const initial_from = student.INITIAL_FROM;

const kode_cabang_initial = student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;


const is_dinamis_query = student.IS_DINAMIS_QUERY; 
const res_call_initial = student.CALL_INITIAL; 
const COMMAND_DIR_D = student.COMMAND_DIR_D;
const COMMAND_DIR_ECAD = student.COMMAND_DIR_ECAD;
const SCHEDULE_CRON_JAM = "08";
const SCHEDULE_CRON_MENIT = "30";
const tipe_bc_area = student.TIPE_BC_AREA;
const tipe_bc = student.TIPE_BC;
const station = student.STATION.split(',');
const list_program = student.LIST_PROGRAM.split(',');

 
  
//subs_progress_bc();
//cron.schedule('0 0 */1 * * *', async function() {
console.log("Berjalan pada jam : "+SCHEDULE_CRON_JAM+" Menit : "+SCHEDULE_CRON_MENIT);
cron.schedule('0 '+SCHEDULE_CRON_MENIT+' '+SCHEDULE_CRON_JAM+' * * *', async function() {
         
});
 