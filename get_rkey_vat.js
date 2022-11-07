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
const client_id = student.CLIENT_ID+"_RKEY1";
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
const SCHEDULE_CRON_JAM = student.SCHEDULE_CRON_JAM;
const SCHEDULE_CRON_MENIT = student.SCHEDULE_CRON_MENIT;

const SCHEDULE_CRON_JAM_SQL = "09,12,14";
const SCHEDULE_CRON_MENIT_SQL = "13";

const tipe_bc_area = student.TIPE_BC_AREA;
const tipe_bc = student.TIPE_BC;
const list_program = student.LIST_PROGRAM.split(',');

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){
    RKEYVatLogger.info("connected MQTT");
    subs_status_service();
    //cron.schedule('0 0 */1 * * *', async function() {
    cron.schedule('0 '+SCHEDULE_CRON_MENIT_SQL+' '+SCHEDULE_CRON_JAM_SQL+' * * *', async function() {
        console.log('---------------------------------------------------------------------');
        console.log('Cron Job Berjalan Jam '+SCHEDULE_CRON_JAM_SQL+':'+SCHEDULE_CRON_MENIT_SQL+'');
        console.log('---------------------------------------------------------------------');
        
        const arr_cabang = kode_cabang_initial.split(',');
        for(var i = 0;i<arr_cabang.length;i++){

            pub_bc_acuan(arr_cabang[i],"2013058359","CEK_SERVICE","LISTENER_BACKEND_523/","systemctl is-active BC_SQL_"+arr_cabang[i],"Service523","")
            RKEYVatLogger.info("CEK Service BC_SQL_"+arr_cabang[i]+" >  LISTENER_BACKEND_523/");
            RKEYVatLogger.info("PROSES CABANG\t:\t"+arr_cabang[i]);
            

            const Parser_STATION = '01'
            const topic_bc = "BC_SQL/"+arr_cabang[i]+"/"+Parser_STATION+"/" //"VALIDASI_BC_COMMAND/"+arr_cabang[i]+"/"//arr_cabang[i]+'/'+station+'/'
            const Parser_TASK = "BC_SQL";
            const Parser_VERSI = "1.0.1";
            const Parser_HASIL = "-";
            const Parser_OTP = "-";
            const Parser_FROM = initial_from;
            const Parser_CHAT_MESSAGE = "-";
            const Parser_CABANG = arr_cabang[i];
            const Parser_SUB_ID = service_controller.get_subid();
            const Parser_REMOTE_PATH = "-";
            const Parser_LOCAL_PATH = "-";
            const Parser_SOURCE = "IDMCommander";
            const Parser_NAMA_FILE = "-";
            const Parser_ID = service_controller.get_id();

            const COMMAND_SQL = "SELECT '"+Parser_ID+"' AS ID,SUM(IF(sub_bkp='C' AND vnppn='0',1,0)) `C0`,SUM(IF(sub_bkp='C' AND vnppn='10',1,0)) `C10`,SUM(IF(sub_bkp='N' AND vnppn='0',1,0)) `N0`,SUM(IF(sub_bkp='N' AND vnppn='10',1,0)) `N10`,SUM(IF(sub_bkp='Y' AND vnppn='0',1,0)) `Y0`,SUM(IF(sub_bkp='Y' AND vnppn='10',1,0)) `Y10`,SUM(IF((sub_bkp=' ' OR SUB_BKP IS NULL) AND vnppn='0',1,0)) `B0`,SUM(IF((sub_bkp=' ' OR SUB_BKP IS NULL) AND vnppn='10',1,0)) `B10`,COUNT(*) TOTAL,(SELECT `DESC` AS DESKRIPSI FROM const WHERE RKEY = 'VAT') AS RKEY, (select FILTER from vir_bacaprod where jenis='PPNRATE') VIR_BACAPROD,(SELECT `DESC` FROM CONST WHERE RKEY='PJR') AS PJR,(SELECT `PERIOD` FROM CONST WHERE RKEY='PJR') PJR_PERIOD FROM prodmast  WHERE CTGR<>'99';"
            const myObj = {"DB":"pos","PORT":"3306","COMMAND_SQL":COMMAND_SQL,"USER":"kasir","PASS":"cL/EohOGyT3uPR/HmG9zSpHt6/V8zPQKs=VunZtrQfh1,goCkeKArFYJYqmN9DHS/Uyn1HGgFpqrVI=REgE+tC2ZG","LIST_IP":""};
            const Parser_COMMAND = JSON.stringify(myObj);
            const Parser_IP_ADDRESS = "127.0.0.1";
           
            const Parser_TO = +arr_cabang[i]+"/01/";
            const Parser_FILE = "";
            const Parser_TANGGAL_JAM = service_controller.get_tanggal_jam("1");
            const Parser_SN_HDD = "W3T06T4N";

            const res_message = service_controller.CreateMessage(Parser_TASK,
                                                Parser_ID,
                                                Parser_SOURCE,
                                                Parser_OTP,
                                                Parser_TANGGAL_JAM,
                                                Parser_VERSI,
                                                Parser_COMMAND,
                                                Parser_HASIL,
                                                Parser_FROM,
                                                Parser_TO,
                                                Parser_SN_HDD,
                                                Parser_IP_ADDRESS,
                                                Parser_STATION,
                                                Parser_CABANG,
                                                Parser_FILE,
                                                Parser_NAMA_FILE,
                                                Parser_CHAT_MESSAGE,
                                                Parser_REMOTE_PATH,
                                                Parser_LOCAL_PATH,
                                                Parser_SUB_ID
                                                );

            const compressed = await gzip(res_message);  
            client.publish(topic_bc,compressed);
            RKEYVatLogger.info("Publish : "+topic_bc+" Cabang : "+Parser_CABANG+" Station : "+Parser_STATION);
            console.log("=================================================");
            await sleep(50000);
        }
    });
     
    

});

client.on('message',async function(topic, compressed){
   try{        
        //const decompressed = await ungzip(compressed);
        const parseJson = JSON.parse(compressed);
        const IN_HASIL = parseJson.HASIL;
        const IN_CABANG = parseJson.CABANG;
        if(IN_HASIL.includes("active")){
            console.log("SERVICE : "+IN_HASIL);
        }else{
            console.log("SERVICE : "+IN_HASIL);
            pub_bc_acuan(IN_CABANG,"2013058359","RESTART_SERVICE","LISTENER_BACKEND_523/","systemctl restart BC_SQL_"+IN_CABANG,"Service523","")
            await sleep(4000);   
        }


    }catch(exc){
        //console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
        ProdmastLogger.error("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed);
    }
});
 
 

function subs_status_service(){
    var location = initial_from.split('_')[0];
    var topic_command = "RES_SERVICE_BACKEND/2013058359/";
    client.subscribe(topic_command,{qos:0});
    //console.log("subs : "+topic_command);
    ProdmastLogger.info("subs : "+topic_command);
}

function pub_bc_acuan(kode_cabang,task,topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){
        var kode_cabang =  kode_cabang_initial;

        const Parser_TASK = task;
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "IDMCommander";
        const Parser_COMMAND= command_kirim;

        const Parser_OTP= "-";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= initial_from;
        const Parser_TO= "IDMCommandApi";//excel_ip;
        const Parser_SN_HDD= "Z9ANTB8R";
        const Parser_IP_ADDRESS= "192.168.131.104";
        const Parser_STATION= "-";
        const Parser_CABANG = kode_cabang;
        const Parser_FILE = "-";
        const Parser_NAMA_FILE= "-";
        const Parser_CHAT_MESSAGE= "-";
        const Parser_REMOTE_PATH= "-";
        const Parser_LOCAL_PATH= "-";
       
        const Parser_SUB_ID= service_controller.get_subid().toString();




        //-- get list target --//
        const res_message = service_controller.CreateMessage(Parser_TASK,
                                                Parser_ID,
                                                Parser_SOURCE,
                                                Parser_OTP,
                                                Parser_TANGGAL_JAM,
                                                Parser_VERSI,
                                                Parser_COMMAND,
                                                Parser_HASIL,
                                                Parser_FROM,
                                                Parser_TO,
                                                Parser_SN_HDD,
                                                Parser_IP_ADDRESS,
                                                Parser_STATION,
                                                Parser_CABANG,
                                                Parser_FILE,
                                                Parser_NAMA_FILE,
                                                Parser_CHAT_MESSAGE,
                                                Parser_REMOTE_PATH,
                                                Parser_LOCAL_PATH,
                                                Parser_SUB_ID
                                                );
                
        //console.log(res_message);
        //console.log("Topic : "+topic_bc);
        //console.log('------------------------------');
        pub_command(topic_bc,res_message,excel_kdtk,excel_station);
}


client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    process.exit(1)
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}



client.on('message',async function(topic, compressed){
   try{        
        //const decompressed = await ungzip(compressed);
        const parseJson = JSON.parse(compressed);
        const IN_SOURCE = parseJson.SOURCE;
        const IN_TASK = parseJson.TASK;
        const IN_HASIL = parseJson.HASIL;
        const IN_CABANG = parseJson.CABANG;
        //const IN_CMD = parseJson.COMMAND.toString().includes("version.txt");

        var tanggal_message_terima = service_controller.get_tanggal_jam("1");
        //console.log(tanggal_message_terima+" - "+IN_CABANG+" : Progress -> "+IN_HASIL+"%");
        RKEYVatLogger.info(tanggal_message_terima+" - "+IN_CABANG+" : Progress -> "+IN_HASIL+"%");
        if(parseFloat(IN_HASIL) == "100"){
            //console.log("PROSES SELESAI - "+IN_CABANG);
            RKEYVatLogger.info("PROSES SELESAI - "+IN_CABANG);
        }

    }catch(exc){
        console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
        RKEYVatLogger.error("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed);
    }
});

async function pub_command(topic_bc,res_message,toko,station){
    //console.log("Message : "+res_message);
    const compressed = await gzip(res_message);  
    client.publish(topic_bc,compressed);
    //console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    //console.log("===================================================================");
    RKEYVatLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
}