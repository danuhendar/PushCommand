var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('../connection/mysql_connection');
var service_controller = require('../controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');
const {userLogger,RKEYVatLogger,LockRegistryLogger,ProdmastLogger,programInstalledLogger,memoryInfoLogger,BootTimeLogger,SPDMastLogger} = require('../controller/logger');



const jsonString = fs.readFileSync("../config/spdmast.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = student.CLIENT_ID+"_"+service_controller.get_id();;
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
const tipe_bc_area = student.TIPE_BC_AREA;
const tipe_bc = student.TIPE_BC;
//202212020100
const history_tanggal_table = service_controller.get_subid().toString().substring(2,8);


var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT");
    SPDMastLogger.info("connected MQTT");
    sleep(5000);    
});

// ------------------------ SCHEDULE CRON JOB --------------------//
console.log("Berjalan pada Menit : "+SCHEDULE_CRON_MENIT);
    cron.schedule('00 */15 * * * *', async function() {
        const arr_cabang = kode_cabang_initial.split(',');
        for(var i = 0;i<arr_cabang.length;i++){
            SPDMastLogger.info("Proses Cabang-"+arr_cabang[i]);  
            const ip = arr_cabang[i]
            //-- get station is_induk != '1' --//
            //var res_station = "01";
            //console.log("res-station : "+res_station)
            //-- loop station --//
                    var sql_get_ip = "SELECT GROUP_CONCAT(TOKO) AS TOKO FROM tokomain WHERE KDCAB = '"+ip+"' AND STATION = '01' ORDER BY TOKO ASC;";
                    console.log(sql_get_ip);
                    mysqlLib.executeQuery(sql_get_ip).then((d) => {
                            //console.log(d[a])
                            const get_kdtk = d[0].TOKO;
                            const topic_bc = "Trigger/SQL/MonitoringSPDMast/REG4/";//"BC_SQL/"+ip+"/"+d[a].IP+"/";
                            //console.log("TOPIC BC : "+topic_bc);
                            //console.log("-------------------------------------------");
                            const kdtk = 'ALL TOKO '+topic_bc;
                            SPDMastLogger.info("Proses Broadcast - "+topic_bc);
                            try{
                                //const command_kirim = "{\"TIPE_BC\":\"SOME STORES\",\"PORT\":\"3306\",\"COMMAND_SQL\":\"CREATE TABLE IF NOT EXISTS bck_reg_spdmast_"+history_tanggal_table+" select * from spdmast_copy;\\r\\nDROP TABLE IF EXISTS act_reg_spdmast;\\r\\nCREATE TABLE act_reg_spdmast SELECT * FROM spdmast_copy GROUP BY prdcd;\\r\\nUPDATE act_reg_spdmast SET supco = '"+d[a].IP+"';\\r\\nDELETE FROM spdmast_copy;\\r\\nINSERT INTO spdmast_copy SELECT * FROM act_reg_spdmast;\",\"PASS\":\"goCkeKArFYJYqmN9DHS/Uyn1HGgFpqrVI=REgE+tC2ZG,cL/EohOGyT3uPR/HmG9zSpHt6/V8zPQKs=VunZtrQfh1\",\"USER\":\"kasir\",\"LIST_IP\":\""+topic_bc+"\",\"DB\":\"pos\"}";      
                                const command_kirim = "CREATE TABLE IF NOT EXISTS bck_reg_spdmast_"+history_tanggal_table+" select * from spdmast;DROP TABLE IF EXISTS act_reg_spdmast;CREATE TABLE act_reg_spdmast SELECT * FROM spdmast GROUP BY prdcd;UPDATE act_reg_spdmast SET supco = '"+ip+"';DELETE FROM spdmast;INSERT INTO spdmast SELECT * FROM act_reg_spdmast;"
                                //const command_kirim = "SELECT * FROM mtran limit 0,1";
                                const chat_message = {LOCATION:"REG4",KDTK:get_kdtk,NIK_USER:"2013058359"};
                                const res_chat_message = JSON.stringify(chat_message);
                                console.log(res_chat_message);
                                //console.log("COMMAND KIRIM : "+command_kirim);
                                //console.log("-------------------------------------------");
                                pub_bc_acuan("BC_SQL",topic_bc,command_kirim,kdtk,"",ip,res_chat_message);
                                console.log("================================================="); 
                            }catch(exc){
                                console.log("ERROR : "+exc);
                            }
                            sleep(5000);
                    });

                    await sleep(10000);
        }
    });
 

function pub_bc_acuan(task,topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip,res_chat_message){

    /*
    {
        "TASK": "BC_SQL",
        "STATION": "-",
        "VERSI": "2.2.8.5",
        "HASIL": "-",
        "OTP": "NTIwNTAwNzMzfDIwMjItMTItMDYgMTc6NDA6NTF8MTUwfFJFRzQ=",
        "FROM": "REG4_2021005005_172.28.64.1_7E9F8055_202212060450",
        "CHAT_MESSAGE": "-",
        "CABANG": "G236",
        "SUB_ID": "20221206054050",
        "REMOTE_PATH": "-",
        "SOURCE": "IDMReporter",
        "NAMA_FILE": "-",
        "LOCAL_PATH": "-",
        "COMMAND": "{\"TIPE_BC\":\"SOME STORES\",\"PORT\":\"3306\",\"COMMAND_SQL\":\"CREATE TABLE IF NOT EXISTS bck_reg_spdmast_221106 select * from spdmast_copy;\\r\\nDROP TABLE IF EXISTS act_reg_spdmast;\\r\\nCREATE TABLE act_reg_spdmast SELECT * FROM spdmast_copy GROUP BY prdcd;\\r\\nUPDATE act_reg_spdmast SET supco = 'G236';\\r\\nDELETE FROM spdmast_copy;\\r\\nINSERT INTO spdmast_copy SELECT * FROM act_reg_spdmast;\",\"PASS\":\"goCkeKArFYJYqmN9DHS\\/Uyn1HGgFpqrVI=REgE+tC2ZG,cL\\/EohOGyT3uPR\\/HmG9zSpHt6\\/V8zPQKs=VunZtrQfh1\",\"USER\":\"kasir\",\"LIST_IP\":\"10.77.8.66,10.77.9.162\",\"DB\":\"pos\"}",
        "IP_ADDRESS": "127.0.0.1",
        "ID": "202212060540",
        "TO": "10.77.9.162",
        "FILE": "",
        "TANGGAL_JAM": "2022-12-06_17:40:50",
        "SN_HDD": "7E9F8055"
    }
    */

        var kode_cabang =  kode_cabang_initial;
        const Parser_TASK = task;
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "IDMReporter";
        const Parser_OTP= "2235";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= "MonitoringSPDMast";
        const Parser_TO= "IDMReporter";
        const Parser_SN_HDD= "Z9ANTB8R";
        const Parser_IP_ADDRESS= "192.168.131.104";
        const Parser_STATION= "-";
        const Parser_CABANG = kode_cabang_initial;
        const Parser_FILE = "-";
        const Parser_NAMA_FILE= "-";
        const Parser_CHAT_MESSAGE = res_chat_message;
        const Parser_REMOTE_PATH= "-";
        const Parser_LOCAL_PATH= "-";
        const Parser_COMMAND= command_kirim;
        const Parser_SUB_ID= service_controller.get_subid().toString();
        /*
        const myObj = {
                        "TASK":Parser_TASK, 
                        "ID":Parser_ID, 
                        "SOURCE":Parser_SOURCE,
                        "OTP":Parser_OTP,
                        "TANGGAL_JAM":Parser_TANGGAL_JAM,
                        "VERSI":Parser_VERSI,
                        "COMMAND":Parser_COMMAND,
                        "HASIL":Parser_HASIL,
                        "FROM":Parser_FROM,
                        "TO":Parser_TO,
                        "SN_HDD":Parser_SN_HDD,
                        "IP_ADDRESS":Parser_IP_ADDRESS,
                        "STATION":Parser_STATION,
                        "CABANG":kode_cabang,
                        "FILE":Parser_FILE,
                        "NAMA_FILE":Parser_NAMA_FILE,
                        "CHAT_MESSAGE":Parser_CHAT_MESSAGE,
                        "REMOTE_PATH":Parser_REMOTE_PATH,
                        "LOCAL_PATH":Parser_LOCAL_PATH,
                        "SUB_ID":Parser_SUB_ID
                    };
        const res_message = JSON.stringify(myObj);
        */
        
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
                
        console.log(res_message);
        
     
        pub_command(topic_bc,res_message,excel_kdtk,excel_station);
}


client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    SPDMastLogger.info("Can't connect MQTT Broker : " + error);
    process.exit(1)
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function subs_progress_bc(){
    var topic_command = "PROGRESS_BC/"+regional+"/";
    client.subscribe(topic_command,{qos:0});
    console.log("subs : "+topic_command);
}


client.on('message',async function(topic, compressed){
   try{        
        const decompressed = await ungzip(compressed);
        const parseJson = JSON.parse(decompressed);
        const IN_SOURCE = parseJson.SOURCE;
        const IN_TASK = parseJson.TASK;
        const IN_HASIL = parseJson.HASIL;
        const IN_CABANG = parseJson.CABANG;
        //const IN_CMD = parseJson.COMMAND.toString().includes("version.txt");

        var tanggal_message_terima = service_controller.get_tanggal_jam("1");
        console.log(tanggal_message_terima+" - "+IN_CABANG+" : Progress -> "+IN_HASIL+"%");
        if(parseFloat(IN_HASIL) == "100"){
            //console.log("PROSES SELESAI - "+IN_CABANG);
        }

    }catch(exc){
        //console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
    }
});

async function pub_command(topic_bc,res_message,toko,station){
    const compressed = await gzip(res_message);  
    client.publish(topic_bc,compressed);
    SPDMastLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    console.log("===================================================================");
}