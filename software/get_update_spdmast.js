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
const station = student.STATION.split(',');
const list_program = student.LIST_PROGRAM.split(',');

//202212020100
const history_tanggal_table = service_controller.get_subid().toString().substring(2,8);


var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT");
    userLogger.info("connected MQTT");
    sleep(5000);    
});

// ------------------------ SCHEDULE CRON JOB --------------------//
console.log("Berjalan pada jam : "+SCHEDULE_CRON_JAM+" Menit : "+SCHEDULE_CRON_MENIT);
    cron.schedule('0 '+SCHEDULE_CRON_MENIT+' '+SCHEDULE_CRON_JAM+' * * *', async function() {
        const arr_cabang = kode_cabang_initial.split(',');
        for(var i = 0;i<arr_cabang.length;i++){
            userLogger.info("Proses Cabang-"+arr_cabang[i]);  
            const ip = arr_cabang[i]
            //-- get station is_induk != '1' --//
            var res_station = "01";
            await sleep(5000);
            console.log("res-station : "+res_station)
            //-- loop station --//
                    var hasil_station = res_station;
                    const topic_bc = "10.77.8.66"; //""+ip+'/'+hasil_station+'/'
                    console.log("TOPIC BC : "+topic_bc);
                    //console.log("-------------------------------------------");
                    const kdtk = 'ALL TOKO '+topic_bc;
                    userLogger.info("Proses Broadcast - "+hasil_station);
                    try{   
                             
                        const command_kirim = "{\"TIPE_BC\":\"SOME STORES\",\"PORT\":\"3306\",\"COMMAND_SQL\":\"CREATE TABLE IF NOT EXISTS bck_reg_spdmast_"+history_tanggal_table+" select * from spdmast_copy;\\r\\nDROP TABLE IF EXISTS act_reg_spdmast;\\r\\nCREATE TABLE act_reg_spdmast SELECT * FROM spdmast_copy GROUP BY prdcd;\\r\\nUPDATE act_reg_spdmast SET supco = 'G236';\\r\\nDELETE FROM spdmast_copy;\\r\\nINSERT INTO spdmast_copy SELECT * FROM act_reg_spdmast;\",\"PASS\":\"goCkeKArFYJYqmN9DHS\\/Uyn1HGgFpqrVI=REgE+tC2ZG,cL\\/EohOGyT3uPR\\/HmG9zSpHt6\\/V8zPQKs=VunZtrQfh1\",\"USER\":\"kasir\",\"LIST_IP\":\"10.77.8.66,10.77.9.162,10.77.7.2,10.77.10.2,10.77.8.34,10.77.9.130,10.77.8.194,10.77.7.34,10.77.9.66,10.77.9.98,10.77.10.98,192.168.194.61,10.77.7.98,10.77.8.130,10.77.9.226,10.77.8.162,192.168.194.62,10.77.7.130,10.77.7.162,10.77.8.226,10.77.9.194,10.77.8.98,10.77.10.66,10.77.9.34,10.77.10.34,10.77.9.2,10.77.8.2,10.77.7.194,10.77.7.226,10.77.7.66\",\"DB\":\"pos\"}";
                        
                        console.log("COMMAND KIRIM : "+command_kirim);
                        //console.log("-------------------------------------------");
                        pub_bc_acuan("BC_SQL",topic_bc,command_kirim,kdtk,ip);
                        console.log("=================================================");

                            
                    }catch(exc){
                        console.log("ERROR : "+exc);
                    }
                    await sleep(jeda_pengecekan_versi);
            
           
        }
    });
 

function pub_bc_acuan(task,topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){

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
        const Parser_OTP= "NTIwNTAwNzMzfDIwMjEtMDctMDYgMTc6MTQ6MDJ8MTQwMDAwMDB8UkVHNA=";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= "REG4_2021005005_172.28.64.1_7E9F8055_202212060450";
        const Parser_TO= excel_ip;
        const Parser_SN_HDD= "Z9ANTB8R";
        const Parser_IP_ADDRESS= "127.0.0.1";
        const Parser_STATION= "-";
        const Parser_CABANG = kode_cabang_initial;
        const Parser_FILE = "-";
        const Parser_NAMA_FILE= "-";
        const Parser_CHAT_MESSAGE= "-";
        const Parser_REMOTE_PATH= "-";
        const Parser_LOCAL_PATH= "-";
        const Parser_COMMAND= command_kirim;
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
        pub_command(topic_bc,res_message,excel_kdtk,excel_station);
}


client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    userLogger.info("Can't connect MQTT Broker : " + error);
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
    //console.log("Message : "+res_message);
    const compressed = await gzip(res_message);  
    //client.publish(topic_bc,compressed);
    //console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    userLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    console.log("===================================================================");
}