var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('../connection/mysql_connection');
var service_controller = require('../controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');
const {userLogger,RKEYVatLogger,LockRegistryLogger,ProdmastLogger,programInstalledLogger,memoryInfoLogger,BootTimeLogger} = require('../controller/logger');

const jsonString = fs.readFileSync("../config/boot_time.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = student.CLIENT_ID+"_"+service_controller.get_id();
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());
const initial_from = student.INITIAL_FROM;
const kode_cabang_initial = student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;

const SCHEDULE_CRON_JAM = student.SCHEDULE_CRON_JAM;
const SCHEDULE_CRON_MENIT = student.SCHEDULE_CRON_MENIT;
const station = student.STATION.split(',');

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT");
    memoryInfoLogger.info("connected MQTT");
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}


// ------------------------ SCHEDULE CRON JOB --------------------//
console.log("Berjalan pada jam : "+SCHEDULE_CRON_JAM+" Menit : "+SCHEDULE_CRON_MENIT);
cron.schedule('0 '+SCHEDULE_CRON_MENIT+' '+SCHEDULE_CRON_JAM+' * * *', async function() {
        const arr_cabang = kode_cabang_initial.split(',');
        console.log("Jumlah Cabang : "+arr_cabang.length);
        for(var i = 0;i<arr_cabang.length;i++){
            BootTimeLogger.info("Proses Cabang-"+arr_cabang[i]);
            const ip = arr_cabang[i];
            const topic_bc = "BOOT_TIME/"+ip+'/'
            const kdtk = 'ALL TOKO '+topic_bc;
            BootTimeLogger.info("Proses Broadcast : "+topic_bc);
            const body_command = "Get-WinEvent -FilterHashtable @{LogName='Microsoft-Windows-Diagnostics-Performance/Operational'; Id=100} -MaxEvents 1 |ForEach-Object {$eventXml = ([xml]$_.ToXml()).Event; [PSCustomObject]@{'BootTime' = [math]::Round([int64]($eventXml.EventData.Data | Where-Object {$_.Name -eq 'BootTime'}).InnerXml / 1000 / 60,2); 'BootFinished' = [datetime]($eventXml.EventData.Data | Where-Object {$_.Name -eq 'BootEndTime'}).InnerXml }} | Format-Table -HideTableHeaders";
            const command_kirim = body_command;
            console.log("COMMAND KIRIM : "+command_kirim);
            pub_bc_acuan("BC_POWERSHELL_COMMAND",topic_bc,command_kirim,kdtk,ip);
            await sleep(5000);
        }
});

function pub_bc_acuan(task,topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){
        var kode_cabang =  kode_cabang_initial;
        const Parser_TASK = task;
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "IDMCommander";
        const Parser_OTP= "NTIwNTAwNzMzfDIwMjEtMDctMDYgMTc6MTQ6MDJ8MTQwMDAwMDB8UkVHNA=";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= "Service_Boot_Time";
        const Parser_TO= excel_ip;
        const Parser_SN_HDD= "Z9ANTB8R";
        const Parser_IP_ADDRESS= "192.168.131.104";
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

async function pub_command(topic_bc,res_message,toko,station){
    //console.log("Message : "+res_message);    
    const compressed = await gzip(res_message);  
    client.publish(topic_bc,compressed);
    BootTimeLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    console.log("===================================================================");
}

client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    BootTimeLogger.info("Can't connect MQTT Broker : " + error);
    process.exit(1)
});
