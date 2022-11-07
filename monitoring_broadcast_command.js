var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('./connection/mysql_connection');
var service_controller = require('./controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');


const jsonString = fs.readFileSync("appconfig.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = student.CLIENT_ID+"_monitoring_broadcast";
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
//const topic_bc = student.TOPIC_BC;
const initial_from = student.INITIAL_FROM;

const kode_cabang_initial = student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;



var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT"); 
    subs_progress_bc();
});

client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    process.exit(1)
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function subs_progress_bc(){
    var topic_command = "PROGRESS_BC/#";
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
        var IN_TO     = parseJson.TO;
        var nik       = IN_TO.split('_')[1];
        var qry = "SELECT NAMA FROM idm_org_structure WHERE NIK = '"+nik+"';";
        console.log(qry);
        //console.log(decompressed);
        mysqlLib.executeQuery(qry).then((d) => {
            var tanggal_message_terima = service_controller.get_tanggal_jam("1");
            var nama = "";
            try{
                nama = d[0]['NAMA'];
            }catch(exc){

            }
            console.log("Tanggal Pesan\t\t: "+tanggal_message_terima);
            console.log("User NIK\t\t: "+nik+" | "+nama);
            console.log("Location\t\t: "+IN_TO.split('_')[0]);
            console.log("Cab. Tujuan\t\t: "+IN_CABANG);
            console.log("Progress\t\t: "+IN_HASIL+" %");
            if(parseFloat(IN_HASIL) == "100"){
                console.log("PROSES SELESAI BROADCAST");
                console.log("=============================================================");
            }else{
                console.log("-------------------------------------------------------------");
            }

        }).catch(e => {
            console.log(e);
        });
        
        
    }catch(exc){
        console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
    }
});
