var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('./connection/mysql_connection');
var clickhouseLib = require('./connection/clickhouse_connect');
var service_controller = require('./controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');


const jsonString = fs.readFileSync("appconfig.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = "Monitoring_Qprod_1";
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
const topic_subs = student.TOPIC_SUBS;
const initial_from = student.INITIAL_FROM;

const kode_cabang_initial = student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;


const is_dinamis_query = student.IS_DINAMIS_QUERY; 
const res_call_initial = student.CALL_INITIAL; 
const COMMAND_DIR_D = student.COMMAND_DIR_D;
const COMMAND_DIR_ECAD = student.COMMAND_DIR_ECAD;

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT"); 
    client.subscribe(topic_subs);
    console.log("SUBS : "+topic_subs);
});

client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    process.exit(1)
});


const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

client.on('message',async function(topic, compressed){
    try{
        
           
            if(topic.includes("BYLINE")){
	            console.log("SKIP MESSAGE : "+topic);
	        }else if(topic === "BC/REPORT"){
	            console.log("SKIP MESSAGE : "+topic);
	        }else{
	        	const decompressed = await ungzip(compressed);
	            const parseJson = JSON.parse(decompressed);
	            const IN_SOURCE = parseJson.SOURCE;
	            const IN_TASK = parseJson.TASK;
	          
	            if(IN_SOURCE === 'IDMCommander' && IN_TASK === 'BC_COMMAND'){
	              console.log("SKIP INSERT : "+IN_SOURCE+" - "+IN_TASK);
	            }else{
	            	const IN_HASIL = parseJson.HASIL;
	            	console.log(IN_HASIL);
	                service_controller.ins_transreport("mysql",topic,decompressed,"qprod","REPLACE");
	            }
	            
	            var tanggal_message_terima = service_controller.get_tanggal_jam("1");
	            console.log(tanggal_message_terima+" : "+topic)
	        }
            console.log('---------------------------------------------------------------------');
       
    }catch(exc){
        console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
    }
});