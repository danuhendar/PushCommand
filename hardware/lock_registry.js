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
const client_id = student.CLIENT_ID+"_LOCK_REGISTRY";
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
//const topic_bc = student.TOPIC_BC;
const initial_from = student.INITIAL_FROM;

const kode_cabang_initial = "G001,G004,G005,G009,G020,G025,G026,G027,G028,G029,G030,G033,G034,G049,G050,G080,G089,G092,G097,G099,G105,G107,G113,G116,G117,G137,G146,G148,G149,G156,G157,G158,G165,G174,G177,G224,G301,G305,G801";//student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;

const is_dinamis_query = student.IS_DINAMIS_QUERY; 
const res_call_initial = student.CALL_INITIAL; 
const COMMAND_DIR_D = student.COMMAND_DIR_D;
const COMMAND_DIR_ECAD = student.COMMAND_DIR_ECAD;
const SCHEDULE_CRON_JAM = student.SCHEDULE_CRON_JAM;
const SCHEDULE_CRON_MENIT = student.SCHEDULE_CRON_MENIT;

const SCHEDULE_CRON_JAM_SQL = student.SCHEDULE_CRON_JAM_SQL;
const SCHEDULE_CRON_MENIT_SQL = student.SCHEDULE_CRON_MENIT_SQL;

const tipe_bc_area = student.TIPE_BC_AREA;
const tipe_bc = student.TIPE_BC;
const list_program = student.LIST_PROGRAM.split(',');

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", async function(){
    LockRegistryLogger.info("connected MQTT");
    
    cron.schedule('0 0 */1 * * *', async function() {
        console.log('---------------------------------------------------------------------');
        console.log('Cron Job Berjalan 1 Jam Sekali');
        console.log('---------------------------------------------------------------------');
        var date = new Date();
        var jam = parseFloat(date.getHours());
        if(jam < 8){
            LockRegistryLogger.info("Skip Lock Registry Jam : "+jam);        
        }else{
            const arr_cabang = kode_cabang_initial.split(',');
            for(var i = 0;i<arr_cabang.length;i++){
            LockRegistryLogger.info("PROSES CABANG\t:\t"+arr_cabang[i]);
                    try {
                        const data = fs.readFileSync(__dirname + '/data.txt', 'utf8')
                        const Parser_STATION = '04'
                        const topic_bc = "BC_COMMAND/"+arr_cabang[i]+"/"; //"VALIDASI_BC_COMMAND/"+arr_cabang[i]+"/"//arr_cabang[i]+'/'+station+'/'
                        const Parser_TASK = "BC_COMMAND";
                        const Parser_VERSI = "1.0.1";
                        const Parser_HASIL = "-";
                        const Parser_OTP = "NTIwNTAwNzMzfDIwMjEtMDctMDYgMTc6MTQ6MDJ8MTQwMDAwMDB8UkVHNA==";
                        const Parser_FROM = initial_from;
                        const Parser_CHAT_MESSAGE = "-";
                        const Parser_CABANG = arr_cabang[i];
                        const Parser_SUB_ID = service_controller.get_subid();
                        const Parser_REMOTE_PATH = "-";
                        const Parser_LOCAL_PATH = "-";
                        const Parser_SOURCE = "IDMCommander";
                        const Parser_NAMA_FILE = "-";
                        const Parser_ID = service_controller.get_id();

                        const periode_log = service_controller.get_tanggal_jam("0");
 
                        const Parser_COMMAND = data;
                        const Parser_IP_ADDRESS = "192.68.131.104";
                       
                        const Parser_TO = arr_cabang[i]+"/"+Parser_STATION+"/";
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
                        RKEYVatLogger.info("Publish Lock : "+topic_bc+" Cabang : "+Parser_CABANG+" Station : "+Parser_STATION);
                        await sleep(6000);

                        //-- bc hapus log --//
                        const Parser_COMMAND_DEL = "del C:\\IDMCommandListeners\\log_"+periode_log+".txt /S\n"+
                                                    "del C:\\Windows\\system32\\log_"+periode_log+".txt /S\n"+
                                                    "del log_"+periode_log+".txt /S\n"+
                                                    "exit";
                        const res_message_del = service_controller.CreateMessage(Parser_TASK,
                                                            Parser_ID,
                                                            Parser_SOURCE,
                                                            Parser_OTP,
                                                            Parser_TANGGAL_JAM,
                                                            Parser_VERSI,
                                                            Parser_COMMAND_DEL,
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

                        const compressed_del = await gzip(res_message_del);  
                        client.publish(topic_bc,compressed_del);
                        RKEYVatLogger.info("Publish Del : "+topic_bc+" Cabang : "+Parser_CABANG+" Station : "+Parser_STATION);
                        

                        console.log("=================================================");
                        await sleep(10000);
                    } catch (err) {
                      console.error(err)
                    }
            }
        }

       
    });    
});

client.on('message',async function(topic, compressed){
   try{        
        const decompressed = await ungzip(compressed);
        const parseJson = JSON.parse(decompressed);
        const IN_SOURCE = parseJson.SOURCE;
        const IN_TASK = parseJson.TASK;
        const IN_HASIL = parseJson.HASIL;
        const IN_CABANG = parseJson.CABANG;
        
        var tanggal_message_terima = service_controller.get_tanggal_jam("1");
        //console.log(tanggal_message_terima+" - "+IN_CABANG+" : Progress -> "+IN_HASIL+"%");
        LockRegistryLogger.info(tanggal_message_terima+" - "+IN_CABANG+" : Progress -> "+IN_HASIL+"%");
        if(parseFloat(IN_HASIL) == "100"){
            //console.log("PROSES SELESAI - "+IN_CABANG);
        }

    }catch(exc){
        //console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
        LockRegistryLogger.error("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed);
    }
});
 

function subs_progress_bc(){
	var location = initial_from.split('_')[0];
    var topic_command = "PROGRESS_BC/"+location+"/";
    client.subscribe(topic_command,{qos:0});
    //console.log("subs : "+topic_command);
    LockRegistryLogger.info("subs : "+topic_command);
}


function pub_bc_acuan(topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){
        var kode_cabang =  kode_cabang_initial;

        const Parser_TASK = "VALIDASI_BC_SQL";
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
        const Parser_CABANG = kode_cabang_initial;
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
        LockRegistryLogger.warning(res_message);
        pub_command(topic_bc,res_message,excel_kdtk,excel_station);
}


client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    LockRegistryLogger.error("Can't connect MQTT Broker : " + error);
    process.exit(1)
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

async function pub_command(topic_bc,res_message,toko,station){
    //console.log("Message : "+res_message);
    const compressed = await gzip(res_message);  
    //client.publish(topic_bc,compressed);
    //console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    //console.log("===================================================================");
    LockRegistryLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
}