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
const client_id = student.CLIENT_ID+"";
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
const topic_bc = student.TOPIC_BC;
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

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT"); 
    subs_progress_bc();
    cron.schedule('0 '+SCHEDULE_CRON_MENIT+' '+SCHEDULE_CRON_JAM+' * * *', async function() {
        console.log('---------------------------------------------------------------------');
        console.log('Cron Job Berjalan Setiap Jam '+SCHEDULE_CRON_JAM+':'+SCHEDULE_CRON_MENIT+'');
        console.log('---------------------------------------------------------------------');
        
        const arr_cabang = kode_cabang_initial.split(',');
        for(var i = 0;i<arr_cabang.length;i++){
            console.log("CABANG\t:\t"+arr_cabang[i]);
            read_config_broadcast(arr_cabang[i]);
            await sleep(jeda_pengecekan_versi);
        }
    });
});

client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
    process.exit(1)
});

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function subs_progress_bc(){
    var topic_command = "PROGRESS_BC/"+initial_from+"/";
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
            console.log("PROSES SELESAI - "+IN_CABANG);
        }

    }catch(exc){
        console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
    }
});
 

 

async function pub_command(res_message){
    //console.log("Message : "+res_message);
    var topic_cek_versi_service = topic_validasi_bc_command;
    const compressed = await gzip(res_message);  
    client.publish(topic_cek_versi_service,compressed);
    console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_cek_versi_service);
    console.log("===================================================================");
}


function read_config_broadcast(res_kode_cabang){
     fs.readFile('appconfig.json', (err, data) => {
        if (err) throw err;
        let student = JSON.parse(data);
        const topic_bc = student.TOPIC_BC;
        const command_update = student.COMMAND_UPDATE;
        const command_versi = student.COMMAND_VERSI;
        const command_sql = student.COMMAND_SQL;
        const is_type = student.IS_TYPE;
         
        

        const counter_start = student.COUNTER_START;
        const limit_klien_update = student.LIMIT_KLIEN_UPDATE; 
        const station_initial = student.STATION;
     

        var query_list_target = student.QUERY_LIST_TARGET+" WHERE KDCAB = '"+res_kode_cabang+"' AND (STATION != 'STB' AND STATION = '"+station_initial+"') AND TOKO NOT IN('DANU','B025','B004','B034','B305','B148','B301','B097','B030')  ORDER BY TOKO ASC;";
        //console.log(query_list_target);

        if(res_call_initial == "1"){
            call_initial(res_kode_cabang);  
        }else{
            if(is_type === "1"){
                var tanggal_message_terima = service_controller.get_tanggal_jam("2");
                var restopic_bc = topic_bc.split("CABANG").join(res_kode_cabang);
                Broadcast_pengajuan(restopic_bc,command_update,res_kode_cabang+"-PENGAMBILAN INFORMASI PROGRAM-"+tanggal_message_terima,tipe_bc,query_list_target);    
            }else if(is_type === "2"){
                Broadcast_pengajuan(topic_bc,command_versi,"CEK VERSI",tipe_bc,query_list_target);
            }else{
                Broadcast_pengajuan(topic_bc,command_sql,"BROADCAST SQL",tipe_bc,query_list_target);
            }
            console.log("Menunggu response dari server !!!");

        }
    });
}


async function Broadcast_pengajuan(topic_bc,command_kirim,info,tipe_bc,query_list_target){

        var kode_cabang = info.split('-')[0];

        const Parser_TASK = "VALIDASI_BC_COMMAND";
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "IDMCommander";
        const Parser_OTP= "";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= initial_from;
        const Parser_TO= "IDMReporter";
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
        //var qry = query_list_target;
        var kode_cabang = info.split('-')[0];
        var qry = "SELECT COUNT(IP) AS JUMLAH FROM tokomain where KDCAB = '"+kode_cabang+"' AND STATION != 'STB';";
        //console.log(qry);
        mysqlLib.executeQuery(qry).then((d) => {
            var code = 200;
            /*
            var ip_concat = "";
            for(var i = 0;i<d.length;i++){
                if(i == (d.length-1) ){
                    ip_concat = ip_concat+""+d[i].IP+"/";        
                }else{
                    ip_concat = ip_concat+""+d[i].IP+"/,";        
                }
                
            }
            */
            var jumlah_client = d[0].JUMLAH;
            if(parseFloat(jumlah_client) > 0){
                //topic_bc = ip_concat;
                //var arr_topic_bc = ip_concat.split(','); 
                //topic_bc = topic_bc.replace('CABANG',topic_bc)
                
                
                
                const Parser_COMMAND = {"KODE_CABANG_TUJUAN":regional,"JUMLAH_CLIENT":""+jumlah_client+"","COMMAND_KIRIM":command_kirim,"NIK":"2013058359","NAMA":"DANU HENDAR","JABATAN":"SUPPORT_TK","TIPE":tipe_bc_area,"KETERANGAN":info,"KONDISI":"-","TIPE_BC":tipe_bc,"TOPIC_BC":topic_bc};
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

                pub_command(res_message);
            }else{
                console.log("Tidak ada data yang diproses. Terimakasih !!!");
                process.exit(1);
            }   
        }).catch(e => {
            console.log(e);
        });
}

