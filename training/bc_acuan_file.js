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
const client_id = student.CLIENT_ID+"_BC_ACUAN";
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

var client  = mqtt.connect("mqtt://"+host_mqtt,{clientId:client_id,clean:clean_session,port:port_mqtt});
client.on("connect", function(){    
    console.log("connected MQTT"); 
    //subs_progress_bc();
    /*
    cron.schedule('0 '+SCHEDULE_CRON_MENIT+' '+SCHEDULE_CRON_JAM+' * * *', async function() {
        console.log('---------------------------------------------------------------------');
        console.log('Cron Job Berjalan Jam '+SCHEDULE_CRON_JAM+':'+SCHEDULE_CRON_MENIT+'');
        console.log('---------------------------------------------------------------------');
        
        const arr_cabang = kode_cabang_initial.split(',');
        for(var i = 0;i<arr_cabang.length;i++){
            console.log("CABANG\t:\t"+arr_cabang[i]);
            //read_config_broadcast(arr_cabang[i]);
            await sleep(jeda_pengecekan_versi);
        }
    });
    */
    bc_acuan_file();

});

function bc_acuan_file(){
    var Excel = require('exceljs');

    var wb = new Excel.Workbook();
    var path = require('path');
    var filePath = path.resolve(__dirname,'REG4_POSNET.xlsx');

    wb.xlsx.readFile(filePath).then(function(){

        var sh = wb.getWorksheet("Sheet1");
        console.log(sh.rowCount);
        for (i = 2; i <=sh.rowCount; i++) {
            const excel_kdcab = sh.getRow(i).getCell(1).value;
            const excel_kdtk = sh.getRow(i).getCell(2).value;
            const excel_station = sh.getRow(i).getCell(3).value;
            const excel_ip = sh.getRow(i).getCell(4).value;
            const excel_install_source = sh.getRow(i).getCell(5).value;
            const excel_local_package = sh.getRow(i).getCell(6).value;
            const excel_addtime = sh.getRow(i).getCell(7).value;
            
            const topic_bc_acuan = "COMMAND/"+excel_ip+"/";
            //----------------------------------------------------------------------------------------//
            const command_kirim_del_installer = "del "+excel_local_package.split("/").join("\\")+" /S";
            const command_kirim_del_pos_net_msi = "del "+excel_install_source.split("/").join("\\")+" /S";

            const command_kirim_dir_installer = "dir "+excel_local_package.split("/").join("\\")+"";
            const command_kirim_dir_pos_net_msi = "dir "+excel_install_source.split("/").join("\\")+"";

            //const command_kirim_dir = "dir "+excel_local_package.split("/").join("\\")+"";
            //----------------------------------------------------------------------------------------//
            const command_kirim = command_kirim_dir_installer;
            pub_bc_acuan(topic_bc_acuan,command_kirim,excel_kdtk,excel_station,excel_ip);
        }
        
    });
}

function pub_bc_acuan(topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){
        var kode_cabang =  kode_cabang_initial;

        const Parser_TASK = "BC_COMMAND";
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "IDMCommander";
        const Parser_OTP= "NTIwNTAwNzMzfDIwMjEtMDctMDYgMTc6MTQ6MDJ8MTQwMDAwMDB8UkVHNA==";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= initial_from;
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
                
        console.log(res_message);
        pub_command(topic_bc,res_message,excel_kdtk,excel_station);
}


client.on("error",function(error){
    console.log("Can't connect MQTT Broker : " + error);
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
            console.log("PROSES SELESAI - "+IN_CABANG);
        }

    }catch(exc){
        console.log("ERROR TERIMA MESAGE : "+exc+" topic : "+topic+" pesan : "+compressed)  
    }
});

async function pub_command(topic_bc,res_message,toko,station){
    //console.log("Message : "+res_message);
    const compressed = await gzip(res_message);  
    client.publish(topic_bc,compressed);
    console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    console.log("===================================================================");
}