var mqtt    = require('mqtt');
const {gzip, ungzip} = require('node-gzip');
var Promise = require('promise');
var mysqlLib = require('./connection/mysql_connection');
var service_controller = require('./controller/service_controller');
const fs = require('fs');
const cron = require('node-cron');
const {userLogger,RKEYVatLogger,LockRegistryLogger,ProdmastLogger} = require('./controller/logger');


const jsonString = fs.readFileSync("./config/monitoring_program_backoff.json");
let student = JSON.parse(jsonString);
const host_mqtt = student.HOST_MQTT;
const client_id = student.CLIENT_ID+"_"+service_controller.get_id();
const clean_session = student.CLEAN_SESSION;
const port_mqtt = student.PORT_MQTT;
var jeda_pengecekan_versi = parseFloat(student.JEDA_PENGECEKAN_VERSI.toString());

const topic_validasi_bc_command = student.TOPIC_VALIDASI_BC_COMMAND;
//const topic_bc = student.TOPIC_BC;
const initial_from = student.INITIAL_FROM;

const kode_cabang_initial = student.KODE_CABANG_INITIAL;
const regional = student.REGIONAL;
const res_call_initial = student.CALL_INITIAL; 
const SCHEDULE_CRON_JAM = student.SCHEDULE_CRON_JAM;
const SCHEDULE_CRON_MENIT = student.SCHEDULE_CRON_MENIT;
const station = student.STATION.split(',');


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
            var ip = arr_cabang[i];
           
           
            //-- loop station --//
                    const topic_bc = ""+ip+'/';
                    const kdtk = 'ALL TOKO '+topic_bc;
                    userLogger.info("Proses Broadcast - "+ip);
                    var arr_list_program = [];
                    var res_hasil_nama_program_command = '';    
                    const sql_query = "SELECT NAMA_PROGRAM,VERSI,SIZE FROM m_table_setting_program ORDER BY NAMA_PROGRAM ASC;";
                    //console.log("sql_query_list_program : "+sql_query)
                    mysqlLib.executeQuery(sql_query).then((d) => {
                        for(var a = 0;a<d.length;a++){
                            var arr_data_program = {"NAMA_PROGRAM":d[a].NAMA_PROGRAM,"VERSI_MASTER":d[a].VERSI,"SIZE":d[a].SIZE};
                            arr_list_program.push(arr_data_program);

                            if(a == (d.length-1) ){
                                res_hasil_nama_program_command += "'"+d[a].NAMA_PROGRAM+"'";
                            }else{
                                res_hasil_nama_program_command += "'"+d[a].NAMA_PROGRAM+"',";    
                            }
                        }

                        try{   
                            //-- publish to is induk --//
                            var arr_list_ip = [];
                            const sql_query_is_induk = "SELECT IP FROM tokomain WHERE IS_INDUK = '1' AND KDCAB = '"+ip+"' AND STATION NOT IN('STB','00','I1') ORDER BY KDCAB,TOKO ASC;"
                            mysqlLib.executeQuery(sql_query_is_induk).then((d) => {
                                for(var a = 0;a<d.length;a++){
                                     arr_list_ip.push(d[a].IP);
                                }
                                var path_backoff = 'd:\\Backoff\\*';
                                const command_powershell = "\"get-childitem -path '"+path_backoff+"'  -include *.dll,*.exe | Where-Object {$_.Name -match (("+res_hasil_nama_program_command+") -Join '|') } | foreach-object { '{0};{1};{2};{3}' -f $_.Name, [System.Diagnostics.FileVersionInfo]::GetVersionInfo($_).FileVersion, $_.lastwritetime,($_.length) }\"";    
                                const command_kirim = {"LIST_PROGRAM":arr_list_program,"LIST_IP":arr_list_ip,"COMMAND_POWERSHELL":command_powershell};
                                var res_excel_ip = ip;
                                var res_station = '';
                                pub_bc_acuan("MONITORING_PROGRAM_BACKOFF",topic_bc,JSON.stringify(command_kirim),kdtk,res_station,res_excel_ip);
                                console.log("=================================================");
                            });
                        }catch(exc){
                            console.log("ERROR : "+exc);
                        }

                        //sleep(jeda_pengecekan_versi);

                        console.log("#####################################################");
                        // try{

                        //     //-- publish to not is induk --//
                        //     var arr_list_ip = [];
                        //     const sql_query_is_not_induk = "SELECT IP FROM tokomain WHERE IS_INDUK = '0' AND KDCAB = '"+ip+"' AND STATION NOT IN('STB','00','I1') ORDER BY KDCAB,TOKO ASC;"
                        //     mysqlLib.executeQuery(sql_query_is_not_induk).then((d) => {
                        //         for(var a = 0;a<d.length;a++){
                        //              arr_list_ip.push(d[a].IP);
                        //         }
                        //         var path_backoff = 'e:\\cad\\Backoff\\*';
                        //         const command_powershell = "\"get-childitem -path '"+path_backoff+"'  -include *.dll,*.exe | Where-Object {$_.Name -match (("+res_hasil_nama_program_command+") -Join '|') } | foreach-object { '{0};{1};{2};{3}' -f $_.Name, [System.Diagnostics.FileVersionInfo]::GetVersionInfo($_).FileVersion, $_.lastwritetime,($_.length) }\"";    
                        //         const command_kirim = {"LIST_PROGRAM":arr_list_program,"LIST_IP":arr_list_ip,"COMMAND_POWERSHELL":command_powershell};
                        //         pub_bc_acuan("MONITORING_PROGRAM_BACKOFF",topic_bc,command_kirim,kdtk,ip);
                        //         console.log("=================================================");
                        //     });
                        // }catch(exc){
                        //     console.log("ERROR : "+exc);   
                        // }
                    });  

            await sleep(jeda_pengecekan_versi);       
        }
    });
 

function pub_bc_acuan(task,topic_bc,command_kirim,excel_kdtk,excel_station,excel_ip){
        
        const Parser_TASK = task;
        const Parser_ID= service_controller.get_id().toString();
        const Parser_SOURCE= "ServiceProgramBackoff";
        const Parser_OTP= "NTIwNTAwNzMzfDIwMjEtMDctMDYgMTc6MTQ6MDJ8MTQwMDAwMDB8UkVHNA=";
        const Parser_TANGGAL_JAM= service_controller.get_tanggal_jam("1").toString();
        const Parser_VERSI= "1.0.1";
        const Parser_HASIL= "-";
        const Parser_FROM= "ServiceProgramBackoff";
        const Parser_TO= excel_ip;
        const Parser_SN_HDD= "Z9ANTB8R";
        const Parser_IP_ADDRESS= "192.168.131.104";
        const Parser_STATION= "-";
        const Parser_CABANG = excel_ip;
        const Parser_FILE = "-";
        const Parser_NAMA_FILE= "-";
        const Parser_CHAT_MESSAGE= "-";
        const Parser_REMOTE_PATH= "-";
        const Parser_LOCAL_PATH= "-";
        const Parser_COMMAND= command_kirim;
        const Parser_SUB_ID= service_controller.get_subid().toString();
        //console.log("Parser_CABANG : "+Parser_CABANG);
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
    client.publish(topic_bc,compressed);
    //console.log(service_controller.get_tanggal_jam("1")+" - Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    userLogger.info("Publish : "+topic_bc+" Toko : "+toko+" Station : "+station);
    console.log("===================================================================");
}