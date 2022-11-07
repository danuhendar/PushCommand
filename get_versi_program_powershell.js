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
            var res_station = "";
            const sql_get_station = "SELECT GROUP_CONCAT(DISTINCT(STATION)) AS STATION FROM `tokomain` WHERE KDCAB = '"+ip+"' AND IS_INDUK != '1' AND STATION NOT IN('STB','I1') AND NAMA NOT LIKE '%SIMULASI%' AND NAMA NOT LIKE '%BAZAR%' AND NAMA NOT LIKE '%EVENT%' ";
            //console.log(sql_get_station);
            //console.log("-------------------------------------------");
            mysqlLib.executeQuery(sql_get_station).then((d) => {
                res_station = d[0].STATION;
            });

            await sleep(5000);
            console.log("res-station : "+res_station)
            //-- loop station --//
            var sp_station = res_station.split(',');
            for(var k = 0;k<sp_station.length;k++){
                    var hasil_station = sp_station[k];
                    const topic_bc = ""+ip+'/'+hasil_station+'/'
                    console.log("TOPIC BC : "+topic_bc);
                    //console.log("-------------------------------------------");
                    const kdtk = 'ALL TOKO '+topic_bc;
                    userLogger.info("Proses Broadcast - "+hasil_station);
                    try{   
                            const sql_query = "SELECT NAMA_PROGRAM FROM m_monitoring_program_toko WHERE KDCAB = '"+ip+"';";
                            //console.log(sql_query)
                            //console.log("-------------------------------------------");
                            mysqlLib.executeQuery(sql_query).then((d) => {
                                
                                var res_hasil = "";
                                for(var a = 0;a<d.length;a++){
                                    if(a == (d.length-1) ){
                                        res_hasil += "'"+d[a].NAMA_PROGRAM+"'";
                                    }else{
                                        res_hasil += "'"+d[a].NAMA_PROGRAM+"',";    
                                    }
                                    
                                }
                                var path_backoff = "";
                                if(res_station == '01'){
                                    path_backoff = 'd:\\Backoff\\*';
                                }else{
                                    path_backoff = 'e:\\cad\\Backoff\\*';
                                }
                                //C:\Windows\System32\wbem\wmic datafile where name="D:\\Backoff\\POS.NET.exe" get version
                                //,'BPJSVB.exe','BSTeras.exe','CekSpek.exe','ChecklistToko.exe','DCPSOVB.exe','edcserver.exe','FingerITControlling.exe','FtpToko.exe','GameOnline.exe','HookXP.exe','I-Paket.exe','I-Pulsa.exe','IDM.Advanced.dll','IDM.ClassCon.dll','IDM.EDC.dll','IDM.Fungsi.dll','IDM.InfoToko.dll','Idm.MiniPOS.dll','IDM.MySQL.dll','IDM.Payment.dll','IDM.Paymentpoint.dll','IDM.Reactor.dll','IDM.Report.dll','IDM.Sector.dll','IDMPulsa.dll','Ikiosk.exe','Ikiosk_CMS.exe','IStoreMonitoring.exe','Kardus.NET.exe','Laundry.exe','Listrik.exe','LoadMaster.exe','MappingDrive.exe','MiniUpdater.exe','MySqlAdmin.exe','PAAI.exe','PaymentPoint.exe','PDAM.exe','POS.NET.exe','POS.Net.Monitoring.exe','POS2.exe','PosBPB.dll','POSIdm.dll','POSMAIN.EXE','PosRealTime.exe','PosUtil.dll','PRGLIST.exe','PriceTag.exe','PUSH.EXE','RakGudang.exe','RegistDLL.exe','REQUESTFINGER.EXE','ReturSales.exe','reversal.exe','RKB.exe','RRAK.NET.exe','ScanFinger.dll','SO.Net.exe','SoContainer.exe','SOTKIDM.exe','SQLFromFile.exe','TakeOver.exe','TCashOut.dll','TCashOut.exe','Telkomvision.exe','Ticketing.exe','TxTravel.exe','UpdVersiOL.exe','WDCP.exe','WhitelistPLU.exe'
                                const command_kirim = "\"get-childitem -path '"+path_backoff+"'  -include *.dll,*.exe | Where-Object {$_.Name -match (("+res_hasil+") -Join '|') } | foreach-object { '{0};{1};{2};{3}' -f $_.Name, [System.Diagnostics.FileVersionInfo]::GetVersionInfo($_).FileVersion, $_.lastwritetime,($_.length) }\"";
                                console.log("COMMAND KIRIM : "+command_kirim);
                                //console.log("-------------------------------------------");
                                pub_bc_acuan("BC_POWERSHELL_COMMAND",topic_bc,command_kirim,kdtk,ip);
                                console.log("=================================================");

                            });
                    }catch(exc){
                        console.log("ERROR : "+exc);
                    }
                    await sleep(jeda_pengecekan_versi);
            }
           
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
        const Parser_FROM= "ServiceProgram";
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