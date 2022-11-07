const { createLogger, format, transports, config } = require('winston');
const { combine, timestamp, json } = format;

const memoryInfoLogger = createLogger({
    levels: config.syslog.levels,
    defaultMeta: {component: 'get_memory_info'},
    format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
    ),
  
    transports: [
       new transports.Console(),
       new transports.File({ filename: 'MemoryInfo.log' })
    ],
    exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
    ]
 });
 
const programInstalledLogger = createLogger({
   levels: config.syslog.levels,
   defaultMeta: {component: 'get_program_installed'},
   format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
     ),
  
   transports: [
       new transports.Console(),
       new transports.File({ filename: 'ProgramInstalled.log' })
   ],
   exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
   ]
 });

const userLogger = createLogger({
   levels: config.syslog.levels,
   defaultMeta: {component: 'get_versi_program_service'},
   format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
     ),
  
   transports: [
       new transports.Console(),
       new transports.File({ filename: 'combined.log' })
   ],
   exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
   ]
 });

const RKEYVatLogger = createLogger({
   levels: config.syslog.levels,
   defaultMeta: {component: 'get_rkey_vat_service'},
   format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
     ),
  
   transports: [
       new transports.Console(),
       new transports.File({ filename: 'combined.log' })
   ],
   exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
   ]
 });

const LockRegistryLogger = createLogger({
   levels: config.syslog.levels,
   defaultMeta: {component: 'lock_registry.log'},
   format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
     ),
  
   transports: [
       new transports.Console(),
       new transports.File({ filename: 'combined.log' })
   ],
   exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
   ]
 });

const ProdmastLogger = createLogger({
   levels: config.syslog.levels,
   defaultMeta: {component: 'prodmast.log'},
   format: combine(
       timestamp({
           format: 'YYYY-MM-DD HH:mm:ss'
       }),
       json()
     ),
  
   transports: [
       new transports.Console(),
       new transports.File({ filename: 'prodmast.log' })
   ],
   exceptionHandlers: [
       new transports.Console(),
       new transports.File({ filename: 'err.log'})
   ]
 });

module.exports = {
 userLogger: userLogger,
 RKEYVatLogger: RKEYVatLogger,
 LockRegistryLogger:LockRegistryLogger,
 ProdmastLogger:ProdmastLogger,
 programInstalledLogger: programInstalledLogger,
 memoryInfoLogger: memoryInfoLogger

};