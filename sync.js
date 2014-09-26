var dotenv = require('dotenv');
var log4js = require('log4js');
dotenv.load();
var pouchdb = require('pouchdb');

var LOG_CATEGORY = 'sync-service';
log4js.configure({
  appenders: [
    { type: 'console' },
    { type: 'file', filename: 'sync-service.log', category: LOG_CATEGORY }
  ]
});
var logger = log4js.getLogger(LOG_CATEGORY);
var LOCAL_DB = 'sl_test';
var DEV_DB = process.env.DB_URL;
var PROD_DB = process.env.PROD_DB;


//Main Program
var options = { live: true, batch_size: 500 };

var source = LOCAL_DB;
var db  = pouchdb(source);

var sync = PouchDB.sync(source, DEV_DB, options)
  .on('change', function (info) {
    logger.info('Change : '+info);
  })
  .on('complete', function (info) {
    logger.info('Complete: '+info);
  })
  .on('UpTodate', function (info) {
    logger.info('UpTodate: '+info);
  })
  .on('error', function (err) {
    logger.error('Error: '+err);
  });