var dotenv = require('dotenv');
var log4js = require('log4js');
dotenv.load();
//var pouchdb = require('pouchdb');
var request = require('request');

var LOG_CATEGORY = 'create-operator';
log4js.configure({
  appenders: [
    { type: 'console' },
    { type: 'file', filename: 'create-operator.log', category: LOG_CATEGORY }
  ]
});
var logger = log4js.getLogger(LOG_CATEGORY);
var LOCAL_DB = 'sl_test';
var DEV_DB = process.env.DB_URL;
var PROD_DB = process.env.PROD_DB;


// var user = { "_id": "org.couchdb.user:operator", "name": "operator", "roles": [ "call_operator" ], "type": "user" ,"password": "testtest" };
// var URL = 'https://admin:everything is awesome.@dev.couchdb.ebola.eocng.org/_users/'+user._id;
// var requestOptions = {
//    method: "PUT",
//    uri: URL,
//    json: user
// }
// request(requestOptions, function(err, res, body) {
//    if (err) {
//      console.log(err);
//    } else {
//      console.info(res.body);
//    }
//  });

var user = { "_id": "org.couchdb.user:operator1", "name": "operator1", "roles": [], "type": "user" ,"password": "testtest" };
var URL = 'https://admin:everything is awesome.@dev.couchdb.ebola.eocng.org/_users/'+user._id;
var requestOptions = {
   method: "PUT",
   uri: URL,
   json: user
}
request(requestOptions, function(err, res, body) {
   if (err) {
     console.log(err);
   } else {
     console.info(res.body);
   }
 });
