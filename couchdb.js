'use strict';
//couch
var couch = {
  getOptions: function(db, production) {
// An object of options to indicate where to post to
//TODO: env / config vars
    var req_options = {
      host: production ? 'ebola.eocng.org' : 'dev.ebola.eocng.org',
      port: '5984',
      path: '/' + db,
      auth: 'ebola:nigeria'
    };
//console.log(req_options);
    return req_options;
  },
  readCouch: function(db, production, callback) {
    var http = require('http');
    var options = couch.getOptions(db, production);
    options.method = 'GET';
    options.accept = 'application/json';
    options['content-type'] = 'application/json';
    var req = http.request(options,function(res) {
      res.setEncoding('utf8');
      var body = "";
      res.on('data', function(chunk) {
        body += chunk;
      });
      res.on('end', function() {
        var obj = JSON.parse(body);
        callback(null, obj.rows);
      });
    }).end();
  },
  postToCouch: function(db, jsonString, production, callback) {
    var opt = couch.getOptions(db, production);
    opt.method = 'POST';
    opt.headers = {
      'Content-Type': 'application/json',
      'Content-Length': jsonString.length
    };
// Set up the request
    couch.writeRequest(opt, jsonString, function(chunk) {
      console.log(chunk);
      callback(chunk);
    });
  },
  writeRequest: function(options, jsonString, callback) {
    var http = require('http');
    var req = http.request(options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function(chunk) {
        callback(chunk);
      });
    });
  //post the data
    req.write(jsonString);
    req.end();
  }
};
module.exports = couch;