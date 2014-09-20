'use strict';

var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var q = require("q");
//var pouchdb = require('pouchdb');
var dotenv = require('dotenv');
var log4js = require('log4js');
dotenv.load();


var LOG_CATEGORY = 'mssql-to-couchdb-sync';
log4js.configure({
  appenders: [
    { type: 'console' },
    { type: 'file', filename: 'mssql-to-couchdb.log', category: LOG_CATEGORY }
  ]
});
var logger = log4js.getLogger(LOG_CATEGORY);
var DELAY = 30000;//30 secs
var LOCAL_DB = 'sl_local';
var DEV_DB = process.env.DB_URL;
var PROD_DB = process.env.PROD_DB;
var SQL_USER = process.env.SQL_USER;
var SQL_PASS = process.env.SQL_PASS;
var SQL_HOST = process.env.SQL_HOST;
var CASES_BY_CONTACT_VIEW = process.env.CASE_CONTACT_VIEW;
var inProgress = false;
var config = {
  userName: SQL_USER,
  password: SQL_PASS,
  server: SQL_HOST,
  options: {
    encrypt: true,
    rowCollectionOnRequestCompletion: true
  }
};
logger.info('MS SQL to Couchdb Sync Started.');
logger.info('connecting to MS SQL server, please wait ..');
var connection = new Connection(config);
var contactTable = 'ContactDtls';
var patientTable = 'Patient_Mst';
var patientRespLog = 'Patient_ResponseLog';
var dbNames = [ 'chiefdom', contactTable, patientTable, 'province', 'Patient_ResponseLog'];
var tableMap = {
  ContactDtls: 'contact',
  chiefdom: 'chiefdom',
  Patient_Mst: 'patient',
  Patient_ResponseLog: 'response'
};
var propertyMap = {
  chiefdom: {
    vPcode: 'provinceCode',
    vDcode: 'districtCode',
    vCcode: 'chiefdomCode',
    vCreatedBy: 'createdBy',
    vCdescr: 'chiefdomName',
    dtCreatedOn: 'createdOn',
    vModifiedBy: 'modifiedBy',
    dtModifiedOn: 'modifiedOn'
  },
  contact: {
    vContactId: 'contactId',
    vContactName: 'name',
    vContactAddress: 'address',
    vPCode: 'province_code',
    vDCode: 'district_code',
    vCcode: 'chiefdom_code',
    vPhoneno1: 'phoneNo',
    vPhoneno2: 'otherPhoneNo',
    vInstitueName: 'instituteName',
    vInstitueType: 'instituteType',
    vCreatedBy: 'createdBy',
    vCreatedon: 'createdOn',
    vModifiedby: 'modifiedBy',
    vModifiedOn: 'modifiedOn',
    vCallNature: 'callNature',
    vCallDetails: 'callDetails',
    vActionRequied: 'actionRequired',
    btPrankCall: 'isPrankCall',
    vCaseStatus: 'caseStatus'
  },
  response: {
    intTranId: 'tranId',
    dtTranDate: 'tranDate',
    vPID: 'patientId',
    vContactID: 'contactId',
    vTeamName: 'teamName',
    vTeamPhoneNo1: 'phoneNo',
    vTeachPhoneNo2: 'phoneNo2',
    vDistrictDtls: 'districtName',
    vActionToTeam: 'actionRecommended',
    vFeedbackFromTram: 'teamFeedback',
    vPatientStatus: 'patientStatus',
    vCaseStatus: 'caseStatus',
    vCreatedBy: 'createdBy',
    dtCreatedOn: 'createdOn',
    vModifiedBy: 'modifiedBy',
    dtModifiedon: 'modifiedOn'
  },
  patient: {
    vPID: 'patientId',
    vPName: 'patientName',
    nPAge: 'age',
    vPGender: 'gender',
    vPAddress: 'address',
    vPCode: 'provinceCode',
    vDCode: 'districtCode',
    vCcode: 'chiefdomCode',
    vPPhoneNo: 'phoneNo',
    vPFamilyPhoneNo: 'familyPhoneNo',
    vCreatedBy: 'createdBy',
    dtCreatedOn: 'createdOn',
    vModifiedBy: 'modifiedBy',
    dtModifiedon: 'modifiedOn',
    vContactId: 'contactId',
    vPatientStatus: 'patientStatus'
  }
};

var getColName = function(record) {
  return record.metadata.colName;
};

var cleanUpRecord = function(record, type) {
  var colName;
  var cleanName;
  var dirtyName;
  var docType;
  docType = tableMap[type];
  if (!docType) {
    docType = type;
  }
  var cleanRecord = {
    doc_type: docType
  };
  record.forEach(function(column) {
    dirtyName = getColName(column);
    cleanName = propertyMap[docType][dirtyName];
    colName = cleanName ? cleanName : dirtyName;
    cleanRecord[colName] = column.value;
  });
  if (cleanRecord.doc_type === 'response') {
    if (cleanRecord.caseStatus !== 'FOLLOW UP COMPLETE') {
      cleanRecord.caseStatus = 'open';
    } else {
      cleanRecord.caseStatus = 'close';
    }
  }
  return cleanRecord;
};

var addCaseStatus = function(caseRecord) {
  if (caseRecord.response) {
    if (caseRecord.response.caseStatus !== 'FOLLOW UP COMPLETE') {
      caseRecord.response.caseStatus = 'open';
    } else {
      caseRecord.response.caseStatus = 'close';
    }
  } else {
    caseRecord.response = {
      caseStatus: 'open'
    };
  }
  return caseRecord;
};

var recordToDoc = function(type, record) {
  if (typeof record === 'undefined') {
    logger.warn('record is undefined.');
    return;
  }
  if (dbNames.indexOf(type) !== -1) {
    return cleanUpRecord(record, type);
  } else {
    logger.warn('unknown document type: ' + type);
  }
};

var getRecord = function(table, key, value) {
  var deferred = q.defer();
  var query = [ 'SELECT * FROM', table, 'WHERE', key, '= '].join(' ');
  query = query + "'" + value + "'";
  var doc;
  var request = new Request(query, function(err, rowCount, rows) {
    if (err) {
      deferred.reject(err);
      return;
    }
    if (rows.length > 0) {
      doc = recordToDoc(table, rows[0]);
      deferred.resolve(doc);
    } else {
      deferred.reject('record not found: table: ' + table + ', PK: ' + key + ', value: ' + value);
    }
  });
  connection.execSql(request);
  return deferred.promise;
};

var createCase = function(patient) {
  //TODO: use variable to hold table field name such as ''vContactId'',
  var reportedCase = { patient: patient, doc_type: "case" };
  var contactPromise = getRecord(contactTable, 'vContactId', patient.contactId);
  var deferred = q.defer();
  contactPromise
    .then(function(res) {
      reportedCase.contact = res;
      getRecord(patientRespLog, 'vPID', patient.patientId)
        .then(function(resp) {
          reportedCase.response = resp;
          reportedCase = addCaseStatus(reportedCase);
          deferred.resolve(reportedCase);
        })
        .catch(function(err) {
          logger.warn(err);
          reportedCase = addCaseStatus(reportedCase);
          deferred.resolve(reportedCase);
        });
    });
  return deferred.promise;
};

var generateCases = function(patients) {
  var cases = [];
  var deferred = q.defer();
  function getNextCase(casePatients, index) {
    var nextIndex = index - 1;
    if (nextIndex >= 0) {
      var patient = casePatients[nextIndex];
      logger.info('creating case for patient : ' + patient.patientId);
      createCase(patient)
        .then(function(res) {
          cases.push(res);
        })
        .finally(function() {
          getNextCase(casePatients, nextIndex);
        });
    } else {
      deferred.resolve(cases);
    }
  }
  getNextCase(patients, patients.length);
  return deferred.promise;
};

function pullAndPushToCouchdb() {
  inProgress = true;
  var request = new Request("SELECT * FROM " + contactTable, function(err, rowCount, rows) {
    if (err) {
      logger.error(err);
      inProgress = false;
      return;
    }

    var contacts = rows
      .map(function(row) {
        return recordToDoc(contactTable, row);
      });
    //TODO: change db name from 'test' remote db address.
    getCasesByContactId(contacts)
      .then(function(newCases) {
        console.log(newCases);
        return;
        return generateCases(newCases)
          .then(function(newCases) {
            return postToLocalCouch(newCases)
              .then(function(res) {
                inProgress = false;
                logger.info(res.length + ' New cases uploaded to local db successfully.');
              })
              .catch(function(err) {
                inProgress = false;
                logger.error('Push to local db failed: ' + err);
              });
            //TODO: uncomment to post to remote couchdb
//        postToCouchdb(res)
//          .then(function() {
//            console.info('New cases uploaded successfully.');
//          })
//          .catch(function(err) {
//            console.error('Pushing cases to couchdb failed: ' + err);
//          });
          })
          .catch(function(err) {
            logger.error('Case Generation Failed:' + err);
            inProgress = false;
          });
      })
      .catch(function() {
        inProgress = false;
        logger.info('Error while filtering new patients.', err);
      });

  });
  connection.execSql(request);
};

//var postToCouchdb = function(docs) {
//  var db = new pouchdb(DEV_DB);
//  var contactIds = docs.map(function(doc) {
//    return doc.contact.contactId;
//  });
//  var options = {
//    include_docs: true,
//    keys: contactIds
//  };
//  return db.query(CASES_BY_CONTACT_VIEW, options)
//    .then(function(res) {
//      var remoteContactIds = res.rows
//        .map(function(doc) {
//          return doc.doc.contact.contactId;
//        });
//      var newCases = docs
//        .filter(function(newCase) {
//          var contact = newCase.contact;
//          return contact && contact.contactId && remoteContactIds.indexOf(contact.contactId) === -1;
//        });
//      logger.info(newCases.length + ' New Cases to be uploaded to couchdb.');
//      //push new cases to couchdb
//      return db.bulkDocs(newCases);
//    });
//};

//var postToLocalCouch = function(sqlCases) {
//  var db = new pouchdb(LOCAL_DB);
//  var caseContactIdMap = function(doc) {
//    if (doc.doc_type === 'case') {
//      emit(doc.contact.contactId, doc);
//    }
//  };
//  return db.query(caseContactIdMap)
//    .then(function(res) {
//      var uploadedCasesContactIds = res.rows
//        .map(function(row) {
//          return row.key;
//        });
//      var newCases = sqlCases.filter(function(c) {
//        var contact = c.contact;
//        return uploadedCasesContactIds.indexOf(contact.contactId) === -1;
//      });
//      logger.info(newCases.length + ' New Cases to be uploaded to local couchdb.');
//      var cases = newCases.map(function(caseDoc) {
//        return addCaseStatus(caseDoc);
//      });
//      return db.bulkDocs(cases);
//    });
//};

//var replicateLocalToRemote = function() {
//  logger.info('Replicating to remote production db.');
//  pouchdb.replicate(LOCAL_DB, PROD_DB, { since: 6723 })
//    .then(function(res) {
//      logger.info('Replication was successful: ' + JSON.stringify(res));
//    })
//    .catch(function(err) {
//      logger.error('Replication Failed: ' + err);
//    })
//    .finally(function() {
//      logger.info('Replicating to remote dev db.');
//      pouchdb.replicate(LOCAL_DB, DEV_DB, { since: 6723 })
//        .then(function(res) {
//          logger.info('Replication was successful: ' + JSON.stringify(res));
//        })
//        .catch(function(err) {
//          logger.error('Replication Failed: ' + err);
//        });
//    });
//};

//var getNewPatients = function(dbUrl, patients) {
//  var db = new pouchdb(dbUrl);
//  var patientCaseMapFun = function(doc) {
//    if (doc.doc_type === 'case' && doc.patient) {
//      emit(doc.patient.patientId, doc);
//    }
//  };
//  return db.query(patientCaseMapFun)
//    .then(function(res) {
//      var patientIds = res.rows
//        .map(function(row) {
//          return row.key;
//        });
//      var newPatients = patients.filter(function(p) {
//        return patientIds.indexOf(p.patientId) === -1;
//      });
//      return newPatients;
//    })
//    .catch(function(err) {
//      console.log(err);
//    });
//};

//Main Program

//connection.on('connect', function(err) {
//    if (err) {
//      logger.error(err);
//      return;
//    }
//    logger.info('Connection was successful.');
//    setInterval(function() {
//      if (inProgress === true) {
//        logger.info('Last job is still in progress.');
//        return;
//      }
//      pullAndPushToCouchdb();
//    }, DELAY);
//  }
//);

//replicateLocalToRemote();

//var db = pouchdb(LOCAL_DB);
//
//var nationwideCallSummary = function(doc) {
//  var queryDate = new Date('2014-09-20');
//  if (doc.doc_type === 'case') {
//    var createdDate = new Date(doc.contact.createdOn);
//    if (createdDate.getFullYear() === queryDate.getFullYear()
//      && createdDate.getMonth() === queryDate.getMonth()
//      && createdDate.getDate() === queryDate.getDate()
//      && doc.contact.isPrankCall !== true) {
//      emit(createdDate.getDate(), doc);
//    }
//  }
//};
//
//var callSummaryByDistrict = function(doc){
//  var queryDate = new Date('2014-09-20');
//  var DISTRICT_CODE = '32';
//  if (doc.doc_type === 'case') {
//    var createdDate = new Date(doc.contact.createdOn);
//    if (createdDate.getFullYear() === queryDate.getFullYear()
//      && createdDate.getMonth() === queryDate.getMonth()
//      && createdDate.getDate() >= queryDate.getDate()
//      && doc.contact.isPrankCall === true) {
//      emit(createdDate.getDate(), doc);
//    }
//  }
//};
//
//db.query(nationwideCallSummary, { include_docs: true })
//  .then(function(res) {
//    var callNatureStats = {};
//    console.log('Total calls: '+res.rows.length);
//    res.rows
//      .forEach(function(row){
//        var caseDoc = row.doc;
//        var  cur = callNatureStats[caseDoc.contact.callNature];
//        if(isNaN(cur)){
//          cur = 0;
//        }
//        cur = cur + 1;
//        callNatureStats[caseDoc.contact.callNature] = cur;
//      });
//    console.log(callNatureStats);
//  })
//  .catch(function(err) {
//    console.error(err);
//  });

//var couchdb = require('felix-couchdb');
//console.log(couchdb);
//var client = couchdb.createClient(80,'https://ebola:nigeria@dev.couchdb.ebola.eocng.org');
//console.log(client);
//var db = client.db('sl_call_centre');
//db.allDocs(function(err, res){
//  console.log(err);
//  console.log(res);
//});

//


var request = require('request');

var getCasesByContactId = function(contacts) {
  var Ids = contacts.map(function(c){
    return c.contact.contactId;
  });
  var dfd = q.defer();
  var url = [
    DEV_DB,
    '_design/cases/_view/by_contact_id?keys=',
    JSON.stringify(Ids),
    '&include_docs=true'
  ].join('');
  url = encodeURI(url);
  request(url, function(error, response, body) {
    if (!error && response.statusCode == 200) {
      var rows = JSON.parse(body).rows;
      var caseContactIds = rows.map(function(row) {
        return row.key;
      });
      var newCases = contacts.filter(function(c) {
        return caseContactIds.indexOf(c.contact.contactId) === -1;
      });
      dfd.resolve(newCases);
    } else {
      dfd.reject(error);
    }
  });
  return dfd.promise;
};


var contact = {
   "_id": "DF3B33D0-C8DA-3C85-8CF9-1B5FC25603A9",
   "_rev": "1-4e70b7dec34f8caa859c74d57b64d3c4",
   "doc_type": "case",
   "patient": {
       "doc_type": "patient",
       "patientId": "P092100855",
       "patientName": "",
       "age": 0,
       "gender": "M",
       "address": "NO1 BODY BLANGO STREET",
       "provinceCode": "S",
       "districtCode": "03",
       "chiefdomCode": "08",
       "phoneNo": "",
       "familyPhoneNo": "",
       "createdBy": "ELEA",
       "createdOn": "2014-09-20T17:23:00.760Z",
       "modifiedBy": null,
       "contactId": "C092100859",
       "patientStatus": null,
       "modifiedOn": null
   },
   "contact": {
       "doc_type": "contact",
       "contactId": "1C092100859",
       "name": "UMU JANNEH",
       "address": "NO 2 BODY BLANGO STREET, NEAR MUSA TARAWALLIE STREET, MOYAMBA",
       "province_code": "S",
       "district_code": "03",
       "chiefdom_code": "08",
       "phoneNo": "077574833",
       "otherPhoneNo": "",
       "instituteName": null,
       "instituteType": null,
       "createdBy": "ELEA",
       "createdOn": "2014-09-20T17:23:00.733Z",
       "modifiedBy": "JOSE",
       "modifiedOn": "2014-09-20T17:47:09.103Z",
       "callNature": "OTHER",
       "callDetails": "2 MEN APPEARED IN HER AREE AND SHE HAS NEVER SEEN THEM BEFORE, SHE IS A BIT SCARED CAUSE THEY ARE VERY ISOLATED",
       "actionRequired": "SEND A SURVIELLANCE TEAM THERE",
       "isPrankCall": false,
       "caseStatus": "ACTION"
   },
   "response": {
       "doc_type": "response",
       "tranId": 21789,
       "tranDate": "2014-09-20T17:44:32.880Z",
       "patientId": "P092100855",
       "contactId": "C092100859",
       "teamName": "0",
       "phoneNo": "0",
       "phoneNo2": "0",
       "vEmailID": "0",
       "districtName": "0",
       "actionRecommended": "NEED THE ATTENTION OF THE POLICE",
       "teamFeedback": null,
       "patientStatus": null,
       "caseStatus": "open",
       "createdBy": "MOHA4",
       "createdOn": "2014-09-20T17:44:32.880Z",
       "modifiedBy": null,
       "modifiedOn": null
   }
};

var ids = [contact];
getCasesByContactId(ids)
  .then(function(res) {
    console.info(res);
  })
  .catch(function(err) {
    console.error(err);
  });

var replicate = function(to, from, options){
  var url = 'https://ebola:nigeria@dev.couchdb.ebola.eocng.org/_replicate';
  var requestOptions = {
    method: "POST",
    uri: url,
    json: { "source" : "alert_tests", "target": "alerts" }
  }
  request(requestOptions, function(err, res, body){
    if(err){
      console.error('Error: '+JSON.stringify(err));
      return;
    }
    console.info(res.body);
  });
};

//replicate();

//return;
//var request = require('request');
//var url = encodeURI(DEV_DB + '_design/cases/_view/by_contact_id?keys=["0000D2C6-340F-A193-AA4B-6A44DF3F85D7", "ZS F"]&include_docs=true');
//console.log(url);
//
//
//request(url, function(error, response, body) {
//  if (!error && response.statusCode == 200) {
//    console.log(body)
//  } else {
//    console.log(error);
//  }
//})

