'use strict';

var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var q = require("q");
var request = require('request');
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
var DELAY = 600000;//10 mins interval.
var LOCAL_DB = process.env.LOCAL_DB;
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
          reportedCase = addCaseStatus(reportedCase);
          deferred.resolve(reportedCase);
        });
    })
    .catch(function(err){
      deferred.reject(err);
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
  var request = new Request("SELECT * FROM " + patientTable, function(err, rowCount, rows) {
    if (err) {
      logger.error(err);
      inProgress = false;
      return;
    }
    var patients = rows
      .map(function(row) {
        return recordToDoc(patientTable, row);
      })
      .filter(function(p){
        return new Date(p.createdOn) >= new Date('2014-10-03');
      });
    generateCases(patients)
      .then(function(cases){
        getNewCases(LOCAL_DB, cases)
         .then(function(newCases){
            bulkDocs(LOCAL_DB, newCases)
            .then(function(res){
              if('length' in  res){
                logger.info(res.length+' cases uploaded to couchdb.');
              }else{
                logger.info(res);
              }
              inProgress = false;
            })
            .catch(function(err){
              inProgress = false;
              logger.error(err);
            });
         })
         .catch(function(err){
          inProgress = false;
          logger.error(err);
         });
      })
      .catch(function(err){
        inProgress = false;
        logger.error(err);
      });
      
  });
  connection.execSql(request);
};

var setLastUpdated = function(createdOn){
  var db = pouchdb('updateInfo');
  return db.put();
};

var getNewCases = function(dbUrl, cases){
  var url = encodeURI(dbUrl + '_design/calls/_view/created_on');
  var dfd = q.defer();
  request(url, function(error, response, body) {
   if (!error && response.statusCode == 200) {
    var res = JSON.parse(response.body);
    var uniqueCaseId = res.rows.map(function(row){
      return row.value;
    });
    
    var newCases = cases.filter(function(caseDoc){
      return caseDoc && caseDoc.contact && uniqueCaseId.indexOf(caseDoc.contact.contactId) === -1;
    });

    dfd.resolve(newCases);
   } else {
    dfd.reject(error);
   }
  });
  return dfd.promise;
};


var bulkDocs = function(db, docs) {
 var body = {
   docs: docs
 };
 var dfd = q.defer();
 var url = [db, '_bulk_docs'].join('');
 var requestOptions = {
   method: "POST",
   uri: url,
   json: body
 }
 request(requestOptions, function(err, res, body) {
   if (err) {
     dfd.reject(err);
   } else {
     dfd.resolve(res.body);
   }
 });
 return dfd.promise;
};


//Main Program
connection
 .on('connect', function(err) {
   if (err) {
     logger.error(err);
     connection = new Connection(config);
     return;
   }
   logger.info('Connection was successful.');
   setInterval(function() {
     if (inProgress === true) {
       return;
     }
     try{
      pullAndPushToCouchdb();
    }catch(err){
      logger.error(err);
      inProgress = false;
    }
   }, DELAY);
 }
)
 .on('end', function(){
    inProgress = false;
    logger.error('Server restart or shutdown.');
    connection = new Connection(config);
 })
 .on('error', function(err){
    inProgress = false;
    logger.error(err);
    connection = new Connection(config);
 });


