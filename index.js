'use strict';

var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var q = require("q");
var pouchdb = require('pouchdb');
var dotenv = require('dotenv');
dotenv.load();

var COUCHDB_URL = process.env.DB_URL;
var SQL_USER = process.env.SQL_USER;
var SQL_PASS = process.env.SQL_PASS;
var SQL_HOST = process.env.SQL_HOST;
var CASES_BY_CONTACT_VIEW = process.env.CASE_CONTACT_VIEW;

var config = {
  userName: SQL_USER,
  password: SQL_PASS,
  server: SQL_HOST,
  options: {
    encrypt: true,
    rowCollectionOnRequestCompletion: true
  }
};

var connection = new Connection(config);
var contactTable = 'ContactDtls';
var patientRespLog = 'Patient_ResponseLog';
var dbNames = [ 'chiefdom', 'ContactDtls', 'Patient_Mst', 'province', 'Patient_ResponseLog'];

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

var recordToDoc = function(type, record) {
  if (typeof record === 'undefined') {
    console.log('record is undefined.');
    return;
  }
  if (dbNames.indexOf(type) !== -1) {
    return cleanUpRecord(record, type);
  } else {
    console.log('unknown document type: ' + type);
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
          deferred.resolve(reportedCase);
        })
        .catch(function(err) {
          console.error(err);
          deferred.resolve(reportedCase);
        });
    });
  return deferred.promise;
};

var generateCases = function(patients) {
  var cases = [];
  var deferred = q.defer();
  function getNextCase(casePatients, index) {
    console.log('Getting case: '+index);
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
      console.log(cases);
      deferred.resolve(cases);
    }
  }
  getNextCase(patients, patients.length);
  return deferred.promise;
};

function pullAndPushToCouchdb() {
  var table = 'Patient_Mst';

  var request = new Request("SELECT * FROM " + table, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
      return;
    }
    //TODO:
    var casesForDate = new Date('2014-09-16');

    var patients = rows
      .map(function(row) {
        return recordToDoc(table, row);
      })
      .filter(function(p){
        return casesForDate <= p.createdOn;
      });
    console.log('Generating cases for '+patients.length+' patients, please wait ....');
    generateCases(patients)
      .then(function(res) {
        postToLocalCouch(res)
          .then(function() {
            console.log('New cases uploaded to local db successfully.');
          })
          .catch(function(err) {
            console.error('Push to local db failed: ' + err);
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
        console.error('Case Generation Failed:' + err);
      })
      .finally(function() {
        console.info('completed');
      });
  });
  connection.execSql(request);
};

var postToCouchdb = function(docs) {
  var db = new pouchdb(COUCHDB_URL);
  var contactIds = docs.map(function(doc) {
    return doc.contact.contactId;
  });
  var options = {
    include_docs: true,
    keys: contactIds
  };
  return db.query(CASES_BY_CONTACT_VIEW, options)
    .then(function(res) {
      var remoteContactIds = res.rows
        .map(function(doc) {
          return doc.doc.contact.contactId;
        });
      var newCases = docs
        .filter(function(newCase) {
          var contact = newCase.contact;
          return contact && contact.contactId && remoteContactIds.indexOf(contact.contactId) === -1;
        });
      console.info(newCases.length + ' New Cases to be uploaded to couchdb.');
      //push new cases to couchdb
      return db.bulkDocs(newCases);
    });
};

var postToLocalCouch = function(sqlCases) {
  var db = new pouchdb('test');
  var caseContactIdMap = function(doc) {
    if (doc.doc_type === 'case') {
      emit(doc.contact.contactId, doc);
    }
  };
  return db.query(caseContactIdMap)
    .then(function(res) {
      var uploadedCasesContactIds = res.rows
        .map(function(row) {
          return row.key;
        });
      var newCases = sqlCases.filter(function(c) {
        var contact = c.contact;
        return contact && contact.contactId &&
          uploadedCasesContactIds.indexOf(contact.contactId) === -1;
      });
      console.info(newCases.length + ' New Cases to be uploaded to local couchdb.');
      return db.bulkDocs(newCases);
    });
};

var replicateLocalToRemote = function(){
  pouchdb.replicate('test', COUCHDB_URL)
    .then(function(res) {
      console.log('Replication was successful: '+res);
    })
    .catch(function(err) {
      console.error('Replication Failed: '+err);
    });
};

//Main Program


connection.on('connect', function(err) {
    if (err) {
      console.error(err);
      return;
    }
    pullAndPushToCouchdb();
  }
);

//replicateLocalToRemote();