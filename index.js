'use strict';

var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var q = require("q");

var config = {
  userName: 'jide',
  password: 'jide@1234',
  server: '192.168.10.15',
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

connection.on('connect', function(err) {
    if (err) {
      console.error(err);
      return;
    }
    executeStatement();
  }
);

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
      deferred.reject('record not found: table: ' + table + ', PK: ' + key + ', value: '+value);
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
      getRecord('Patient_ResponseLog', 'vPID', patient.patientId)
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

function executeStatement() {
  var table = 'Patient_Mst';

  var request = new Request("SELECT * FROM "+table, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
      return;
    }
    var today = new Date('2014-09-16');//TODO: generate current date automatically.

    var patients = rows
      .map(function(row) {
        return recordToDoc(table, row);
      })
      .filter(function(p) {
        return p.createdOn >= today;
      });


    generateCases(patients)
      .then(function(res) {
        console.log(res.length);
        //TODO: push to couchdb
      })
      .catch(function(err) {
        console.error(err);
      })
      .finally(function() {
        console.info('done');
      });
  });
  connection.execSql(request);
}