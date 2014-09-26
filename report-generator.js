                'use strict';

                var Connection = require('tedious').Connection;
                var Request = require('tedious').Request;
                var q = require("q");
                //var pouchdb = require('pouchdb');
                var request = require('request');
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
                var DEV_DB = 'https://ebola:nigeria@dev.couchdb.ebola.eocng.org/sl_call_centre/';
                var PROD_DB = 'https://ebola:nigeria@couchdb.ebola.eocng.org/sl_call_centre/';
                var SQL_USER = 'jide';
                var SQL_PASS = 'jide@123';
                var SQL_HOST = '192.168.10.5';
                var CASES_BY_CONTACT_VIEW = 'cases/by_contact_id';
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

                var DISTRICT_CODES = {
                  '01': 'BO',
                  '02': 'BONTHE',
                  '03': 'MOYAMBA',
                  '04': 'PUJEHUN',
                  '05': 'BONTHE ISLAND',
                  '11': 'KAILAHUN',
                  '12': 'KENEMA',
                  '13': 'KONO',
                  '21': 'BOMBALI',
                  '22': 'KAMBIA',
                  '23': 'KOINADUGU',
                  '24': 'PORT LOKO',
                  '25': 'TONKOLILI',
                  '31': 'WESTERN URBAN',
                  '32': 'WESTERN RURAL'
                };

                var callNature = ['HEALTH INFORMATION', 'OTHER', 'SICK', 'DEATH', 'SUSPECT', 'OTHER', 'QUARANTINE'];

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
                    if((colName === 'createdOn' || colName === 'modifiedOn') && (column.value !== null || column.value !== undefined)){
                      cleanRecord[colName] = new Date(column.value).toJSON();
                    }else{
                      cleanRecord[colName] = column.value;
                    }
                    
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

                var generateReport = function(reportDate, cases){
                  var request = new Request("SELECT * FROM " + patientTable, function(err, rowCount, rows) {
                    var endDate = new Date(reportDate.getFullYear(), reportDate.getMonth(), reportDate.getDate() + 1);
                
                      var reportDateCases = cases.filter(function(caseDoc){
                        var createdDate = new Date(caseDoc.contact.createdOn);
                        return (createdDate >= reportDate) && (createdDate <= endDate) && caseDoc.contact.isPrank !== true;
                      });

                      console.log('Running NationWide Report for '+reportDate.toJSON());
                      var report = generateCaseReport(reportDateCases);
                      console.log('Nation Wide Report');
                      console.log('Total: '+reportDateCases.length);
                      console.log(report);

                      console.log();
                      
                      console.log('Running report for districts.');

                      //TODO: not efficient, refactor later.
                    var DISTRICT_CODES = {
                    '01': 'BO',
                    '02': 'BONTHE',
                    '03': 'MOYAMBA',
                    '04': 'PUJEHUN',
                    '05': 'BONTHE ISLAND',
                    '11': 'KAILAHUN',
                    '12': 'KENEMA',
                    '13': 'KONO',
                    '21': 'BOMBALI',
                    '22': 'KAMBIA',
                    '23': 'KOINADUGU',
                    '24': 'PORT LOKO',
                    '25': 'TONKOLILI',
                    '31': 'WESTERN URBAN',
                    '32': 'WESTERN RURAL'
                  };

                      var districtReport = {};
                      for(var k in DISTRICT_CODES){

                        var district = DISTRICT_CODES[k];
                        console.log('Report Date Caes: '+reportDateCases.length);
                        var districtCases = [];
                        reportDateCases.forEach(function(caseDoc){
                          if(caseDoc.contact && caseDoc.contact.district_code === k){
                            districtCases.push(caseDoc);
                          }
                        });
                        console.log(districtCases[0]);
                        districtReport[district] = generateCaseReport(districtCases);
                      }
                      console.log('District rpeort for: '+reportDate.toJSON());
                      console.log(districtReport);
                  });
                  connection.execSql(request);
                };

                    var generateCaseReport = function(cases){
                      var callNatureStats = {
                        'HEALTH INFORMATION': 0, 
                        'OTHER': 0, 
                        'SICK': 0, 
                        'DEATH': 0, 
                        'SUSPECT': 0,
                        'QUARANTINE': 0
                      };
                      cases.forEach(function(caseDoc){
                        callNatureStats[caseDoc.contact.callNature] += 1;
                      });
                      return callNatureStats;
                    };

                //Main Program

                // connection.on('connect', function(err) {
                //     if (err) {
                //       logger.error(err);
                //       return;
                //     }
                //     logger.info('Connection was successful.');
                //     var reportDate = new Date('2014-09-21');
                //     generateReport(reportDate);
                //   }
                // );



  var reportDate = new Date('2014-09-21T00:00:00Z');

  var getCases = function(db){
    var dfd = q.defer();
    var url = db+'_all_docs?include_docs='+true;
    var requestOptions = {
      method: "GET",
      uri: url
    };
    request(requestOptions, function(err, res, body) {
      if (err) {
        dfd.reject(err);
      } else {
        dfd.resolve(JSON.parse(res.body));
      }
    });
    return dfd.promise;
  };

  getCases(PROD_DB)
    .then(function(res){
      var cases = res.rows
      .map(function(row){
        return row.doc;
      })
      .filter(function(doc){
        return doc.doc_type === 'case';
      });
      generateReport(reportDate, cases);
    })
    .catch(function(err){
      console.log(err);
    });


