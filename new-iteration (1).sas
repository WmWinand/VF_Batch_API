/******************************************************************************
** Macro: Visual Forecasting - New Iteration
**
** Description: Invoke Visual Forecasting REST API to run an existing project with new data.
**
******************************************************************************/

%macro vf_new_iteration(host, projectId, outputCaslib, outputTable);

filename resp TEMP;

/******************************************************************************
** Get authorization token...
******************************************************************************/
/* %put Get authorization token...; */
/* %global authtoken; */
/*  */
/* proc http */
/*   method="POST" */
/*   url="https://&host./SASLogon/oauth/token" */
/*   in="grant_type=password%nrstr(&)username=&username.%nrstr(&)password=&password." */
/*   out=resp; */
/*   headers */
/*     "Authorization"="Basic c2FzLmVjOg==" */
/*     "Accept"="application/json" */
/*     "Content-Type"="application/x-www-form-urlencoded"; */
/* run; */
/* %put Response status: &SYS_PROCHTTP_STATUS_CODE; */
/*  */
/* libname respjson JSON fileref=resp; */
/* data _null_; */
/*   set respjson.root; */
/*   call symput('authtoken', access_token); */
/* run; */
/*  */
/* %if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do; */
/*   %put ERROR: An invalid response was received.; */
/*   %abort; */
/* %end; */

/******************************************************************************
** Load project data...
******************************************************************************/
%put Load project data...;
proc http
  method="PUT"
  url="https://&host./forecastingGateway/projects/&projectId./dataState?scope=input"
  oauth_bearer=sas_services
  out=resp;
/*   headers */
/*     "Authorization"="bearer &authtoken."; */
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

%if not (&SYS_PROCHTTP_STATUS_CODE = 204) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Fetch data specification, checking for updated input data...
******************************************************************************/
%put Fetch data specification, checking for updated input data...;
proc http
  method="GET"
  url="https://&host./forecastingGateway/projects/&projectId./dataDefinitions/@current?checkForUpdates=true"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Accept"="application/vnd.sas.analytics.forecasting.data.definition+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

%if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** If necessary, import new input data...
******************************************************************************/
%put If necessary, import new input data...;
proc http
  method="POST"
  url="https://&host./forecastingGateway/projects/&projectId./dataDefinitions/@current/dataUpdateJobs?category=INPUT"
  oauth_bearer=sas_services
  out=resp;
/*   headers */
/*     "Authorization"="bearer &authtoken."; */
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Run all pipelines...
******************************************************************************/
%put Run all pipelines...;
proc http
  method="POST"
  url="https://&host./forecastingGateway/projects/&projectId./pipelineJobs"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Accept"="application/vnd.sas.job.execution.job+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Wait for pipelines to finish running...
******************************************************************************/
%put Wait for pipelines to finish running...;
%global jobState;

%do %until(&jobState ^= running);

  proc http
    method="GET"
    url="https://&host./forecastingGateway/projects/&projectId./pipelineJobs/@currentJob"
    oauth_bearer=sas_services
    out=resp;
    headers
/*       "Authorization"="bearer &authtoken." */
      "Accept"="application/vnd.sas.job.execution.job+json";
    DEBUG RESPONSE_BODY OUTPUT_TEXT;
  run;
  %put Response status: &SYS_PROCHTTP_STATUS_CODE;

  libname respjson JSON fileref=resp;
  data _null_;
    set respjson.root;
    call symput('jobState', state);
  run;

  %put jobState = &jobState;


  data _null_;
    call sleep(10000);
  run;

%end;

%if not (&jobState = completed) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Fetch override plan...
******************************************************************************/
%put Fetch override plan...;
%global overridePlanId;
%global overridePlanStatus;

proc http
  method="GET"
  url="https://&host./forecastingGateway/projects/&projectId./overridePlan"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Accept"="application/vnd.sas.forecasting.overrides.plan+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

libname respjson JSON fileref=resp;
data _null_;
  set respjson.root;
  call symput('overridePlanId', id);
  call symput('overridePlanStatus', status);
run;

%put overridePlanId = &overridePlanId;
%put overridePlanStatus = &overridePlanStatus;

%if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Check for overrides in pending or conflict state...
******************************************************************************/
%put Check for overrides in pending or conflict state...;
%global pendingOverridesCount;

proc http
  method="GET"
  url="https://&host./forecastingGateway/projects/&projectId./specificationDetails?start=0%nrstr(&)limit=1%nrstr(&)filter=or(eq(status,%27conflict%27),eq(status,%27pending%27))"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Accept"="application/vnd.sas.collection+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

libname respjson JSON fileref=resp;
data _null_;
  set respjson.root;
  call symput('pendingOverridesCount', count);
run;

%put pendingOverridesCount = &pendingOverridesCount;

%if not (&pendingOverridesCount = 0) %then %do;
  %put ERROR: Overrides were found in pending or conflict state. Please submit these overrides first.;
  %abort;
%end;

/******************************************************************************
** Prepare for overrides...
******************************************************************************/
%put Prepare for overrides...;
%global prepareOverrideJobId;

proc http
  method="POST"
  url="https://&host./forecastingGateway/projects/&projectId./dataDefinitions/@current/dataUpdateJobs?category=FORECAST"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Content-Type"="application/vnd.sas.analytics.forecasting.data.specification+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

libname respjson JSON fileref=resp;
data _null_;
  set respjson.root;
  call symput('prepareOverrideJobId', id);
run;

%put prepareOverrideJobId = &prepareOverrideJobId;

%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Wait for all overrides to be prepared...
******************************************************************************/
%put Wait for all overrides to be prepared...;
%global prepareOverrideJobState;

%do %until(&prepareOverrideJobState ^= running);

  proc http
    method="GET"
    url="https://&host./jobExecution/jobs/&prepareOverrideJobId."
    oauth_bearer=sas_services
    out=resp;
    headers
/*       "Authorization"="bearer &authtoken." */
      "Accept"="application/vnd.sas.job.execution.job+json";
    DEBUG RESPONSE_BODY OUTPUT_TEXT;
  run;
  %put Response status: &SYS_PROCHTTP_STATUS_CODE;

  libname respjson JSON fileref=resp;
  data _null_;
    set respjson.root;
    call symput('prepareOverrideJobState', state);
  run;

  %put prepareOverrideJobState = &prepareOverrideJobState;


  data _null_;
    call sleep(3000);
  run;

%end;

%if not (&prepareOverrideJobState = completed) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Wait for override plan to be ready...
******************************************************************************/
%put Wait for override plan to be ready...;

%do %until(&overridePlanStatus ^= applied AND &overridePlanStatus ^= resubmitPreparing);

  proc http
    method="GET"
    url="https://&host./forecastingGateway/projects/&projectId./overridePlan"
    oauth_bearer=sas_services
    out=resp;
    headers
/*       "Authorization"="bearer &authtoken." */
      "Accept"="application/vnd.sas.forecasting.overrides.plan+json";
    DEBUG RESPONSE_BODY OUTPUT_TEXT;
  run;
  %put Response status: &SYS_PROCHTTP_STATUS_CODE;

  libname respjson JSON fileref=resp;
  data _null_;
    set respjson.root;
    call symput('overridePlanStatus', status);
  run;

  %put overridePlanStatus = &overridePlanStatus;


  data _null_;
    call sleep(3000);
  run;

%end;

%if (&overridePlanStatus = unknown) %then %do;
  %put No overrides found.;
  %goto exportData;
%end;

%if not (&overridePlanStatus = resubmitPending) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Resubmit overrides...
******************************************************************************/
%put Resubmit overrides...;
%global planJobId;

proc http
  method="POST"
  url="https://&host./forecastingOverrides/plans/&overridePlanId./jobs"
  in="{""autoResolve"":true}"
  oauth_bearer=sas_services
  out=resp;
  headers
/*     "Authorization"="bearer &authtoken." */
    "Accept"="application/vnd.sas.job.execution.job+json"
    "Content-Type"="application/vnd.sas.forecasting.overrides.plan.job.request+json";
  DEBUG RESPONSE_BODY OUTPUT_TEXT;
run;
%put Response status: &SYS_PROCHTTP_STATUS_CODE;

libname respjson JSON fileref=resp;
data _null_;
  set respjson.root;
  call symput('planJobId', id);
run;

%put planJobId = &planJobId;

%if not (&SYS_PROCHTTP_STATUS_CODE = 202) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Wait for all overrides to be resubmitted...
******************************************************************************/
%put Wait for all overrides to be resubmitted...;
%global planJobState;

%do %until(&planJobState ^= running);

  proc http
    method="GET"
    url="https://&host./jobExecution/jobs/&planJobId."
    oauth_bearer=sas_services
    out=resp;
    headers
/*       "Authorization"="bearer &authtoken." */
      "Accept"="application/vnd.sas.job.execution.job+json";
    DEBUG RESPONSE_BODY OUTPUT_TEXT;
  run;
  %put Response status: &SYS_PROCHTTP_STATUS_CODE;

  libname respjson JSON fileref=resp;
  data _null_;
    set respjson.root;
    call symput('planJobState', state);
  run;

  %put planJobState = &planJobState;

  data _null_;
    call sleep(3000);
  run;

%end;

%if not (&planJobState = completed) %then %do;
  %put ERROR: An invalid response was received.;
  %abort;
%end;

/******************************************************************************
** Export the output data...
******************************************************************************/
%exportData:
%if %length(&outputCaslib) > 0 and %length(&outputTable) > 0 %then %do;
  %put Export the output data...;
  proc http
    method="POST"
    url="https://&host./forecastingGateway/projects/&projectId./dataDefinitions/@current/finalForecasts?overwrite=true%nrstr(&)promote=false"
    in="{""dataSourceUri"":""&outputCaslib."",""tableName"":""&outputTable.""}"
    oauth_bearer=sas_services
    out=resp;
    headers
/*       "Authorization"="bearer &authtoken." */
      "Accept"="application/vnd.sas.data.table+json"
      "Content-Type"="application/vnd.sas.forecasting.table.reference+json";
    DEBUG RESPONSE_BODY OUTPUT_TEXT;
  run;
  %put Response status: &SYS_PROCHTTP_STATUS_CODE;

  %if not (&SYS_PROCHTTP_STATUS_CODE = 201) %then %do;
    %put ERROR: An invalid response was received.;
    %abort;
  %end;
%end;

%mend;

/* Call the macro */

%let host = server.demo.sas.com;
/* %let username = student; */
/* %let password = ; */
%let projectId = 4d08a317-ed5d-4014-b658-318586c0ade9;

/* Set the following to export the output data */
%let outputCaslib = casuser;
%let outputTable = fcst_out;

%vf_new_iteration(&host, &projectId, &outputCaslib, &outputTable);

cas mySess;

%let vfcaslib = Analytics_Project_&projectId;
%put &vfcaslib;

proc casutil;
  list tables incaslib="&vfcaslib";
quit;

cas mySess terminate;