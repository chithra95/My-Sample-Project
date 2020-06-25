/**************************************************************************** 
 * Job:             MVBS_CASO_REPORT1                     A5V8BXSQ.AR0001BU * 
 * Description:                                                             * 
 *                                                                          * 
 * Metadata Server: bire-sas94-meta-dev.srv.allianz                         * 
 * Port:            8561                                                    * 
 * Location:        /ETL/tdbpcd/Jobs/MVBS/MVBS_REPORTING                    * 
 *                                                                          * 
 * Server:          tdbpcd                                A5HYR2NS.AS00001O * 
 *                                                                          * 
 * Source Tables:   MVBS_OUTPUT - RESASDWH.MVBS_OUTPUT    A5V8BXSQ.AE0001W7 * 
 *                  MVBS_LOG - RESASDWH.MVBS_LOG          A5V8BXSQ.AE0001W3 * 
 * Target Table:    MVBS_CASO_REPORT1 -                   A5V8BXSQ.AE0001UY * 
 *                   RESASDWH.MVBS_CASO_REPORT1                             * 
 *                                                                          * 
 * Generated on:    Tuesday, January 24, 2017 3:55:24 PM IST                * 
 * Generated by:    re00580                                                 * 
 * Version:         SAS Data Integration Studio 4.901                       * 
 ****************************************************************************/ 

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A5V8BXSQ.AR0001BU);
%let etls_jobName = %nrquote(MVBS_CASO_REPORT1);
%let etls_userID = %nrquote(re00580);

/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%global etls_stepStartTime; 
/* initialize syserr to 0 */
data _null_; run;

%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet; 

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 

/* Create metadata macro variables */
%let IOMServer      = %nrquote(tdbpcd);
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(ibire-sas94-meta-dev.srv.allianz);

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;

%global etls_debug; 
%macro etls_setDebug; 
   %if %str(&etls_debug) ne 0 %then 
      OPTIONS MPRINT%str(;); 
%mend; 
%etls_setDebug; 

/*---- Start of Pre-Process Code  ----*/ 

/*Call to an initialization macro  */
%mvbsJobInit;
/*---- End of Pre-Process Code  ----*/ 

%rcSet(&syserr); 
%rcSet(&sqlrc); 

/*==========================================================================* 
 * Step:            SQL Join                              A5V8BXSQ.AW0003H3 * 
 * Transform:       Join                                                    * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   MVBS_OUTPUT - RESASDWH.MVBS_OUTPUT    A5V8BXSQ.AE0001W7 * 
 *                  MVBS_LOG - RESASDWH.MVBS_LOG          A5V8BXSQ.AE0001W3 * 
 * Target Table:    MVBS_CASO_REPORT1 -                   A5V8BXSQ.AE0001UY * 
 *                   RESASDWH.MVBS_CASO_REPORT1                             * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003H3);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

%macro etls_prepareTarget();
   proc datasets lib = RESASDWH nolist nowarn memtype = (data view);
      delete MVBS_CASO_REPORT1;
   quit;
   
   /*---- Create a new table  ----*/ 
   data RESASDWH.MVBS_CASO_REPORT1
           (dbnull = (
                      LEGAL_ENTITY_CODE = YES
                      GROSSRETRO = YES
                      AMT_TYPE = YES
                      AMOUNT_DISCOUNTED = YES
                      AMOUNT_UNDISCOUNTED = YES
                      LOAD_TIMESTAMP = YES));
      attrib LEGAL_ENTITY_CODE length = $6
         format = $6.
         informat = $6.
         label = 'LEGAL_ENTITY_CODE'; 
      attrib GROSSRETRO length = $10
         format = $10.
         informat = $10.
         label = 'GROSSRETRO'; 
      attrib AMT_TYPE length = $100
         format = $100.
         informat = $100.
         label = 'AMT_TYPE'; 
      attrib AMOUNT_DISCOUNTED length = 8
         label = 'AMOUNT_DISCOUNTED'; 
      attrib AMOUNT_UNDISCOUNTED length = 8
         label = 'AMOUNT_UNDISCOUNTED'; 
      attrib LOAD_TIMESTAMP length = 8
         format = DATETIME20.
         informat = DATETIME20.
         label = 'LOAD_TIMESTAMP'; 
      call missing(of _all_);
      stop;
   run;
   
   %rcSet(&syserr); 
   
   %let etls_tableExist = 0;
   
%mend etls_prepareTarget;
%etls_prepareTarget;

proc sql;
   connect to ORACLE
   ( 
       DBMAX_TEXT=32767 PATH="&ORA_RESASDWH_PATH" user="&ORA_RESASDWH_USER" pw="&ORA_RESASDWH_PWD"
   ); 
   execute 
   ( 
      insert into &ORA_RESASDWH_SCHEMA..MVBS_CASO_REPORT1
      select
         mvbs_output.LEGAL_ENTITY_CODE,
         mvbs_output.GROSSRETRO,
         mvbs_output.AMT_TYPE,
         sum(case when upper(trim(mvbs_output.DISCUNDISC)) = 'DISCOUNTED'  
                THEN mvbs_output.AMOUNT ELSE 0 END ) as AMOUNT_DISCOUNTED,
         sum(case when upper(trim(mvbs_output.DISCUNDISC )) = 'UNDISCOUNTED'  
                THEN mvbs_output.AMOUNT  ELSE 0 END ) as AMOUNT_UNDISCOUNTED,
         sysdate as LOAD_TIMESTAMP
      from
         (
            select
               MVBS_OUTPUT.LINE_NUMBER,
               MVBS_OUTPUT.FILENAME,
               MVBS_OUTPUT.DISCUNDISC,
               MVBS_OUTPUT.LEGAL_ENTITY_NAME,
               MVBS_OUTPUT.MANDANT_ID,
               MVBS_OUTPUT.PROP_NONPROP,
               MVBS_OUTPUT.CEDENT_DOM_ID,
               MVBS_OUTPUT.INTEXT,
               MVBS_OUTPUT.CONVENTIONAL_SCOB_IN_SICS,
               MVBS_OUTPUT.RES_SEG_LEVEL1,
               MVBS_OUTPUT.RES_SEG_LEVEL2,
               MVBS_OUTPUT.RES_SEG_LEVEL3,
               MVBS_OUTPUT.RES_SEG_LEVEL4,
               MVBS_OUTPUT.RES_SEG_LEVEL5,
               MVBS_OUTPUT.CURR_TO_USE,
               MVBS_OUTPUT.INWARD_BUS_ID,
               MVBS_OUTPUT.OUTWARD_BUS_ID,
               MVBS_OUTPUT.PARTNER_ID_RETRO,
               MVBS_OUTPUT.VBUND_RETRO,
               MVBS_OUTPUT.INTEXT_RETRO,
               MVBS_OUTPUT.REL_YR_ID,
               MVBS_OUTPUT.REL_YR_VAL,
               MVBS_OUTPUT.LRDB_VAL_DATE,
               MVBS_OUTPUT.AMT_TYPE,
               MVBS_OUTPUT.GROSSRETRO,
               MVBS_OUTPUT.PERIOD,
               MVBS_OUTPUT.EUR_AMOUNT,
               MVBS_OUTPUT.AMOUNT,
               MVBS_OUTPUT.PARTNER_ID_GROSS,
               MVBS_OUTPUT.VBUND_GROSS,
               MVBS_OUTPUT.SEGMENT,
               MVBS_OUTPUT.LEGAL_ENTITY_CODE,
               MVBS_OUTPUT.REGION,
               MVBS_OUTPUT.COUNTRY_ID_TECHDB,
               MVBS_OUTPUT.ABACUS_SII_LOB,
               MVBS_OUTPUT.CARVE_OUT_LOB,
               MVBS_OUTPUT.GUIDE_6_LOB,
               MVBS_OUTPUT.SUPER_LOB,
               MVBS_OUTPUT.SII_LOB_CODE,
               MVBS_OUTPUT.SII_LOB_STRING,
               MVBS_OUTPUT.SM_LOB,
               MVBS_OUTPUT.PARTNER_ID_GROSS_DERIVED,
               MVBS_OUTPUT.PROP_NONPROP_RETRO,
               MVBS_OUTPUT.LOAD_TIMESTAMP,
               MVBS_OUTPUT.VERSION_ID
            from
               &ORA_RESASDWH_SCHEMA..MVBS_OUTPUT MVBS_OUTPUT
            where
               MVBS_OUTPUT.VERSION_ID IN (
                  select distinct
                     MVBS_LOG.VERSION_ID
                  from
                     &ORA_RESASDWH_SCHEMA..MVBS_LOG MVBS_LOG
                  where
                     MVBS_LOG.BUSINESS_UNIT = 'PC'
                     and MVBS_LOG.REPORTING_PERIOD = %sysfunc(compress(%str(%')&reporting_period.%str(%')))
               )
         ) mvbs_output
      group by
         mvbs_output.LEGAL_ENTITY_CODE,
         mvbs_output.GROSSRETRO,
         mvbs_output.AMT_TYPE
      
   ) by ORACLE; 
   
   %rcSet(&sqlrc); 
   
   disconnect from ORACLE; 
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end SQL Join **/

%let etls_endTime = %sysfunc(datetime(),datetime.);

/* Check return code for job  */ 
%macro etls_jobRCChk; 
   %if (&job_rc ge 5) %then
   %do; 
      %macro etls_sendEmail(address=, message=); 
      
         filename sendMail email "&address" subject='ETL Process Status'; 
      
         %local etls_syntaxcheck; 
         %let etls_syntaxcheck = %sysfunc(getoption(syntaxcheck)); 
         /* Turn off syntaxcheck option to perform following steps  */ 
         options nosyntaxcheck;
      
         data _null_; 
            file sendMail; 
            dttm = put(datetime(),nldatm.); 
            put dttm "&message."; 
         run; 
      
         /* Reset syntaxcheck option to previous setting  */ 
         options &etls_syntaxcheck; 
      %mend etls_sendEmail; 
      %etls_sendEmail 
         (address = &mail_to, 
          Message = %str(&etls_jobName Error auf &run_system : Error%%superq%(syserrortext%))); 
   %end; 
%mend etls_jobRCChk; 
%etls_jobRCChk; 

/* Check return code for job  */ 
%macro etls_jobRCChk; 
   %if (&job_rc ge 5) %then
   %do; 
      %macro etls_sendFile(directory=, filename=, message=); 
      
         filename sendfile "&directory.&filename"; 
      
         %local etls_syntaxcheck; 
         %let etls_syntaxcheck = %sysfunc(getoption(syntaxcheck)); 
         /* Turn off syntaxcheck option to perform following steps  */ 
         options nosyntaxcheck;
      
         data _null_; 
            file sendFile; 
            dttm = put(datetime(),nldatm.); 
            put dttm "&message."; 
         run; 
      
         /* Reset syntaxcheck option to previous setting  */ 
         options &etls_syntaxcheck; 
      %mend etls_sendFile; 
      %etls_sendFile 
         (FileName = &proj_env/run_control/mvbs_error_&etls_jobName, 
          Message = Error); 
   %end; 
%mend etls_jobRCChk; 
%etls_jobRCChk; 

