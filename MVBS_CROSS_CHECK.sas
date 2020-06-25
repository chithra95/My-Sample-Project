/**************************************************************************** 
 * Job:             MVBS_CROSS_CHECK                      A5V8BXSQ.AR0001G1 * 
 * Description:                                                             * 
 *                                                                          * 
 * Metadata Server: bire-sas94-meta-dev.srv.allianz                         * 
 * Port:            8561                                                    * 
 * Location:        /ETL/tdbpcd/Jobs/MVBS/MVBS_OUTPUT/MVBS_CHECK            * 
 *                                                                          * 
 * Server:          tdbpcd                                A5HYR2NS.AS00001O * 
 *                                                                          * 
 * Source Tables:   V_MVBS_OUTPUT_CORETP_N -              A5V8BXSQ.AE00021O * 
 *                   RESASDWH.V_MVBS_OUTPUT_CORETP_N                        * 
 *                  META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 *                  META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 *                  V_MVBS_OUTPUT_NCORETP_N -             A5V8BXSQ.AE0001TN * 
 *                   RESASDWH.V_MVBS_OUTPUT_NCORETP_N                       * 
 *                  V_MVBS_OUTPUT_RISKNL_N -              A5V8BXSQ.AE00021R * 
 *                   RESASDWH.V_MVBS_OUTPUT_RISKNL_N                        * 
 *                  META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 *                  MVBS_TEMP_CROSS_CHECK_CORETP -        A5V8BXSQ.AE000213 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_CORE                     * 
 *                  TP                                                      * 
 *                  MVBS_TEMP_CROSS_CHECK_NCORETP -       A5V8BXSQ.AE000214 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_NCOR                     * 
 *                  ETP                                                     * 
 *                  MVBS_TEMP_CROSS_CHECK_RISKNL -        A5V8BXSQ.AE000215 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_RISK                     * 
 *                  NL                                                      * 
 *                  MVBS_CROSS_CHECK_NL_SUM1 -            A5V8BXSQ.AE0001VI * 
 *                   RESASDWH.MVBS_CROSS_CHECK_NL_SUM1                      * 
 *                  MVBS_TEMP_CROSS_CHECK_CORETP -        A5V8BXSQ.AE000213 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_CORE                     * 
 *                  TP                                                      * 
 *                  MVBS_CROSS_CHECK_NL_SUM2 -            A5V8BXSQ.AE0001VJ * 
 *                   RESASDWH.MVBS_CROSS_CHECK_NL_SUM2                      * 
 *                  MVBS_TECHDB_CHECK_NL1 -               A5V8BXSQ.AE000210 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL1                         * 
 *                  MVBS_TECHDB_CHECK_NL2 -               A5V8BXSQ.AE000211 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL2                         * 
 * Target Tables:   MVBS_TEMP_CROSS_CHECK_CORETP -        A5V8BXSQ.AE000213 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_CORE                     * 
 *                  TP                                                      * 
 *                  MVBS_TEMP_CROSS_CHECK_NCORETP -       A5V8BXSQ.AE000214 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_NCOR                     * 
 *                  ETP                                                     * 
 *                  MVBS_TEMP_CROSS_CHECK_RISKNL -        A5V8BXSQ.AE000215 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_RISK                     * 
 *                  NL                                                      * 
 *                  MVBS_CROSS_CHECK_NL_SUM1 -            A5V8BXSQ.AE0001VI * 
 *                   RESASDWH.MVBS_CROSS_CHECK_NL_SUM1                      * 
 *                  MVBS_TECHDB_CHECK_NL1 -               A5V8BXSQ.AE000210 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL1                         * 
 *                  MVBS_CROSS_CHECK_NL_SUM2 -            A5V8BXSQ.AE0001VJ * 
 *                   RESASDWH.MVBS_CROSS_CHECK_NL_SUM2                      * 
 *                  MVBS_TECHDB_CHECK_NL2 -               A5V8BXSQ.AE000211 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL2                         * 
 *                  MVBS_TECHDB_CHECK_NL -                A5V8BXSQ.AE00020Z * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL                          * 
 *                                                                          * 
 * Generated on:    Tuesday, January 24, 2017 3:54:16 PM IST                * 
 * Generated by:    re00580                                                 * 
 * Version:         SAS Data Integration Studio 4.901                       * 
 ****************************************************************************/ 

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A5V8BXSQ.AR0001G1);
%let etls_jobName = %nrquote(MVBS_CROSS_CHECK);
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

/*==========================================================================* 
 * Step:            Join                                  A5V8BXSQ.AW0003V0 * 
 * Transform:       Join                                                    * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   V_MVBS_OUTPUT_CORETP_N -              A5V8BXSQ.AE00021O * 
 *                   RESASDWH.V_MVBS_OUTPUT_CORETP_N                        * 
 *                  META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 * Target Table:    MVBS_TEMP_CROSS_CHECK_CORETP -        A5V8BXSQ.AE000213 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_CORE                     * 
 *                  TP                                                      * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003V0);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recnt = -1;
%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

%macro etls_prepareTarget();
   proc datasets lib = RESASDWH nolist nowarn memtype = (data view);
      delete MVBS_TEMP_CROSS_CHECK_CORETP;
   quit;
   
   /*---- Create a new table  ----*/ 
   data RESASDWH.MVBS_TEMP_CROSS_CHECK_CORETP
           (dbnull = (
                      LEGAL_ENTITY_CODE = YES
                      TDB_LOCAL_LOB_ID = YES
                      ABACUS_SII_LOB = YES
                      TDB_ITEM = YES
                      PROP_NONPROP = YES
                      TDB_GROSS_CEDED = YES
                      TDB_AY_UWY_FLAG = YES
                      TDB_AY_UWY_YEAR = YES
                      TDB_ANNUITIES_FLAG = YES
                      TDB_SETTLE_CURR = YES
                      TDB_VALUE_IN_CON_CURR = YES));
      attrib LEGAL_ENTITY_CODE length = $6
         format = $6.
         informat = $6.
         label = 'LEGAL_ENTITY_CODE'; 
      attrib TDB_LOCAL_LOB_ID length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_LOCAL_LOB_ID'; 
      attrib ABACUS_SII_LOB length = $8
         format = $8.
         informat = $8.
         label = 'ABACUS_SII_LOB'; 
      attrib TDB_ITEM length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ITEM'; 
      attrib PROP_NONPROP length = $8
         format = $8.
         informat = $8.
         label = 'PROP_NONPROP'; 
      attrib TDB_GROSS_CEDED length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_GROSS_CEDED'; 
      attrib TDB_AY_UWY_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_FLAG'; 
      attrib TDB_AY_UWY_YEAR length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_YEAR'; 
      attrib TDB_ANNUITIES_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ANNUITIES_FLAG'; 
      attrib TDB_SETTLE_CURR length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_SETTLE_CURR'; 
      attrib TDB_VALUE_IN_CON_CURR length = 8
         label = 'TDB_VALUE_IN_CON_CURR'; 
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
      insert into &ORA_RESASDWH_SCHEMA..MVBS_TEMP_CROSS_CHECK_CORETP
      select
         V_MVBS_OUTPUT_CORETP_N.LEGAL_ENTITY_CODE,
         V_MVBS_OUTPUT_CORETP_N.TDB_LOCAL_LOB_ID,
         sub.ABACUS_SII_LOB,
         V_MVBS_OUTPUT_CORETP_N.TDB_ITEM,
         sub.PROP_NONPROP,
         V_MVBS_OUTPUT_CORETP_N.TDB_GROSS_CEDED,
         V_MVBS_OUTPUT_CORETP_N.TDB_AY_UWY_FLAG,
         V_MVBS_OUTPUT_CORETP_N.TDB_AY_UWY_YEAR,
         V_MVBS_OUTPUT_CORETP_N.TDB_ANNUITIES_FLAG,
         V_MVBS_OUTPUT_CORETP_N.TDB_SETTLE_CURR,
         V_MVBS_OUTPUT_CORETP_N.TDB_VALUE_IN_CON_CURR
      from
         &ORA_RESASDWH_SCHEMA..V_MVBS_OUTPUT_CORETP_N V_MVBS_OUTPUT_CORETP_N, 
         (
            select distinct
               META_MVBS_LOB_MAPPING.ABACUS_SII_LOB,
               META_MVBS_LOB_MAPPING.CARVE_OUT_LOB,
               META_MVBS_LOB_MAPPING.PROP_NONPROP
            from
               &ORA_RESASDWH_SCHEMA..META_MVBS_LOB_MAPPING META_MVBS_LOB_MAPPING
         ) sub
      where
         V_MVBS_OUTPUT_CORETP_N.TDB_LOCAL_LOB_ID = sub.CARVE_OUT_LOB
         and (V_MVBS_OUTPUT_CORETP_N.TDB_AY_UWY_FLAG IN ('U','A' )
         or V_MVBS_OUTPUT_CORETP_N.TDB_AY_UWY_FLAG IS NULL )
      
   ) by ORACLE; 
   
   %rcSet(&sqlrc); 
   
   disconnect from ORACLE; 
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Join **/

/*==========================================================================* 
 * Step:            Join                                  A5V8BXSQ.AW0003V1 * 
 * Transform:       Join                                                    * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 *                  V_MVBS_OUTPUT_NCORETP_N -             A5V8BXSQ.AE0001TN * 
 *                   RESASDWH.V_MVBS_OUTPUT_NCORETP_N                       * 
 * Target Table:    MVBS_TEMP_CROSS_CHECK_NCORETP -       A5V8BXSQ.AE000214 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_NCOR                     * 
 *                  ETP                                                     * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003V1);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recnt = 0;
%macro etls_recordCheck; 
   %let etls_recCheckExist = %eval(%sysfunc(exist(RESASDWH.META_MVBS_LOB_MAPPING, DATA)) or 
         %sysfunc(exist(RESASDWH.META_MVBS_LOB_MAPPING, VIEW))); 
   
   %if (&etls_recCheckExist) %then
   %do;
      proc sql noprint;
         select count(*) into :etls_recnt from RESASDWH.META_MVBS_LOB_MAPPING;
      quit;
   %end;
%mend etls_recordCheck;
%etls_recordCheck;

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

%macro etls_prepareTarget();
   proc datasets lib = RESASDWH nolist nowarn memtype = (data view);
      delete MVBS_TEMP_CROSS_CHECK_NCORETP;
   quit;
   
   /*---- Create a new table  ----*/ 
   data RESASDWH.MVBS_TEMP_CROSS_CHECK_NCORETP
           (dbnull = (
                      LEGAL_ENTITY_CODE = YES
                      TDB_LOCAL_LOB_ID = YES
                      ABACUS_SII_LOB = YES
                      TDB_ITEM = YES
                      PROP_NONPROP = YES
                      TDB_GROSS_CEDED = YES
                      TDB_AY_UWY_FLAG = YES
                      TDB_AY_UWY_YEAR = YES
                      TDB_ANNUITIES_FLAG = YES
                      TDB_VALUE_IN_CON_CURR = YES));
      attrib LEGAL_ENTITY_CODE length = $6
         format = $6.
         informat = $6.
         label = 'LEGAL_ENTITY_CODE'; 
      attrib TDB_LOCAL_LOB_ID length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_LOCAL_LOB_ID'; 
      attrib ABACUS_SII_LOB length = $8
         format = $8.
         informat = $8.
         label = 'ABACUS_SII_LOB'; 
      attrib TDB_ITEM length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ITEM'; 
      attrib PROP_NONPROP length = $8
         format = $8.
         informat = $8.
         label = 'PROP_NONPROP'; 
      attrib TDB_GROSS_CEDED length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_GROSS_CEDED'; 
      attrib TDB_AY_UWY_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_FLAG'; 
      attrib TDB_AY_UWY_YEAR length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_YEAR'; 
      attrib TDB_ANNUITIES_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ANNUITIES_FLAG'; 
      attrib TDB_VALUE_IN_CON_CURR length = 8
         label = 'TDB_VALUE_IN_CON_CURR'; 
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
      insert into &ORA_RESASDWH_SCHEMA..MVBS_TEMP_CROSS_CHECK_NCORETP
      select
         V_MVBS_OUTPUT_NCORETP_N.LEGAL_ENTITY_CODE,
         V_MVBS_OUTPUT_NCORETP_N.TDB_LOCAL_LOB_ID,
         sub3.ABACUS_SII_LOB,
         V_MVBS_OUTPUT_NCORETP_N.TDB_ITEM,
         sub3.PROP_NONPROP,
         V_MVBS_OUTPUT_NCORETP_N.TDB_GROSS_CEDED,
         V_MVBS_OUTPUT_NCORETP_N.TDB_AY_UWY_FLAG,
         V_MVBS_OUTPUT_NCORETP_N.TDB_AY_UWY_YEAR,
         V_MVBS_OUTPUT_NCORETP_N.TDB_ANNUITIES_FLAG,
         V_MVBS_OUTPUT_NCORETP_N.TDB_VALUE_IN_CON_CURR
      from
         &ORA_RESASDWH_SCHEMA..V_MVBS_OUTPUT_NCORETP_N V_MVBS_OUTPUT_NCORETP_N, 
         (
            select distinct
               META_MVBS_LOB_MAPPING.ABACUS_SII_LOB,
               META_MVBS_LOB_MAPPING.CARVE_OUT_LOB,
               META_MVBS_LOB_MAPPING.PROP_NONPROP
            from
               &ORA_RESASDWH_SCHEMA..META_MVBS_LOB_MAPPING META_MVBS_LOB_MAPPING
         ) sub3
      where
         V_MVBS_OUTPUT_NCORETP_N.TDB_LOCAL_LOB_ID = sub3.CARVE_OUT_LOB
         and (V_MVBS_OUTPUT_NCORETP_N.TDB_AY_UWY_FLAG IN ('U','A' )
         or V_MVBS_OUTPUT_NCORETP_N.TDB_AY_UWY_FLAG IS NULL )
      
   ) by ORACLE; 
   
   %rcSet(&sqlrc); 
   
   disconnect from ORACLE; 
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Join **/

/*==========================================================================* 
 * Step:            Join                                  A5V8BXSQ.AW0003V2 * 
 * Transform:       Join                                                    * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   V_MVBS_OUTPUT_RISKNL_N -              A5V8BXSQ.AE00021R * 
 *                   RESASDWH.V_MVBS_OUTPUT_RISKNL_N                        * 
 *                  META_MVBS_LOB_MAPPING -               A5V8BXSQ.AE0001U5 * 
 *                   RESASDWH.META_MVBS_LOB_MAPPING                         * 
 * Target Table:    MVBS_TEMP_CROSS_CHECK_RISKNL -        A5V8BXSQ.AE000215 * 
 *                  RESASDWH.MVBS_TEMP_CROSS_CHECK_RISK                     * 
 *                  NL                                                      * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003V2);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recnt = -1;
%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

%macro etls_prepareTarget();
   proc datasets lib = RESASDWH nolist nowarn memtype = (data view);
      delete MVBS_TEMP_CROSS_CHECK_RISKNL;
   quit;
   
   /*---- Create a new table  ----*/ 
   data RESASDWH.MVBS_TEMP_CROSS_CHECK_RISKNL
           (dbnull = (
                      LEGAL_ENTITY_CODE = YES
                      TDB_LOCAL_LOB_ID = YES
                      ABACUS_SII_LOB = YES
                      TDB_ITEM = YES
                      PROP_NONPROP = YES
                      TDB_GROSS_CEDED = YES
                      TDB_AY_UWY_FLAG = YES
                      TDB_AY_UWY_YEAR = YES
                      TDB_ANNUITIES_FLAG = YES
                      TDB_VALUE_IN_CON_CURR = YES));
      attrib LEGAL_ENTITY_CODE length = $6
         format = $6.
         informat = $6.
         label = 'LEGAL_ENTITY_CODE'; 
      attrib TDB_LOCAL_LOB_ID length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_LOCAL_LOB_ID'; 
      attrib ABACUS_SII_LOB length = $8
         format = $8.
         informat = $8.
         label = 'ABACUS_SII_LOB'; 
      attrib TDB_ITEM length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ITEM'; 
      attrib PROP_NONPROP length = $8
         format = $8.
         informat = $8.
         label = 'PROP_NONPROP'; 
      attrib TDB_GROSS_CEDED length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_GROSS_CEDED'; 
      attrib TDB_AY_UWY_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_FLAG'; 
      attrib TDB_AY_UWY_YEAR length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_AY_UWY_YEAR'; 
      attrib TDB_ANNUITIES_FLAG length = $1024
         format = $1024.
         informat = $1024.
         label = 'TDB_ANNUITIES_FLAG'; 
      attrib TDB_VALUE_IN_CON_CURR length = 8
         label = 'TDB_VALUE_IN_CON_CURR'; 
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
      insert into &ORA_RESASDWH_SCHEMA..MVBS_TEMP_CROSS_CHECK_RISKNL
      select
         V_MVBS_OUTPUT_RISKNL_N.LEGAL_ENTITY_CODE,
         V_MVBS_OUTPUT_RISKNL_N.TDB_LOCAL_LOB_ID,
         sub2.ABACUS_SII_LOB,
         V_MVBS_OUTPUT_RISKNL_N.TDB_ITEM,
         sub2.PROP_NONPROP,
         V_MVBS_OUTPUT_RISKNL_N.TDB_GROSS_CEDED,
         V_MVBS_OUTPUT_RISKNL_N.TDB_AY_UWY_FLAG,
         V_MVBS_OUTPUT_RISKNL_N.TDB_AY_UWY_YEAR,
         V_MVBS_OUTPUT_RISKNL_N.TDB_ANNUITIES_FLAG,
         V_MVBS_OUTPUT_RISKNL_N.TDB_VALUE_IN_CON_CURR
      from
         &ORA_RESASDWH_SCHEMA..V_MVBS_OUTPUT_RISKNL_N V_MVBS_OUTPUT_RISKNL_N, 
         (
            select distinct
               META_MVBS_LOB_MAPPING.ABACUS_SII_LOB,
               META_MVBS_LOB_MAPPING.CARVE_OUT_LOB,
               META_MVBS_LOB_MAPPING.PROP_NONPROP
            from
               &ORA_RESASDWH_SCHEMA..META_MVBS_LOB_MAPPING META_MVBS_LOB_MAPPING
         ) sub2
      where
         V_MVBS_OUTPUT_RISKNL_N.TDB_LOCAL_LOB_ID = sub2.CARVE_OUT_LOB
         and (V_MVBS_OUTPUT_RISKNL_N.TDB_AY_UWY_FLAG IN ('U','A' )
         or V_MVBS_OUTPUT_RISKNL_N.TDB_AY_UWY_FLAG IS NULL )
      
   ) by ORACLE; 
   
   %rcSet(&sqlrc); 
   
   disconnect from ORACLE; 
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Join **/

/*---- Start of User Written Code  ----*/ 


/* SORTING THE DATASET */
proc sort data=RESASDWH.MVBS_TEMP_CROSS_CHECK_CORETP out=CORETP; 
by LEGAL_ENTITY_CODE
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID ;
run;


/* CORETP -- SUMMARY TABLE EXCEPT NL100 */

data CORETP_SUM (drop=i);
 set CORETP ;

 by LEGAL_ENTITY_CODE
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID ;

retain 
NL01_COUNT 
NL02_COUNT 
NL03_SUM NL03_SUM_COMPARE 
NL06_COUNT NL06_CNT_COMPARE 
NL10_SUM 
NL12_SUM 
NL13_SUM 
NL25_SUM NL25_SUM_COMPARE 
NL36_SUM9 NL36_SUM10 NL36_SUM11  
NL37_SUM26 NL37_SUM27 NL37_SUM28
NL38_SUM 
NL55_SUM NL55_SUM_COMPARE 
NL56_SUM    
NL57_SUM 
NL59_SUM NL59_SUM_COMPARE 
NL79_SUM 
NL81_SUM0 NL81_SUM1 NL81_SUM_COMPARE0 NL81_SUM_COMPARE1 
NL82_SUM
NL83_SUM
NL84_SUM
NL85_SUM 
NL86_SUM NL86_SUM_COMPARE 
NL87_SUM ;

if first.TDB_LOCAL_LOB_ID 
	then do; 
	  array a(*) NL:;
	    /* While CNT or COUNT variables have to be initialized with 0, SUM variables have to be initialized with . */
	    /* This is, because amount values can have a 0 value. Therefore . and 0 have a different meaning. */
	    do i = 1 to dim(a);
	      a(i)=.;
       end;

	    /* initialize count variables with 0 as this is equivalent with "no data available" */
	    NL01_COUNT = 0;
	    NL02_COUNT = 0;
	    NL06_COUNT = 0;
	    NL06_CNT_COMPARE = 0;
    end;
 
     if TDB_ITEM in ('8720006','8720011','8720028')  and PROP_NONPROP = 'PROP' then 
       NL01_COUNT = NL01_COUNT + 1 ; 
 
     if TDB_ITEM in ( '8720004' , '8720005' , '8720009' , '8720010' , '8720026' , '8720027')  
      and PROP_NONPROP = 'NONPROP' then 
        NL02_COUNT = NL02_COUNT + 1 ;

     if TDB_ITEM in ('8720026','8720027','8720028','872009','8720010','8720011') and TDB_ANNUITIES_FLAG = '0' then 
      NL03_SUM = sum(NL03_SUM, TDB_VALUE_IN_CON_CURR);
     if TDB_ITEM = '8720008' and TDB_ANNUITIES_FLAG = '0' then
      NL03_SUM_COMPARE = sum(NL03_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);

     if TDB_ITEM in ('8720009','8720010','8720011') and TDB_ANNUITIES_FLAG = '0'  then 
      NL06_COUNT = NL06_COUNT + 1 ;
     if TDB_ITEM in ( '8720016' , '8720017' , '8720018' , '8720019' )  and TDB_ANNUITIES_FLAG = '0' then 
      NL06_CNT_COMPARE = NL06_CNT_COMPARE + 1  ;

     if TDB_ITEM = '8720016' and TDB_ANNUITIES_FLAG = '0' then 
      NL10_SUM = sum(NL10_SUM, TDB_VALUE_IN_CON_CURR);
 
	 if TDB_ITEM = '8720018' and TDB_ANNUITIES_FLAG = '0' then 
      NL12_SUM = sum(NL12_SUM, TDB_VALUE_IN_CON_CURR);
  
    if TDB_ITEM = '8720019' and TDB_ANNUITIES_FLAG = '0' then 
      NL13_SUM = sum(NL13_SUM, TDB_VALUE_IN_CON_CURR);

    if TDB_ITEM in ( '8720009' , '8720010' , '8720011' ) and TDB_ANNUITIES_FLAG = '0' then 
      NL25_SUM = sum(NL25_SUM, TDB_VALUE_IN_CON_CURR);
    if TDB_ITEM = '8720047'  and TDB_GROSS_CEDED = 'G' and TDB_SETTLE_CURR = '0TL' then 
      NL25_SUM_COMPARE = sum(NL25_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);
 
	if TDB_ITEM = '8720009' then 
      NL36_SUM9  = sum(NL36_SUM9,  TDB_VALUE_IN_CON_CURR) ;
	if TDB_ITEM = '8720010' then 
      NL36_SUM10 = sum(NL36_SUM10, TDB_VALUE_IN_CON_CURR) ;
	if TDB_ITEM = '8720011' then 
      NL36_SUM11 = sum(NL36_SUM11, TDB_VALUE_IN_CON_CURR) ;

	if TDB_ITEM = '8720026' then 
      NL37_SUM26 = sum(NL37_SUM26, TDB_VALUE_IN_CON_CURR) ;
	if TDB_ITEM = '8720027' then 
      NL37_SUM27 = sum(NL37_SUM27, TDB_VALUE_IN_CON_CURR) ;
	if TDB_ITEM = '8720028' then 
      NL37_SUM28 = sum(NL37_SUM28, TDB_VALUE_IN_CON_CURR) ;

	if TDB_ITEM = '8720008' then
      NL38_SUM = sum(NL38_SUM, TDB_VALUE_IN_CON_CURR);

	if TDB_ITEM in ( '8720012' , '8720013' , '8720014' )  and TDB_ANNUITIES_FLAG = '0'  then 
      NL55_SUM = sum(NL55_SUM, TDB_VALUE_IN_CON_CURR) ;
	if TDB_ITEM = '8720015'  and TDB_ANNUITIES_FLAG = '0' then 
     NL55_SUM_COMPARE = sum(NL55_SUM_COMPARE, TDB_VALUE_IN_CON_CURR) ;

	if TDB_ITEM in ( '8720012' , '8720013' , '8720014' )  and TDB_ANNUITIES_FLAG = '0'  then 
      NL56_SUM = sum(NL56_SUM, TDB_VALUE_IN_CON_CURR) ;
	
	if TDB_ITEM = '8720015'  and TDB_ANNUITIES_FLAG = '0'  then 
      NL57_SUM = sum(NL57_SUM, TDB_VALUE_IN_CON_CURR) ;

	if TDB_ITEM in ( '8720012' , '8720013' , '8720014' )  and TDB_ANNUITIES_FLAG = '0'  then 
      NL59_SUM = sum(NL59_SUM, TDB_VALUE_IN_CON_CURR);
  	if TDB_ITEM = '8720047' and TDB_GROSS_CEDED = 'C' and TDB_SETTLE_CURR = '0TL' then 
      NL59_SUM_COMPARE = sum(NL59_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);

	if TDB_ITEM in ( '8720029' , '8720030' , '8720031' )  and TDB_ANNUITIES_FLAG = '0'  then 
      NL79_SUM = sum(NL79_SUM, TDB_VALUE_IN_CON_CURR);	 

	 if TDB_ITEM in (  '8720020','8720021','8720022','8720023' ) then do ;
	     if TDB_ANNUITIES_FLAG = '0' then
            NL81_SUM0 = sum(NL81_SUM0, TDB_VALUE_IN_CON_CURR);
	     else if TDB_ANNUITIES_FLAG = '1' then
	         NL81_SUM1 = sum(NL81_SUM1, TDB_VALUE_IN_CON_CURR);
	 end;

	 if TDB_ITEM in ( '8720026','8720027','8720028' ) then do ;
	     if TDB_ANNUITIES_FLAG = '0' then
	         NL81_SUM_COMPARE0 = sum(NL81_SUM_COMPARE0, TDB_VALUE_IN_CON_CURR);
	     else if TDB_ANNUITIES_FLAG = '1' then
            NL81_SUM_COMPARE1 = sum(NL81_SUM_COMPARE1, TDB_VALUE_IN_CON_CURR);
	 end;

	 if TDB_ITEM = '8720020'  and TDB_ANNUITIES_FLAG = '0'  then 
       NL82_SUM = sum(NL82_SUM, TDB_VALUE_IN_CON_CURR);

	 if TDB_ITEM = '8720021'  and TDB_ANNUITIES_FLAG = '0'  then 
       NL83_SUM = sum(NL83_SUM, TDB_VALUE_IN_CON_CURR);

	 if TDB_ITEM = '8720022'  and TDB_ANNUITIES_FLAG = '0'  then 
       NL84_SUM = sum(NL84_SUM, TDB_VALUE_IN_CON_CURR);

	 if TDB_ITEM = '8720023'  and TDB_ANNUITIES_FLAG = '0'  then 
       NL85_SUM = sum(NL85_SUM, TDB_VALUE_IN_CON_CURR);

	 if TDB_ITEM in ( '8720029','8720030','8720031' ) and TDB_ANNUITIES_FLAG = '0'  then 
       NL86_SUM = sum(NL86_SUM, TDB_VALUE_IN_CON_CURR);
	 if TDB_ITEM = '8720032' and TDB_ANNUITIES_FLAG = '0' then 
       NL86_SUM_COMPARE = sum(NL86_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);

	 if TDB_ITEM = '8720032' and TDB_ANNUITIES_FLAG = '0'  then 
       NL87_SUM = sum(NL87_SUM, TDB_VALUE_IN_CON_CURR);

  if last.TDB_LOCAL_LOB_ID then do;
    OUTPUT;
  end;

run;
/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 


/* SORTING THE DATASET */

DATA CORETPY;
	 SET RESASDWH.MVBS_TEMP_CROSS_CHECK_CORETP ;

		WHERE tdb_item in ('8720047');
	LAT=compress(LEGAL_ENTITY_CODE||'-'||ABACUS_SII_LOB||'-'||TDB_LOCAL_LOB_ID);
run;

proc sort data=CORETPY;  BY LAT; RUN;


/* CORETP -- SUMMARY TABLE FOR NL100 */

data CORETP_SUM100;
 set CORETPY ;
by lat;
		if first.lat then CORETP_COUNT=0; CORETP_COUNT+1;
			if last.lat then output;
run;

/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 


/* SORTING THE DATASET */
proc sort data = resasdwh.MVBS_TEMP_CROSS_CHECK_RISKNL out=RISKNL; 
by LEGAL_ENTITY_CODE 
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID;
run;

/* RISKNL -- SUMMARY TABLE */

data RISKNL_SUM (drop=i);
set RISKNL;

by LEGAL_ENTITY_CODE 
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID ;

retain 
NL10_SUM_COMPARE
NL12_SUM_COMPARE
NL13_SUM_COMPARE
NL56_SUM_COMPARE
NL57_SUM_COMPARE
NL79_SUM_COMPARE
NL82_SUM_COMPARE
NL83_SUM_COMPARE
NL84_SUM_COMPARE
NL85_SUM_COMPARE
NL87_SUM_COMPARE
;

if first.TDB_LOCAL_LOB_ID 
   then do; 
	   array a(*) NL:;
	    /* While CNT or COUNT variables have to be initialized with 0, SUM variables have to be initialized with . */
	    /* This is, because amount values can have a 0 value. Therefore . and 0 have a different meaning. */
	    do i = 1 to dim(a);
	      a(i)=.;
        end;
     end;
  
    if TDB_ITEM = '8720039' and TDB_ANNUITIES_FLAG = '0' then 
        NL10_SUM_COMPARE = sum(NL10_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);
 
    if TDB_ITEM = '8720041' and TDB_ANNUITIES_FLAG = '0' then 
       NL12_SUM_COMPARE = sum(NL12_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);

    if TDB_ITEM = '8720042' and TDB_ANNUITIES_FLAG = '0' then 
       NL13_SUM_COMPARE = sum(NL13_SUM_COMPARE, TDB_VALUE_IN_CON_CURR); 

    if TDB_ITEM IN('8720035', '8720203') and TDB_ANNUITIES_FLAG = '0' then  
       NL56_SUM_COMPARE = sum(NL56_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);  
 
    if TDB_ITEM ='8720036' and TDB_ANNUITIES_FLAG = '0' then  
       NL57_SUM_COMPARE = sum(NL57_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);    

    if TDB_ITEM IN('8720037', '8720204') and TDB_ANNUITIES_FLAG = '0' then 
       NL79_SUM_COMPARE = sum(NL79_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);  

    if TDB_ITEM ='8720043' and TDB_ANNUITIES_FLAG = '0'  then  
       NL82_SUM_COMPARE = sum(NL82_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);  

    if TDB_ITEM ='8720044' and TDB_ANNUITIES_FLAG = '0' then 
       NL83_SUM_COMPARE = sum(NL83_SUM_COMPARE, TDB_VALUE_IN_CON_CURR); 

    if TDB_ITEM ='8720045' and TDB_ANNUITIES_FLAG = '0' then 
       NL84_SUM_COMPARE = sum(NL84_SUM_COMPARE, TDB_VALUE_IN_CON_CURR); 

    if TDB_ITEM ='8720046' and TDB_ANNUITIES_FLAG = '0' then 
       NL85_SUM_COMPARE = sum(NL85_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);  

    if TDB_ITEM ='8720038' and TDB_ANNUITIES_FLAG = '0' then 
       NL87_SUM_COMPARE = sum(NL87_SUM_COMPARE, TDB_VALUE_IN_CON_CURR);  

    if last.TDB_LOCAL_LOB_ID then do;
      OUTPUT;
    end;
run;

/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 


/* SORTING THE DATASET */

DATA NCORETPY;
	SET RESASDWH.MVBS_TEMP_CROSS_CHECK_NCORETP;
		WHERE tdb_item in ('8720049','8720050');
LAT=compress(LEGAL_ENTITY_CODE||'-'||ABACUS_SII_LOB||'-'||TDB_LOCAL_LOB_ID);
run;

proc sort data=NCORETPY; by  LEGAL_ENTITY_CODE ABACUS_SII_LOB TDB_LOCAL_LOB_ID ; run;

/* NCORETP -- SUMMARY TABLE FOR NL100 */

data NCORETP_SUM100 (drop=i);
 set NCORETPY ;

by LEGAL_ENTITY_CODE
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID ;

retain 
NL100_CNT_COMPARE_A49  
NL100_CNT_COMPARE_A50 
NL100_CNT_COMPARE_U49  
NL100_CNT_COMPARE_U50 ;

if first.TDB_LOCAL_LOB_ID 
   then do; 
	  array a(*) NL:;
	    do i = 1 to dim(a);
	     a(i)=0;
        end;
    end;

   if TDB_ITEM = '8720049' then do ;
        if TDB_AY_UWY_FLAG = 'A' then do ; NL100_CNT_COMPARE_A49 = NL100_CNT_COMPARE_A49 + 1 ; end;
        if TDB_AY_UWY_FLAG = 'U' then do ; NL100_CNT_COMPARE_U49 = NL100_CNT_COMPARE_U49 + 1 ; end;
    end;

	if TDB_ITEM = '8720050' then do ;
        if TDB_AY_UWY_FLAG = 'A' then do ; NL100_CNT_COMPARE_A50 = NL100_CNT_COMPARE_A50 + 1 ; end;
        if TDB_AY_UWY_FLAG = 'U' then do ; NL100_CNT_COMPARE_U50 = NL100_CNT_COMPARE_U50 + 1 ; end;
    end;

   if last.TDB_LOCAL_LOB_ID then do;
    if TDB_AY_UWY_YEAR ne '' then OUTPUT;
   end;

run;


data NCORETP_SUM100;
	set NCORETP_SUM100;
	MAX_NONCORETP=max(NL100_CNT_COMPARE_A49,NL100_CNT_COMPARE_U49,NL100_CNT_COMPARE_A50,NL100_CNT_COMPARE_U50);;
run;

proc sort data=NCORETP_SUM100;	by lat MAX_NONCORETP; run;


data NCORETP_SUM100;
	set NCORETP_SUM100;
		by LAT MAX_NONCORETP;
		if last.LAT then output;
	run;
/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 

proc datasets lib=resasdwh nolist ;
 delete MVBS_CROSS_CHECK_NL_SUM2;
quit;

/***********1*************************** MERGED SUMMARY TABLE *************************************/

data RESASDWH.MVBS_CROSS_CHECK_NL_SUM2;
   merge CORETP_SUM100 (KEEP= LEGAL_ENTITY_CODE ABACUS_SII_LOB TDB_LOCAL_LOB_ID LAT CORETP_COUNT)
		NCORETP_SUM100 (KEEP= LEGAL_ENTITY_CODE ABACUS_SII_LOB TDB_LOCAL_LOB_ID LAT MAX_NONCORETP);
by  LAT;
run;



/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 


proc datasets lib=resasdwh nolist ;
 delete MVBS_TECHDB_CHECK_NL2;
quit;

/******************************************** VALIDATION CHECK *********************************/



data RESASDWH.MVBS_TECHDB_CHECK_NL2 
(KEEP= LEGAL_ENTITY_CODE LOB_ID ADDITIONAL_INFO VALUE COMPARE_VALUE 
RULE_CLASS RULE_TYPE RULE_NAME LOAD_TIMESTAMP); 
retain 
   LEGAL_ENTITY_CODE 
   LOB_ID 
   ADDITIONAL_INFO 
   VALUE 
   COMPARE_VALUE 
   RULE_CLASS 
   RULE_TYPE 
   RULE_NAME 
   LOAD_TIMESTAMP;

length  RULE_CLASS $15
        RULE_NAME  $20
        RULE_TYPE  $10
        LEGAL_ENTITY_CODE $6
        LOB_ID    $5
        ADDITIONAL_INFO   $100
        VALUE             8
        COMPARE_VALUE     8
        LOAD_TIMESTAMP    8
           ;
    format LOAD_TIMESTAMP datetime20.;

 set RESASDWH.MVBS_CROSS_CHECK_NL_SUM2 ;

 if _N_ = 1 then do ;
	  LOAD_TIMESTAMP = datetime() ;
	  RULE_CLASS = 'Validation';
  end ;

 
/* RULE FOR NL100  */
 if (( CORETP_COUNT ~= MAX_NONCORETP )  
OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (CORETP_COUNT = . and MAX_NONCORETP ~= .) or 
             (CORETP_COUNT ~= . and MAX_NONCORETP = .)
        ))
        AND
        not (CORETP_COUNT = . and MAX_NONCORETP = .)

then do ;
       LOB_ID = TDB_LOCAL_LOB_ID;
       ADDITIONAL_INFO = 'AY/UWY-Flag= A';
	   VALUE = CORETP_COUNT;
	   COMPARE_VALUE = MAX_NONCORETP;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 100';
	   output;
	   end;

run;
/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 

proc datasets lib=resasdwh nolist ;
 delete MVBS_CROSS_CHECK_NL_SUM1;
quit;

/***********1*************************** MERGED SUMMARY TABLE *************************************/

data RESASDWH.MVBS_CROSS_CHECK_NL_SUM1;
 merge CORETP_SUM RISKNL_SUM ;
by LEGAL_ENTITY_CODE 
ABACUS_SII_LOB 
TDB_LOCAL_LOB_ID ;
run;



/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 


proc datasets lib=resasdwh nolist ;
 delete MVBS_TECHDB_CHECK_NL1;
quit;

/******************************************** VALIDATION CHECK *********************************/

data RESASDWH.MVBS_TECHDB_CHECK_NL1 
(KEEP= LEGAL_ENTITY_CODE LOB_ID ADDITIONAL_INFO VALUE COMPARE_VALUE 
RULE_CLASS RULE_TYPE RULE_NAME LOAD_TIMESTAMP  ); 
retain 
   LEGAL_ENTITY_CODE 
   LOB_ID 
   ADDITIONAL_INFO 
   VALUE 
   COMPARE_VALUE 
   RULE_CLASS 
   RULE_TYPE 
   RULE_NAME 
   LOAD_TIMESTAMP;

length  RULE_CLASS $15
        RULE_NAME  $20
        RULE_TYPE  $10
        LEGAL_ENTITY_CODE $6
        LOB_ID    $5
        ADDITIONAL_INFO   $100
        VALUE             8
        COMPARE_VALUE     8
        LOAD_TIMESTAMP    8
           ;
    format LOAD_TIMESTAMP datetime20.;

 set RESASDWH.MVBS_CROSS_CHECK_NL_SUM1 ;


 if _N_ = 1 then do ;
	  LOAD_TIMESTAMP = datetime() ;
	  RULE_CLASS = 'Validation';
  end ;

  
/* RULE FOR NL01 */	
 if NL01_COUNT > 0 then do;
   LOB_ID = TDB_LOCAL_LOB_ID;
   ADDITIONAL_INFO = '';
   VALUE = NL01_COUNT;
   COMPARE_VALUE=.;
   RULE_TYPE='Defect';
   RULE_NAME='Non-Life 01';
   output;
  end;
 
/* RULE FOR NL02 */
 if NL02_COUNT > 0  then do;
     LOB_ID = TDB_LOCAL_LOB_ID;
     ADDITIONAL_INFO = '';
	  VALUE = NL02_COUNT;
	  COMPARE_VALUE=.;
	  RULE_TYPE='Defect';
	  RULE_NAME='Non-Life 02';
	  output;
   end;

/* RULE FOR NL03 */
 if ((NL03_SUM > 0 and NL03_SUM_COMPARE <= 0)                   
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL03_SUM = . and NL03_SUM_COMPARE ~= .) or 
             (NL03_SUM ~= . and NL03_SUM_COMPARE = .)
        ))
        AND
        not (NL03_SUM = . and NL03_SUM_COMPARE = .)
    

then do ;
		LOB_ID = ABACUS_SII_LOB;
 		ADDITIONAL_INFO = 'Annuities Flag= 0';
		VALUE = NL03_SUM;
	  	COMPARE_VALUE=NL03_SUM_COMPARE;
	  	RULE_TYPE='Defect';
	  	RULE_NAME='Non-Life 03';
	  output;
  end;


/* RULE FOR NL06 */
 if ((NL06_COUNT > 0 and NL06_CNT_COMPARE = 0)                  
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL06_COUNT = . and NL06_CNT_COMPARE ~= .) or 
             (NL06_COUNT ~= . and NL06_CNT_COMPARE = .)
        ))
        AND
        not (NL06_COUNT = . and NL06_CNT_COMPARE = .)
    

then do ;
		LOB_ID = TDB_LOCAL_LOB_ID;
     	ADDITIONAL_INFO = 'Annuities Flag= 0';
	   	VALUE = NL06_COUNT;
	   	COMPARE_VALUE=NL06_CNT_COMPARE ;
	   	RULE_TYPE='Defect';
	   	RULE_NAME='Non-Life 06';
	 output;
  end;


/* RULE FOR NL10 */
 if ((NL10_SUM > abs(NL10_SUM_COMPARE) )
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL10_SUM = . and NL10_SUM_COMPARE ~= .) or 
             (NL10_SUM ~= . and NL10_SUM_COMPARE = .)
        ))
        AND
        not (NL10_SUM = . and NL10_SUM_COMPARE = .)
        

then do ;
	       LOB_ID = ABACUS_SII_LOB;
	       ADDITIONAL_INFO = 'Annuities Flag= 0';
		   VALUE = NL10_SUM;
		   COMPARE_VALUE=NL10_SUM_COMPARE;
		   RULE_TYPE='Warning';
		   RULE_NAME='Non-Life 10';
		 output;
  end;


/* RULE FOR NL12 */
 if (( abs(NL12_SUM) > NL12_SUM_COMPARE)                
	OR
     /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL12_SUM = . and NL12_SUM_COMPARE ~= .) or 
             (NL12_SUM ~= . and NL12_SUM_COMPARE = .)
        ))
        AND
        not (NL12_SUM = . and NL12_SUM_COMPARE = .)
    

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL12_SUM;
	   COMPARE_VALUE=NL12_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 12';
 output;
 end;

/* RULE FOR NL13 */
 if (( abs(NL13_SUM) > NL13_SUM_COMPARE)              
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL13_SUM = . and NL13_SUM_COMPARE ~= .) or 
             (NL13_SUM ~= . and NL13_SUM_COMPARE = .)
        ))
        AND
        not (NL13_SUM = . and NL13_SUM_COMPARE = .)
        

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL13_SUM;
	   COMPARE_VALUE=NL13_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 13';
 output;
 end;

/* RULE FOR NL25 */ 
 if ((NL25_SUM ~= NL25_SUM_COMPARE )             
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL25_SUM = . and NL25_SUM_COMPARE ~= .) or 
             (NL25_SUM ~= . and NL25_SUM_COMPARE = .)
        ))
        AND
        not (NL25_SUM = . and NL25_SUM_COMPARE = .)
       

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0 and Gross/Ceded = G and Settlement Currency = 0TL';
	   VALUE = NL25_SUM;
	   COMPARE_VALUE=NL25_SUM_COMPARE;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 25';
	output;
  end;

/* RULE FOR NL36 */
 if ( NL36_SUM9 < 0 ) AND NOT (NL36_SUM9=.) then do ;
       ADDITIONAL_INFO ='Item= 8720009';
	   VALUE = NL36_SUM9;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 36';
     output;
 end;

 if ( NL36_SUM10 < 0 ) AND NOT (NL36_SUM10=.) then do ;
       ADDITIONAL_INFO ='Item= 8720010';
	   VALUE = NL36_SUM10;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 36';
    output;
 end;

 if ( NL36_SUM11 < 0 ) AND NOT (NL36_SUM11=.) then do ;
       ADDITIONAL_INFO ='Item= 8720011';
	   VALUE = NL36_SUM11;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 36';
     output;
 end;

/* RULE FOR NL37 */ 
 if ( NL37_SUM26 < 0 ) AND NOT (NL37_SUM26=.) then do ;
       ADDITIONAL_INFO ='Item= 8720026';
	   VALUE = NL37_SUM26;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 37';
    output;
  end;

  if ( NL37_SUM27 < 0 ) AND NOT (NL37_SUM27=.)  then do ;
       ADDITIONAL_INFO ='Item= 8720027';
	   VALUE = NL37_SUM27;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 37';
      output;
  end;

  if ( NL37_SUM28 < 0 )  AND NOT (NL37_SUM28=.)  then do ;
       ADDITIONAL_INFO ='Item= 8720028';
	   VALUE = NL37_SUM28;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 37';
      output;
  end;
  
/* RULE FOR NL38 */ 
 if ( NL38_SUM < 0 ) AND NOT (NL38_SUM=.) then do ;
       ADDITIONAL_INFO ='Item= 8720008';
	   VALUE = NL38_SUM;
	   COMPARE_VALUE=.;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 38';
     output;
 end;

/* RULE FOR NL55 */ 
 if (( NL55_SUM <= abs(NL55_SUM_COMPARE) )           
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL55_SUM = . and NL55_SUM_COMPARE ~= .) or 
             (NL55_SUM ~= . and NL55_SUM_COMPARE = .)
        ))
        AND
        not (NL55_SUM = . and NL55_SUM_COMPARE = .)
       

then do ;   
       LOB_ID = TDB_LOCAL_LOB_ID;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL55_SUM;
	   COMPARE_VALUE = NL55_SUM_COMPARE;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 55';
	   output;
 end;

/* RULE FOR NL56 */ 
 if (( NL56_SUM >  abs(NL56_SUM_COMPARE) )          
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL56_SUM = . and NL56_SUM_COMPARE ~= .) or 
             (NL56_SUM ~= . and NL56_SUM_COMPARE = .)
        ))
        AND
        not (NL56_SUM = . and NL56_SUM_COMPARE = .)
    

then do ;
	   LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL56_SUM;
	   COMPARE_VALUE = NL56_SUM_COMPARE;
	   RULE_TYPE='Warning';
 output;
 end;

/* RULE FOR NL57 */ 
 if (( abs(NL57_SUM) >  abs(NL57_SUM_COMPARE))         
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL57_SUM = . and NL57_SUM_COMPARE ~= .) or 
             (NL57_SUM ~= . and NL57_SUM_COMPARE = .)
        ))
        AND
        not (NL57_SUM = . and NL57_SUM_COMPARE = .)
        

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL57_SUM;
	   COMPARE_VALUE = NL57_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 57';
 output;
 end;

/* RULE FOR NL59 */
 if (( NL59_SUM ~=  NL59_SUM_COMPARE )        
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL59_SUM = . and NL59_SUM_COMPARE ~= .) or 
             (NL59_SUM ~= . and NL59_SUM_COMPARE = .)
        ))
        AND
        not (NL59_SUM = . and NL59_SUM_COMPARE = .)
        

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0 and Gross/Ceded = C and Settlement Currency = 0TL';
	   VALUE = NL59_SUM;
	   COMPARE_VALUE = NL59_SUM_COMPARE;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 59';
  output;
  end;

/* RULE FOR NL79 */ 
 if (( NL79_SUM >  abs(NL79_SUM_COMPARE) )       
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL79_SUM = . and NL79_SUM_COMPARE ~= .) or 
             (NL79_SUM ~= . and NL79_SUM_COMPARE = .)
        ))
        AND
        not (NL79_SUM = . and NL79_SUM_COMPARE = .)
      

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL79_SUM;
	   COMPARE_VALUE = NL79_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 79';
 output;
 end;

/* RULE FOR NL81 */ 
 if (( NL81_SUM0 ~=  NL81_SUM_COMPARE0 )      
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL81_SUM0 = . and NL81_SUM_COMPARE0 ~= .) or 
             (NL81_SUM0 ~= . and NL81_SUM_COMPARE0 = .)
        ))
        AND
        not (NL81_SUM0 = . and NL81_SUM_COMPARE0 = .)
      

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL81_SUM0;
	   COMPARE_VALUE = NL81_SUM_COMPARE0;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 81';
 output;
 end;

 if (( NL81_SUM1 ~= NL81_SUM_COMPARE1 )      
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL81_SUM1 = . and NL81_SUM_COMPARE1 ~= .) or 
             (NL81_SUM1 ~= . and NL81_SUM_COMPARE1 = .)
        ))
        AND
        not (NL81_SUM1 = . and NL81_SUM_COMPARE1 = .)
  

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 1';
	   VALUE = NL81_SUM1;
	   COMPARE_VALUE = NL81_SUM_COMPARE1;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 81';
 output;
 end;

/* RULE FOR NL82 */ 
 if ( (NL82_SUM >  abs(NL82_SUM_COMPARE) )     
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL82_SUM = . and NL82_SUM_COMPARE ~= .) or 
             (NL82_SUM ~= . and NL82_SUM_COMPARE = .)
        ))
        AND
        not (NL82_SUM = . and NL82_SUM_COMPARE = .)
   

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL82_SUM;
	   COMPARE_VALUE = NL82_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 82';
 output;
 end;


/* RULE FOR NL83 */
 if (( NL83_SUM >  abs(NL83_SUM_COMPARE) )    
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL83_SUM = . and NL83_SUM_COMPARE ~= .) or 
             (NL83_SUM ~= . and NL83_SUM_COMPARE = .)
        ))
        AND
        not (NL83_SUM = . and NL83_SUM_COMPARE = .)
  

then do ;
	   LOB_ID = ABACUS_SII_LOB;
	   ADDITIONAL_INFO = 'Annuities Flag= 0'; 
	   VALUE = NL83_SUM;
	   COMPARE_VALUE = NL83_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 83';
 output;
 end;

 
/* RULE FOR NL84 */  
 if ( (abs(NL84_SUM) >  NL84_SUM_COMPARE )    
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL84_SUM = . and NL84_SUM_COMPARE ~= .) or 
             (NL84_SUM ~= . and NL84_SUM_COMPARE = .)
        ))
        AND
        not (NL84_SUM = . and NL84_SUM_COMPARE = .)
     

then do ;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0'; 
	   VALUE = NL84_SUM;
	   COMPARE_VALUE = NL84_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 84';
output;
end;

/* RULE FOR NL85 */  
 if (( abs(NL85_SUM) >  NL85_SUM_COMPARE ) 
    OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL85_SUM = . and NL85_SUM_COMPARE ~= .) or 
             (NL85_SUM ~= . and NL85_SUM_COMPARE = .)
        ))
        AND
        not (NL85_SUM = . and NL85_SUM_COMPARE = .)
 

then do ;;
       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL85_SUM;
	   COMPARE_VALUE = NL85_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 85';
	   output;
 end;


/* RULE FOR NL86 */
 if (( NL86_SUM <= abs(NL86_SUM_COMPARE) )    
	OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL86_SUM = . and NL86_SUM_COMPARE ~= .) or 
             (NL86_SUM ~= . and NL86_SUM_COMPARE = .)
        ))
        AND
        not (NL86_SUM = . and NL86_SUM_COMPARE = .)
  

then do ;

       LOB_ID = TDB_LOCAL_LOB_ID;
       ADDITIONAL_INFO = 'Annuities Flag= 0';
	   VALUE = NL86_SUM;
	   COMPARE_VALUE = NL86_SUM_COMPARE;
	   RULE_TYPE='Defect';
	   RULE_NAME='Non-Life 86';
	output;
 end;

/* RULE FOR NL87 */ 
 if (( abs(NL87_SUM) >  abs(NL87_SUM_COMPARE) ) 
    OR
      /* if both values are missing, then there is no data for applying the rule -> do not create a defect */
        (    (NL87_SUM = . and NL87_SUM_COMPARE ~= .) or 
             (NL87_SUM ~= . and NL87_SUM_COMPARE = .)
        ))
        AND
        not (NL87_SUM = . and NL87_SUM_COMPARE = .)
  

then do ;

       LOB_ID = ABACUS_SII_LOB;
       ADDITIONAL_INFO = 'Annuities Flag= 0'; 
	   VALUE = NL87_SUM;
	   COMPARE_VALUE = NL87_SUM_COMPARE;
	   RULE_TYPE='Warning';
	   RULE_NAME='Non-Life 87';
     output;
 end;
run;

/*---- End of User Written Code  ----*/ 

/*==========================================================================* 
 * Step:            Append                                A5V8BXSQ.AW0003VB * 
 * Transform:       Append                                                  * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   MVBS_TECHDB_CHECK_NL1 -               A5V8BXSQ.AE000210 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL1                         * 
 *                  MVBS_TECHDB_CHECK_NL2 -               A5V8BXSQ.AE000211 * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL2                         * 
 * Target Table:    MVBS_TECHDB_CHECK_NL -                A5V8BXSQ.AE00020Z * 
 *                   RESASDWH.MVBS_TECHDB_CHECK_NL                          * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003VB);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recnt = 0;
%macro etls_recordCheck; 
   %let etls_recCheckExist = %eval(%sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL1, DATA)) or 
         %sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL1, VIEW))); 
   
   %if (&etls_recCheckExist) %then
   %do;
      proc sql noprint;
         select count(*) into :etls_recnt from RESASDWH.MVBS_TECHDB_CHECK_NL1;
      quit;
   %end;
%mend etls_recordCheck;
%etls_recordCheck;

%let dbxrc = %eval(%sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL1, DATA)) or 
      %sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL1, VIEW))); 

/*---- Map the columns  ----*/ 
proc datasets lib = work nolist nowarn memtype = (data view);
   delete W3BARZSG;
quit;

%put %str(NOTE: Mapping columns ...);
proc sql;
   create view work.W3BARZSG as
      select
         LEGAL_ENTITY_CODE,
         LOB_ID,
         ADDITIONAL_INFO,
         VALUE,
         COMPARE_VALUE,
         RULE_CLASS,
         RULE_TYPE,
         RULE_NAME length = 50   
            format = $50.
            informat = $50.,
         LOAD_TIMESTAMP
   from RESASDWH.MVBS_TECHDB_CHECK_NL1
   ;
quit;

%let SYSLAST = work.W3BARZSG;

%let dbxrc = %eval(%sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL2, DATA)) or 
      %sysfunc(exist(RESASDWH.MVBS_TECHDB_CHECK_NL2, VIEW))); 

proc datasets lib = RESASDWH nolist nowarn memtype = (data view);
   delete MVBS_TECHDB_CHECK_NL;
quit;

data RESASDWH.MVBS_TECHDB_CHECK_NL;
   set work.W3BARZSG
       RESASDWH.MVBS_TECHDB_CHECK_NL2;
   keep LEGAL_ENTITY_CODE LOB_ID ADDITIONAL_INFO VALUE COMPARE_VALUE RULE_CLASS 
    RULE_TYPE RULE_NAME LOAD_TIMESTAMP; 
run;

%rcSet(&syserr); 



/**  Step end Append **/

%let etls_endTime = %sysfunc(datetime(),datetime.);

