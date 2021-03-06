/**************************************************************************** 
 * Job:             MVBS_CASO_ADT                         A5V8BXSQ.AR0001I5 * 
 * Description:                                                             * 
 *                                                                          * 
 * Metadata Server: bire-sas94-meta-dev.srv.allianz                         * 
 * Port:            8561                                                    * 
 * Location:        /ETL/tdbpcd/Jobs/MVBS/MVBS_OUTPUT                       * 
 *                                                                          * 
 * Server:          tdbpcd                                A5HYR2NS.AS00001O * 
 *                                                                          * 
 * Source Tables:   MVBS_INTERIM_PC_INPUT -               A5V8BXSQ.AE0001VZ * 
 *                   RESASDWH.MVBS_INTERIM_PC_INPUT                         * 
 *                  META_MVBS_SUBJECT_FILTER -            A5V8BXSQ.AE0001UN * 
 *                   RESASDWH.META_MVBS_SUBJECT_FILTER                      * 
 *                  META_MVBS_AMOUNT_TYPE -               A5V8BXSQ.AE0001TX * 
 *                   RESASDWH.META_MVBS_AMOUNT_TYPE                         * 
 *                  MVBS_OUTPUT_4ADT2 -                   A5V8BXSQ.AE0001W9 * 
 *                   RESASDWH.MVBS_OUTPUT_4ADT2                             * 
 *                  META_MVBS_SUBJECT_CONTROL -           A5V8BXSQ.AE0001UK * 
 *                   RESASDWH.META_MVBS_SUBJECT_CONTROL                     * 
 *                  MVBS_LOG - RESASDWH.MVBS_LOG          A5V8BXSQ.AE0001W3 * 
 *                                                                          * 
 * Generated on:    Monday, August 28, 2017 3:09:25 PM IST                  * 
 * Generated by:    re00580                                                 * 
 * Version:         SAS Data Integration Studio 4.902                       * 
 ****************************************************************************/ 

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A5V8BXSQ.AR0001I5);
%let etls_jobName = %nrquote(MVBS_CASO_ADT);
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
 * Step:            MVBS TDB Get Version IDs              A5V8BXSQ.AW0003Y2 * 
 * Transform:       MVBS TDB Get Version IDs                                * 
 * Description:     1. input: MVBS_LOG2. input:                             * 
 *                  META_MVBS_SUBJECT_CONTROLReturns the IDs of             * 
 *                  signed-off data + the IDs of data that only requires    * 
 *                  an OK status. If there are no IDs avaible the job's     * 
 *                   rc is set to 1009.                                     * 
 *                                                                          * 
 * Source Tables:   META_MVBS_SUBJECT_CONTROL -           A5V8BXSQ.AE0001UK * 
 *                   RESASDWH.META_MVBS_SUBJECT_CONTROL                     * 
 *                  MVBS_LOG - RESASDWH.MVBS_LOG          A5V8BXSQ.AE0001W3 * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y2);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 2; 
%let _INPUT = RESASDWH.META_MVBS_SUBJECT_CONTROL;
%let _INPUT_connect =  DBMAX_TEXT=32767 DBCLIENT_MAX_BYTES=1 ADJUST_BYTE_SEMANTIC_COLUMN_LENGTHS=YES ADJUST_NCHAR_COLUMN_LENGTHS=YES DB_LENGTH_SEMANTICS_BYTE=NO PATH="&ORA_RESASDWH_PATH" user="&ORA_RESASDWH_USER" pw="&ORA_RESASDWH_PWD"
;
%let _INPUT_engine = ORACLE;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/ETL/tdbpcd/Tables/Target/META/META_MVBS_SUBJECT_CONTROL%(Table%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT1 = RESASDWH.META_MVBS_SUBJECT_CONTROL;
%let _INPUT1_connect =  DBMAX_TEXT=32767 DBCLIENT_MAX_BYTES=1 ADJUST_BYTE_SEMANTIC_COLUMN_LENGTHS=YES ADJUST_NCHAR_COLUMN_LENGTHS=YES DB_LENGTH_SEMANTICS_BYTE=NO PATH="&ORA_RESASDWH_PATH" user="&ORA_RESASDWH_USER" pw="&ORA_RESASDWH_PWD"
;
%let _INPUT1_engine = ORACLE;
%let _INPUT1_memtype = DATA;
%let _INPUT1_options = %nrquote();
%let _INPUT1_alter = %nrquote();
%let _INPUT1_path = %nrquote(/ETL/tdbpcd/Tables/Target/META/META_MVBS_SUBJECT_CONTROL%(Table%));
%let _INPUT1_type = 1;
%let _INPUT1_label = %nrquote();

%let _INPUT2 = RESASDWH.MVBS_LOG;
%let _INPUT2_connect =  DBMAX_TEXT=32767 DBCLIENT_MAX_BYTES=1 ADJUST_BYTE_SEMANTIC_COLUMN_LENGTHS=YES ADJUST_NCHAR_COLUMN_LENGTHS=YES DB_LENGTH_SEMANTICS_BYTE=NO PATH="&ORA_RESASDWH_PATH" user="&ORA_RESASDWH_USER" pw="&ORA_RESASDWH_PWD"
;
%let _INPUT2_engine = ORACLE;
%let _INPUT2_memtype = DATA;
%let _INPUT2_options = %nrquote();
%let _INPUT2_alter = %nrquote();
%let _INPUT2_path = %nrquote(/ETL/tdbpcd/Tables/Target/MVBS/MVBS_LOG%(Table%));
%let _INPUT2_type = 1;
%let _INPUT2_label = %nrquote();

%let _OUTPUT_count = 0; 

%let macroVarName = %nrquote( list_of_versions);

%mvbsGetVersionIDs(mvbs_log=&_INPUT1., meta_mvbs_subject_control=&_INPUT2., returnValue=&macroVarName.);

%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/**  Step end MVBS TDB Get Version IDs **/

/*==========================================================================* 
 * Step:            Return Code Check                     A5V8BXSQ.AW0003Y3 * 
 * Transform:       Return Code Check                                       * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y3);
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%macro etls_rcCheck; 

   %if (&trans_rc ge 5) %then
   %do; 
      /* Abort with a return code of  &trans_rc  */ 
      %abort return  &trans_rc; 
   %end; 
   
%mend etls_rcCheck; 
%etls_rcCheck; 



/**  Step end Return Code Check **/

/*==========================================================================* 
 * Step:            Create Table                          A5V8BXSQ.AW0003Y4 * 
 * Transform:       Create Table                                            * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    META_MVBS_AMOUNT_TYPE -               A5V8BXSQ.AE0001TX * 
 *                   RESASDWH.META_MVBS_AMOUNT_TYPE                         * 
 * Target Table:    Create Table - work.adt_amount_types  A5V8BXSQ.AQ0001NA * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y4);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(RESASDWH.META_MVBS_AMOUNT_TYPE); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

proc datasets lib = work nolist nowarn memtype = (data view);
   delete adt_amount_types;
quit;

proc sql
;
create view work.adt_amount_types as
select distinct
   META_MVBS_AMOUNT_TYPE.AMT_TYPE length = 20,
   META_MVBS_AMOUNT_TYPE.ENTRY_CODE length = 4
from
   RESASDWH.META_MVBS_AMOUNT_TYPE as META_MVBS_AMOUNT_TYPE
where
   META_MVBS_AMOUNT_TYPE.ENTRY_CODE IS NOT NULL 
;
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Create Table **/

/*==========================================================================* 
 * Step:            Create Table                          A5V8BXSQ.AW0003Y5 * 
 * Transform:       Create Table                                            * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   MVBS_INTERIM_PC_INPUT -               A5V8BXSQ.AE0001VZ * 
 *                   RESASDWH.MVBS_INTERIM_PC_INPUT                         * 
 *                  Create Table - work.adt_amount_types  A5V8BXSQ.AQ0001NA * 
 *                  META_MVBS_SUBJECT_FILTER -            A5V8BXSQ.AE0001UN * 
 *                   RESASDWH.META_MVBS_SUBJECT_FILTER                      * 
 * Target Table:    Create Table - work.data_delivery     A5V8BXSQ.AQ0001NB * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y5);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

proc datasets lib = work nolist nowarn memtype = (data view);
   delete data_delivery;
quit;

proc sql
;
create table work.data_delivery as
select
   MVBS_INTERIM_PC_INPUT.MANDANT_ID length = 20   
      format = $20.
      informat = $20.,
   MVBS_INTERIM_PC_INPUT.AMT_TYPE length = 100   
      format = $100.
      informat = $100.
      label = 'AMT_TYPE',
   coalescec(adt_amount_types.ENTRY_CODE, 'UNDEFINED') as ENTRY_CODE length = 10,
   sum(
      case
         when upcase(MVBS_INTERIM_PC_INPUT.GROSS_RETRO) = 'GROSS' then MVBS_INTERIM_PC_INPUT.AMOUNT
         else 0
      end
   ) as AMOUNT_GROSS length = 8   
      format = 16.2
      informat = 16.2,
   sum( 
      case 
         when upcase(MVBS_INTERIM_PC_INPUT.GROSS_RETRO) = 'NET' then MVBS_INTERIM_PC_INPUT.AMOUNT 
         else 0 
      end 
   ) as AMOUNT_NET length = 8   
      format = 16.2
      informat = 16.2
from
   RESASDWH.MVBS_INTERIM_PC_INPUT as MVBS_INTERIM_PC_INPUT left join 
   work.adt_amount_types as adt_amount_types
      on
      (
         MVBS_INTERIM_PC_INPUT.AMT_TYPE = adt_amount_types.AMT_TYPE
      )
where
   MVBS_INTERIM_PC_INPUT.VERSION_ID IN (&list_of_versions.)
   and MVBS_INTERIM_PC_INPUT.QRT_RELEVANT = 'Y'
   and upcase(trim(MVBS_INTERIM_PC_INPUT.DISC_UNDISC)) = 'DISCOUNTED'
   and upcase(trim(MVBS_INTERIM_PC_INPUT.PERIOD)) = 'TOTAL'
   and upcase(trim( MVBS_INTERIM_PC_INPUT.INSTRUMENT)) = 'VA'
   and MVBS_INTERIM_PC_INPUT.CONS_UNIT IN (
      select distinct
         META_MVBS_SUBJECT_FILTER.CHAR_VALUE length = 100   
            label = 'CHAR_VALUE'
      from
         RESASDWH.META_MVBS_SUBJECT_FILTER as META_MVBS_SUBJECT_FILTER
      where
         META_MVBS_SUBJECT_FILTER.SUBJECT_GROUP = "%scan(&sub.,1,%str(_))"
         and META_MVBS_SUBJECT_FILTER.COLUMN_NAME = 'LEGAL_ENTITY_CODE'
   )
group by
   MVBS_INTERIM_PC_INPUT.MANDANT_ID,
   MVBS_INTERIM_PC_INPUT.AMT_TYPE,
   3
;
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Create Table **/

/*==========================================================================* 
 * Step:            Create Table                          A5V8BXSQ.AW0003Y6 * 
 * Transform:       Create Table                                            * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Tables:   MVBS_OUTPUT_4ADT2 -                   A5V8BXSQ.AE0001W9 * 
 *                   RESASDWH.MVBS_OUTPUT_4ADT2                             * 
 *                  Create Table - work.data_delivery     A5V8BXSQ.AQ0001NB * 
 * Target Table:    Create Table - work.csv_output        A5V8BXSQ.AQ0001NC * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y6);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%global etls_sql_pushDown;
%let etls_sql_pushDown = -1;
option DBIDIRECTEXEC;

proc datasets lib = work nolist nowarn memtype = (data view);
   delete csv_output;
quit;

proc sql
;
create view work.csv_output as
select
   coalescec(ADT_OUTPUT.BASE_COMPANY_ID, data_delivery.MANDANT_ID) as BASE_COMPANY_ID length = 20   
      format = $20.
      informat = $20.,
   coalescec(upcase(ADT_OUTPUT.GROSSRETRO), 'GROSS') as GROSSRETRO length = 44   
      format = $44.
      informat = $44.,
   coalescec(ADT_OUTPUT.AMT_TYPE, data_delivery.AMT_TYPE) as AMT_TYPE length = 100   
      format = $100.
      informat = $100.,
   coalescec(ADT_OUTPUT.EC_CODE, data_delivery.ENTRY_CODE) as EC_CODE length = 10   
      format = $10.
      informat = $10.,
   case
      when upcase(ADT_OUTPUT.GROSSRETRO) = 'GROSS' then data_delivery.AMOUNT_GROSS
      when upcase(ADT_OUTPUT.GROSSRETRO) = 'RETRO' then sum(data_delivery.AMOUNT_NET, -data_delivery.AMOUNT_GROSS)
      else data_delivery.AMOUNT_GROSS
   end as AMOUNT_INPUT length = 8   
      format = 16.2
      informat = 16.2,
   ADT_OUTPUT.AMOUNT as AMOUNT_ADT length = 8   
      format = 16.2
      informat = 16.2
from
   (
      select
         MVBS_OUTPUT_4ADT2.BASE_COMPANY_ID length = 20   
            format = $20.
            informat = $20.
            label = 'BASE_COMPANY_ID',
         MVBS_OUTPUT_4ADT2.GROSSRETRO length = 44   
            format = $44.
            informat = $44.
            label = 'GROSSRETRO',
         MVBS_OUTPUT_4ADT2.AMT_TYPE length = 100   
            format = $100.
            informat = $100.
            label = 'AMT_TYPE',
         MVBS_OUTPUT_4ADT2.EC_CODE length = 4   
            format = $4.
            informat = $4.
            label = 'EC_CODE',
         sum(MVBS_OUTPUT_4ADT2.AMOUNT) as AMOUNT length = 8   
            label = 'AMOUNT'
      from
         RESASDWH.MVBS_OUTPUT_4ADT2 as MVBS_OUTPUT_4ADT2
      group by
         MVBS_OUTPUT_4ADT2.BASE_COMPANY_ID,
         MVBS_OUTPUT_4ADT2.GROSSRETRO,
         MVBS_OUTPUT_4ADT2.AMT_TYPE,
         MVBS_OUTPUT_4ADT2.EC_CODE
   ) as ADT_OUTPUT full join 
   work.data_delivery as data_delivery
      on
      (
         ADT_OUTPUT.BASE_COMPANY_ID = data_delivery.MANDANT_ID
         and ADT_OUTPUT.AMT_TYPE = data_delivery.AMT_TYPE
         and ADT_OUTPUT.EC_CODE = data_delivery.ENTRY_CODE
      )
order by
   BASE_COMPANY_ID,
   GROSSRETRO,
   AMT_TYPE,
   EC_CODE
;
quit;

%global etls_sql_pushDown;
%let etls_sql_pushDown = &sys_sql_ip_all;

%rcSet(&sqlrc); 



/**  Step end Create Table **/

/*==========================================================================* 
 * Step:            MVBS TDB BULK_CSV_WRITE               A5V8BXSQ.AW0003Y7 * 
 * Transform:       MVBS TDB BULK_CSV_WRITE                                 * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    Create Table - work.csv_output        A5V8BXSQ.AQ0001NC * 
 * Target Table:    MVBS TDB                              A5V8BXSQ.AQ0001ND * 
 *                  BULK_CSV_WRITE_OUTPUT_BULK_CSV -                        * 
 *                   work.WII4K1W                                           * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y7);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(work.csv_output); 

%let _INPUT_count = 1; 
%let _INPUT = work.csv_output;
%let _INPUT_connect = ;
%let _INPUT_engine = ;
%let _INPUT_memtype = VIEW;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/Create Table_A5V8BXSQ.AQ0001NC%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT0 = work.csv_output;
%let _INPUT0_connect = ;
%let _INPUT0_engine = ;
%let _INPUT0_memtype = VIEW;
%let _INPUT0_options = %nrquote();
%let _INPUT0_alter = %nrquote();
%let _INPUT0_path = %nrquote(/Create Table_A5V8BXSQ.AQ0001NC%(WorkTable%));
%let _INPUT0_type = 1;
%let _INPUT0_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = work.WII4K1W;
%let _OUTPUT_connect = ;
%let _OUTPUT_engine = ;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
%let _OUTPUT_path = %nrquote(/MVBS TDB BULK_CSV_WRITE_OUTPUT_BULK_CSV_A5V8BXSQ.AQ0001ND%(WorkTable%));
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = FILENAME;

%let _OUTPUT_BULK_CSV = work.WII4K1W;
%let _OUTPUT_BULK_CSV_connect = ;
%let _OUTPUT_BULK_CSV_engine = ;
%let _OUTPUT_BULK_CSV_memtype = DATA;
%let _OUTPUT_BULK_CSV_options = %nrquote();
%let _OUTPUT_BULK_CSV_alter = %nrquote();
%let _OUTPUT_BULK_CSV_path = %nrquote(/MVBS TDB BULK_CSV_WRITE_OUTPUT_BULK_CSV_A5V8BXSQ.AQ0001ND%(WorkTable%));
%let _OUTPUT_BULK_CSV_type = 1;
%let _OUTPUT_BULK_CSV_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_BULK_CSV_keep = FILENAME;


%let filename = %nrquote(%str("mvbs_output/&sub._CASO_&reporting_period..csv"));
%let header_count = %nrquote(0);
%let header = ;
%let column = BASE_COMPANY_ID GROSSRETRO AMT_TYPE EC_CODE AMOUNT_INPUT AMOUNT_ADT;
%let column_count = 6;
%let column0 = 6;
%let column1 = BASE_COMPANY_ID;
%let column2 = GROSSRETRO;
%let column3 = AMT_TYPE;
%let column4 = EC_CODE;
%let column5 = AMOUNT_INPUT;
%let column6 = AMOUNT_ADT;
%let columnHeaders = %nrquote(YES);
%let file_open_mode = %nrquote(o);
%let by_group = ;
%let by_group_count = 0;
%let by_group0 = 0;
%let by_group = ;
%let orderbycolumn = ;
%let orderbycolumn_count = 0;
%let orderbycolumn0 = 0;
%let orderbycolumn = ;
%let path = ;
%let dlm = %nrquote(semi);
%let dlm1 = ;
%let where = ;
%let TERMSTR = %nrquote(TERMSTR=CRLF);

/* List of target columns to keep  */ 
%let _keep = FILENAME;
/* List of target columns to keep  */ 
%let keep = FILENAME;
/*****************************************************************************************************************************/
/*********** CODE FOR WRITING THE SELECTED INPUT COLUMNS TO EXTERANAL CSV FILE ***********************************************/
/*****************************************************************************************************************************/

options noquotelenmax;

/* New prompt file_open_mode introduced. Must have "o" as default value for compatibility reasons */
%global file_open_mode;
%let file_open_mode=%sysfunc(coalescec(&file_open_mode., o));

%macro define_delimiter;
%global dlm2;
%if  &dlm eq semi %then %do;
%let dlm2=%str(;);
%end;
%if  &dlm eq others %then %do;
data _null_;
call symput('dlm2',"&dlm1");
run;%end;
%if &dlm eq comma %then %do;
%let dlm2=%str(,);
%end;
%mend define_delimiter;
%define_delimiter;

data _null_;
length g $100.;
g=tranwrd("&orderbycolumn"," ",",");
call symput('orderbycol',compress(g));
run;

data _null_; 
length COlUMN_LIST $2000;  
if symget('column_count')=1 then COlUMN_LIST=trim(symget('column')); 
else do i=1 to symget('column_count'); 
COlUMN_LIST= catx("&dlm2" ,COlUMN_LIST, trim(symget(cats('column',i)))); 
end; 
if missing(COlUMN_LIST)=0 then 
call symputx('column_list',COlUMN_LIST); 
run; 

%macro dd3;
%if &orderbycolumn ne  %then %do;
proc sql;
create table order_table as select * from &_input
order by &orderbycol;
quit;
%end;
%mend dd3;
%dd3;

%macro produce_csv;
	%local firstDataRowOfGroup lastDataRowOfGroup byStatement;
	%if "&by_group." ne "" %then %do;
        %let byStatement = %str(by &by_group;);
		%let firstDataRowOfGroup = first.%scan(&by_group., %sysfunc(countw(&by_group.)));
		%let lastDataRowOfGroup  = last.%scan(&by_group., %sysfunc(countw(&by_group.)));
	%end;
	%else %do;
        %let byStatement = ;
		%let firstDataRowOfGroup = %str(_N_=1);
		%let lastDataRowOfGroup  = %str(eof=1);
	%end;

	/* check whether an output table is defined to collect all filenames of CSV files that are created in this step */
	%local outTable outKeep;

	%if %symexist(_OUTPUT_BULK_CSV) = 1 %then %do;
		%if %quote(&_OUTPUT_BULK_CSV.) eq %then %do;
			/* No output table defined, set output to _null_ for the following data step */
			%let outTable = _null_;
			%let outKeep = ;
		%end;
		%else %do;
			/* Output table defined, set local variables outTable and outKeep */
			%let outTable = &_OUTPUT_BULK_CSV.;
			%let outKeep = (keep=&_OUTPUT_BULK_CSV_keep.);
		%end;
	%end;
	%else %do;
			/*
			 * No output table defined, set output to _null_ for the following data step.
			 * Create a local dummy macro variable in order to avoid warnings/errors when executing the rest of the code
			 * (which relies on an existing macro variable).
			 */
		%local _OUTPUT_BULK_CSV;
		%let outTable = _null_;
		%let outKeep = ;
	%end;

	data &outTable.&outKeep.;
		%if "&orderbycolumn" ne "" %then %do;
			set order_table end=eof;
		%end;
		%else %do;
			set &_input end=eof;
		%end;
		where &where.;
		length column_put $2000 filename $1000 header $2000;
		retain file_count 0 file_id;

		&byStatement.

		if &firstDataRowOfGroup. then do;
			filename = strip("&proj_env./data/" || &filename.);
			rc = filename('ft' || strip(file_count), strip(filename) , 'DISK' , "&TERMSTR.");			
			file_id = fopen('ft' || strip(file_count), "&file_open_mode.", 1000, 'v');
			%do header_nr = 1 %to &header_count.;
				%let my_header = %sysfunc(trim(&&header&header_nr.));
				header = trim(&my_header.);
				rcp = fput(file_id, strip(header));
				rcw = fwrite(file_id);
			%end;
			%if %upcase(&columnHeaders.) eq YES %then %do;
				rcp = fput(file_id, "&column_list.");
				rcw = fwrite(file_id);
			%end;
			%else %if %upcase(&columnHeaders.) eq LABELS %then %do;
				header = strip(vlabelx(scan("&column.", 1)));
				do count = 2 to countw("&column.");
					col = scan("&column.", count);
					header = strip(header) || "&dlm2" || strip(vlabelx(col));
				end;
				rcp = fput(file_id, strip(header));
				rcw = fwrite(file_id);
			%end;

			%if %quote(&_OUTPUT_BULK_CSV.) ne %then %do;
				/* Add current file name to the output table containing all file names. */
				output &outTable.;
			%end;

			file_count + 1;
		end;
 
/*      if  ( ifc(index(upcase("&column"),"NOMINAL")>1, vvaluex(scan("&column.", 5)),1) ne  0 ) */
/*         then do;*/

		/* If condition - added as part of user story TDB-156. */
			column_put = strip(vvaluex(scan("&column.", 1)));
			do count = 2 to countw("&column.");
				col = scan("&column.", count);
				column_put = strip(column_put) || "&dlm2" || strip(vvaluex(col));
			end;
			rcp = fput(file_id, strip(column_put));
			rcw = fwrite(file_id);
/*     end;*/
		if &lastDataRowOfGroup. then do;
			rcc = fclose(file_id);

			%if %quote(&_OUTPUT_BULK_CSV.) eq %then %do;
				/* No output table defined -> add files directly to a zip file (default beaviour until 2015Q3 release) */
				%if "&path" eq "" %then %do;
					call execute('ods package(LongRun) add file=' ||'"' || strip("&proj_env/data/" || &filename.)  || '"' ||';');
				%end;
				%else %do;
					call execute('ods package(LongRun) add file=' ||'"' || strip("&proj_env/data/" || &filename.)  || '"' 
									|| ' path =' || '"' || &path. || '"' ||';');
				%end;
			%end;
		end;
	run;

	/* Delete output macro variable, just to be sure that no subsequent "BULK CSV write" gets in trouble */
	%symdel _OUTPUT_BULK_CSV;
%mend;
%produce_csv;

%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/**  Step end MVBS TDB BULK_CSV_WRITE **/

/*==========================================================================* 
 * Step:            MVBS TDB Create zip file              A5V8BXSQ.AW0003Y8 * 
 * Transform:       MVBS TDB Create zip file                                * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    MVBS TDB                              A5V8BXSQ.AQ0001ND * 
 *                  BULK_CSV_WRITE_OUTPUT_BULK_CSV -                        * 
 *                   work.WII4K1W                                           * 
 *==========================================================================*/ 

%let transformID = %quote(A5V8BXSQ.AW0003Y8);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let SYSLAST = %nrquote(work.WII4K1W); 

%let _INPUT_count = 1; 
%let _INPUT = work.WII4K1W;
%let _INPUT_connect = ;
%let _INPUT_engine = ;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/MVBS TDB BULK_CSV_WRITE_OUTPUT_BULK_CSV_A5V8BXSQ.AQ0001ND%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT0 = work.WII4K1W;
%let _INPUT0_connect = ;
%let _INPUT0_engine = ;
%let _INPUT0_memtype = DATA;
%let _INPUT0_options = %nrquote();
%let _INPUT0_alter = %nrquote();
%let _INPUT0_path = %nrquote(/MVBS TDB BULK_CSV_WRITE_OUTPUT_BULK_CSV_A5V8BXSQ.AQ0001ND%(WorkTable%));
%let _INPUT0_type = 1;
%let _INPUT0_label = %nrquote();

%let _OUTPUT_count = 0; 

%let zipFilePath = %nrquote(&proj_env./data/mvbs_output/);
%let zipFileName = %nrquote(%str("&sub._CASO.zip"));

%macro createZIP;
   /* Open a new zip archive */
   ods package open nopf;

   /* Add the required files to the zip archive */	
	%do inputCount=0 %to %eval(&_INPUT_count. - 1);
	   data _null_;
	      length filename path $1000;
	      set &&_INPUT&inputCount;
	
	      if missing(path) then
	         call execute('ods package add file="' || strip(filename) || '";');
	      else
	         call execute('ods package add file="' || strip(filename) || '"  path = "' || strip(path) || '";');
	   run;
	%end;
	
   /*
    * Store the zip archive to the file system.
    * No quotation marks around zipFileName because this macro variable may contain ANY statement
    * (and therefore must also take care of the quotes).
    */
   ods package publish archive properties(archive_name=&zipFileName. archive_path="&zipFilePath.");

   /* Close the zip archive and delete the ods package object */
   ods package close;
%mend;

%createZIP;

%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 



/**  Step end MVBS TDB Create zip file **/

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
         (FileName = &proj_env./run_control/mvbs_error_&etls_jobName, 
          Message = Error); 
   %end; 
%mend etls_jobRCChk; 
%etls_jobRCChk; 

