DM "clear log";
filename altlog 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/SAS files/Clean datasets/ACS 2016/altlog.log';


LIBNAME stubs 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/ACS 2016/group2';
LIBNAME sas 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/SAS files/Clean datasets/ACS 2016';

proc printto log=altlog;
run;

%macro AnyGeo(geography);
/* All ACS geographic Summary File headers have the same following layout
   See Technical documentation for more information on geographic header files
   and additional ACS Geography information                                         */
data work.&geography;

/* Location on geographic header files saved to from;                               */
  INFILE "C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/ACS 2016/group2/&geography..txt" MISSOVER TRUNCOVER LRECL=500;
LABEL	
		FILEID  ='File Identification'              STUSAB   ='State Postal Abbreviation'
		SUMLEVEL='Summary Level'                    COMPONENT='geographic Component'
		LOGRECNO='Logical Record Number'            US       ='US'
		REGION  ='Region'                           DIVISION ='Division'
		STATECE ='State (Census Code)'              STATE    ='State (FIPS Code)'
		COUNTY  ='County'                           COUSUB   ='County Subdivision (FIPS)'
		PLACE   ='Place (FIPS Code)'                TRACT    ='Census Tract'
		BLKGRP  ='Block Group'                      CONCIT   ='Consolidated City'
		CSA     ='Combined Statistical Area'        METDIV   ='Metropolitan Division'
		UA      ='Urban Area'                       UACP     ='Urban Area Central Place'
		VTD     ='Voting District'                  ZCTA3    ='ZIP Code Tabulation Area (3-digit)'
		SUBMCD  ='Subbarrio (FIPS)'                 SDELM    ='School District (Elementary)'
		SDSEC   ='School District (Secondary)'      SDUNI    ='School District (Unified)'
		UR      ='Urban/Rural'                      PCI      ='Principal City Indicator'
		TAZ     ='Traffic Analysis Zone'            UGA      ='Urban Growth Area'
		GEOID   ='geographic Identifier'            NAME     ='Area Name' 					    
		AIANHH  ='American Indian Area/Alaska Native Area/Hawaiian Home Land (Census)'
		AIANHHFP='American Indian Area/Alaska Native Area/Hawaiian Home Land (FIPS)'
		AIHHTLI ='American Indian Trust Land/Hawaiian Home Land Indicator'
		AITSCE  ='American Indian Tribal Subdivision (Census)'
		AITS    ='American Indian Tribal Subdivision (FIPS)'
		ANRC    ='Alaska Native Regional Corporation (FIPS)'
		CBSA    ='Metropolitan and Micropolitan Statistical Area'
		MACC    ='Metropolitan Area Central City'	
		MEMI    ='Metropolitan/Micropolitan Indicator Flag'
		NECTA   ='New England City and Town Combined Statistical Area'
		CNECTA  ='New England City and Town Area'
		NECTADIV='New England City and Town Area Division'
		CDCURR  ='Current Congressional District'
		SLDU    ='State Legislative District Upper'	
		SLDL    ='State Legislative District Lower'
		ZCTA5   ='ZIP Code Tabulation Area (5-digit)'
		PUMA5   ='Public Use Microdata Area - 5% File'
		PUMA1   ='Public Use Microdata Area - 1% File';

INPUT
		FILEID    $ 1-6         STUSAB    $ 7-8			SUMLEVEL  $ 9-11							
		COMPONENT $ 12-13       LOGRECNO  $ 14-20		US        $ 21-21  
		REGION    $ 22-22       DIVISION  $ 23-23		STATECE   $ 24-25							
		STATE     $ 26-27       COUNTY    $ 28-30		COUSUB    $ 31-35 
		PLACE     $ 36-40       TRACT     $ 41-46		BLKGRP    $ 47-47							
		CONCIT    $ 48-52       AIANHH    $ 53-56		AIANHHFP  $ 57-61
		AIHHTLI   $ 62-62       AITSCE    $ 63-65		AITS      $ 66-70							
		ANRC      $ 71-75       CBSA      $ 76-80		CSA       $ 81-83
		METDIV    $ 84-88       MACC      $ 89-89		MEMI      $ 90-90							
		NECTA     $ 91-95       CNECTA    $ 96-98		NECTADIV  $ 99-103	
		UA        $ 104-108     UACP      $ 109-113		CDCURR    $ 114-115						    
		SLDU      $ 116-118     SLDL      $ 119-121		VTD       $ 122-127
		ZCTA3     $ 128-130     ZCTA5     $ 131-135		SUBMCD    $ 136-140						    
		SDELM     $ 141-145     SDSEC     $ 146-150		SDUNI     $ 151-155
		UR        $ 156-156     PCI       $ 157-157		TAZ       $ 158-163							
		UGA       $ 164-168     PUMA5     $ 169-173		PUMA1     $ 174-178
		GEOID     $ 179-218     NAME      $ 219-418;
run;
%mend;

%macro TableShell(tblid);
/*The TableShell Marco is a basic SAS set statement that will get basic metadata 																			
  information about ACS Detailed Tables from the SequenceNumberTableNumberLookup 
  dataset by table id.  See the technical documentation 1.4 Tools for Obtaining 
  Data for more information                                                         */
data work.Table_&tblid;
set stubs.ACS_5yr_Seq_Lookup;
/*Convert single quotes in the metadata  (simplifies reading in the metadata)       */
   title=tranwrd(title, "'", "''");
/*Prep for removing non-data lines (these lines end with .7) becasue they do NOT 
  have a linking field in the data files.  (can be kept for readablity if desired)  */
   if index(order,".") then order = ".";
   if tblid=upcase("&tblid") then output;
run;
%mend;
%macro TablesBySeq(Seq);
/*The TablesBySeq Marco is a basic SAS set statement that will get basic metadata 																			
  information about ACS Detailed Tables from the SequenceNumberTableNumberLookup 
  dataset by sequence.  See the technical documentation 1.4 Tools for Obtaining 
  Data for more information                                                         */

data work.Seq_&seq;
set stubs.ACS_5yr_Seq_Lookup;
/*Convert single quotes in the metadata  (simplifies reading in the metadata)       */
   title=tranwrd(title, "'", "''");
/*Prep for removing non-data lines (these lines end with .7) becasue they do NOT 
  have a linking field in the data files.  (can be kept for readablity if desired)  */
   if index(order,".") then order = ".";
   if seq=upcase("&seq") then output;
run;
%mend;
%macro ReadDataFile(type,geo,seq);
/*The ReadDataFile is a macro that will generate SAS code for a specific estimate 
  type, a specific geography and by sequence number.  The macro will run the 
  code as well writing the data into the SAS work directory.                        */


/*rootdir is a directory (that must exist) to store the generated SAS code          */
%let rootdir=C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/SAS files/Code/Autogenerated ACS 2016 Code/;
/*Start to generate SAS code from the metadata file created from %TablesBySeq macro */
data _null_;
  set work.Seq_&seq;
  /*Save code to FILE statement below                                               */
    FILE "&rootdir&type&geo._&seq..sas" ;
	/*For the first observation of the metadata dataset start to write out code 
      to read in the first 6 fields of data these variables are consistent for
      every summary file sequence see technical documentation for detailes          */
	if _n_ =1 then do;
   		put "TITLE ""&type.20165&geo.&seq.000"";";
		put "DATA work.SF&type.&seq.&geo;";
		put " ";
		put "LENGTH FILEID   $6";
		put "       FILETYPE $6";
		put "       STUSAB   $2";
		put "       CHARITER $3";
		put "       SEQUENCE $4";
		put "       LOGRECNO $7;";
		put " ";
		put "INFILE 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/ACS 2016/group2/&type.20165&geo.&seq.000.txt' DSD TRUNCOVER DELIMITER =',' LRECL=3000;";
		put " ";
		put "LABEL  FILEID  ='File Identification'";
		put "       FILETYPE='File Type'  ";
		put "       STUSAB  ='State/U.S.-Abbreviation (USPS)'";
		put " 	    CHARITER='Character Iteration'";
		put " 	    SEQUENCE='Sequence Number'";
		put " 	    LOGRECNO='Logical Record Number'";
		put " ";
	 end;
																
	if position ^=. then put " ";
    /*If the order is blank then the title is a non-data line, Table Title, Table
      Universe or non-data line; these lines are written out but commented out      */
	if order =.     then put "/*" title "*/";
    /*If we are at the first line of the table put in a space for readability       */
	if order =1     then put " ";
    /*If the order is not blank then write out SAS code for LABEL                   */
	if order ^=. then do;
        lineout= compress(tblid)||"&type"||compress(order)||"='"||trim(title)||"'";
        put lineout;
	end;
run;

/*Now write out the "INPUT" section of the SAS code to read in the data             */
data _null_;
  set work.Seq_&seq;
   FILE "&rootdir&type&geo._&seq..sas" MOD;
    /*Again first 6 fields are constants like the LABEL section                     */
   	if _n_ =1 then do;
		put ";";            put " ";        put " ";
		put "INPUT";        put " ";		
		put "FILEID   $ ";
		put "FILETYPE $ ";         
		put "STUSAB   $ ";   
		put "CHARITER $ "; 
		put "SEQUENCE $ "; 
		put "LOGRECNO $ "; 
	end;
	if order =1 then put " ";
    /*INPUT the table data                                                          */
	if order ^=. then do;
		lineout= compress(tblid)||"&type"||compress(order);
	    put	lineout;
	end;
run;

data _null_;
   /*Finish up the program                                                          */
   FILE "&rootdir&type&geo._&seq..sas" MOD;
   		put ";"; put "RUN;";
run;

/*Run the generated code                                                            */
%include "&rootdir&type&geo._&seq..sas";		
%mend;

%macro AllTableShells;
/*NOTE:  This is just an example of getting multiple table shells                   */
/*The AllTableShells macro will divide up the SequenceNumberTableNumberLookup
  dataset into separate metadata files by Table ID for more information on 
  the SequenceNumberTableNumberLookup dataset please see technical documentation    */											


/*Create a dataset with distinct table ids                                          */
proc sql;
	create table work.tblids as select distinct(tblid) from 
	stubs.ACS_5yr_Seq_Lookup;
quit;

/*Call the TableShells macro with each distinct Table ID                            */
data _null_;
  set work.tblids;
	call execute('%TableShells(' || compress(tblid) || ')');
run;

%mend;

%macro AllSeqs(geo);
/*The AllSeqs Macro serves as a control to read in geography and data from the 
  summary file                                                                      */


/*Step 1 get the geography information based on the two digit state abbreviation 
  from the Macro %CallSt.                                                           */
%AnyGeo (g20165&geo);

/*Step 2 decide what sequences you want from the summary file, the do loop will
  create sas code for each sequence it is valide for. Values are 1 to 165 if you 
  only want a table in sequence 73 then set the do loop to be %do x=73 %to 73;   
  and that will be the only sequence you read in to SAS                             */
%do x=1 %to 122;
    %let var=000&x;
	%let seq = %substr(&var,%length(&var)-3,4);
    /*Note:  The Sequence number IS 0 filled                                        */
    /*Get the metadata for the sequence number created by the do loop               */
	%TablesBySeq(&seq);
	/*Step 3 generate SAS code to read in the estimates and margin of error and for
      the geography passed into the AllSeqs macro (Step 1) and the sequence 
      number created in the do loop above (Step 2)                                  */
	%ReadDataFile(e,&geo,&seq);
	%ReadDataFile(m,&geo,&seq);

    /*Step 4 Merge the Geography file created from the %AnyGeo macro and the 2 types 
      of estimates from the %ReadDataFile macro into a single file by geography     */

/*************************************FINAL OUTPUT***********************************/
   data sas.SF&seq&geo;
      merge  g20165&geo(IN=g) SFe&seq&geo(IN=x);
/*SFm&seq&geo(IN=y);*/
      by logrecno;
	  if not cmiss(BLKGRP);
   run;
/*************************************FINAL OUTPUT***********************************/
%end;

%mend;
%macro CallSt;
/*The CallSt macro is used to generate 2 digit state abbreviations codes from 
 the 2-digit FIPS numeric code.  This is used to automate reading in multiple 
 geography files at a time.                                                         */

/*Note:  FIPS codes are NOT sequential so if a code does not exist such as 71  
  The call execute statement will NOT run because there is no state abbreviation    */

/*Note:  If you want just a single state, such as Alabama set the 'do' statement 
 to start and end at that state code.  For example Texas:  %do i=48 %to 48;         */

%do i=0 %to 72;
	data _null_;
  	  stabbrv=compress(trim(lowcase(FIPSTATE(&i))));
      /*Note:  US, DC and PR are not covered in FIPS state function
       *Note:  The FIPSTATE function is not required to be 0 filled                 */
	    if &i=0  then stabbrv = 'us';
  		if &i=11 then stabbrv = 'dc';
  		if &i=72 then stabbrv = 'pr';
        /*FIPS Codes 60 and 66 are fpr American Samoa and Guam 
          which are not valid ACS geographies                                       */
  		if &i>56 and &i<72 then stabbrv = "--";
        /*If the function returns a state abbreviation then run the AllSeqs macro   */
  		if stabbrv ^= "--" then do;
  			call execute('%AllSeqs(' || compress(stabbrv) || ')');				
       	end;
	run;
%end;
%mend;
%macro GetData(seq,tblid,geo);
/*The GetData macro will get an individual table's estimates and margin of errors.
  NOTE:  This macro is just an example on how to get just an individual table       */


/*Get the maximum number of lines in the table so it can be used in a keep statement*/
proc sql;
   select max(order) into :tot from stubs.ACS_5yr_Seq_Lookup
   where tblid="&tblid";
quit;

/*Remove spaces from the maximum number of lines variable                           */
%let max = %trim(&tot);

/*Separate a single table's estimates and margin of errors from the rest of the 
  tables in the sequence                                                            */
data work.test_&tblid (keep = &tblid.e1-&tblid.e&max &tblid.m1-&tblid.m&max);
 set work.SF&seq&geo;
run;
%mend;
%CallSt;
*%GetData(0010,B01001,tx);
