options symbolgen; 
ods listing off;

%macro create_exportable_files;


%DO I = 1 %TO 122;
	
	%if &i.<10 %then %let datas=%scan(SF000,1)%scan(&i.,1);
	%else %if &i.<100 %then %let datas=%scan(SF00,1)%scan(&i.,1);
	%else %if &i.<1000 %then %let datas=%scan(SF0,1)%scan(&i.,1);
	%else %let datas=%scan(SF,1)%scan(&i.,1);

	data &datas._2016;
		set sas.&datas.:;
		drop
					NAME
					AIANHH
					AIANHHFP
					AIHHTLI
					AITSCE
					AITS
					ANRC
					CBSA
					MACC
					MEMI
					NECTA
					CNECTA
					NECTADIV
					CDCURR
					SLDU
					SLDL
					ZCTA5
					PUMA5
					PUMA1
					FILETYPE
					CHARITER
					CONCIT
					CSA
					METDIV
					UA
					UACP
					VTD
					ZCTA3
					SUBMCD
					SDELM
					SDSEC
					SDUNI
					UR
					PCI
					TAZ
					UGA;

	run;

/*	  PROC EXPORT DBMS=CSV DATA=&datas._2016*/
/*	  OUTFILE="C:\Users\jgu\Dropbox (Delta Partners)\MScA\2 - Data Engineering Platforms for Analytics\Data\Census\CSV\ACS 2016 CSV\&datas._2016.CSV";*/
/*	  RUN;*/


	ODS EXCLUDE ALL ;
	ODS OUTPUT nLevels = work.levels ;
	PROC FREQ DATa = work.&datas._2016 NLEVELS ;
	TABLE _ALL_ / NOPRINT ;
	RUN ;
	ODS SELECT ALL ;
	PROC SQL NOPRINT ;
	SELECT tableVar INTO : empty_vars SEPARATED BY " "
	FROM work.levels
	WHERE NNonMissLevels = 0
	;
	QUIT ;
	DATA work.just_data ;
	SET work.&datas._2016 (keep = &empty_vars) ;
	/*IF N(OF _NUMERIC_) = 0 AND MISSING(COMPRESS(CATS(OF _CHARACTER_)));*/
	RUN ;
		


	proc contents
	     data = just_data
	          noprint
	          out = data_info
	               (keep = name varnum);
	run;


	* sort "data_info" by "varnum";
	* export the sorted data set with the name "variable_names", and keep just the "name" column;
	proc sort
	     data = data_info
	          out = variable_names_aux(keep = name);
	     by varnum;
	run;

	proc datasets library=WORK;
	   delete &datas._2016;
	run;

	data variable_names;
		set variable_names variable_names_aux;
	run;
	DM "clear log";

%END;



%mend;

%create_exportable_files;








