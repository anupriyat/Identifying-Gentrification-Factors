TITLE "e20165ak0001000";
DATA work.SFe0001ak;
 
LENGTH FILEID   $6
       FILETYPE $6
       STUSAB   $2
       CHARITER $3
       SEQUENCE $4
       LOGRECNO $7;
 
INFILE 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/ACS 2016/group2/e20165ak0001000.txt' DSD TRUNCOVER DELIMITER =',' LRECL=3000;
 
LABEL  FILEID  ='File Identification'
       FILETYPE='File Type'  
       STUSAB  ='State/U.S.-Abbreviation (USPS)'
       CHARITER='Character Iteration'
       SEQUENCE='Sequence Number'
       LOGRECNO='Logical Record Number'
 
 
/*UNWEIGHTED SAMPLE COUNT OF THE POPULATION */
/*Universe:  Total Population */
 
B00001e1='Total'
 
/*UNWEIGHTED SAMPLE HOUSING UNITS */
/*Universe:  Housing Units */
 
B00002e1='Total'
;
 
 
INPUT
 
FILEID   $ 
FILETYPE $ 
STUSAB   $ 
CHARITER $ 
SEQUENCE $ 
LOGRECNO $ 
 
B00001e1
 
B00002e1
;
RUN;
