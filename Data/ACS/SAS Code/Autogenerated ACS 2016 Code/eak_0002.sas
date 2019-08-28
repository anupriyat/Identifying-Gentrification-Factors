TITLE "e20165ak0002000";
DATA work.SFe0002ak;
 
LENGTH FILEID   $6
       FILETYPE $6
       STUSAB   $2
       CHARITER $3
       SEQUENCE $4
       LOGRECNO $7;
 
INFILE 'C:/Users/jgu/Dropbox (Delta Partners)/MScA/2 - Data Engineering Platforms for Analytics/Data/Census/ACS 2016/group2/e20165ak0002000.txt' DSD TRUNCOVER DELIMITER =',' LRECL=3000;
 
LABEL  FILEID  ='File Identification'
       FILETYPE='File Type'  
       STUSAB  ='State/U.S.-Abbreviation (USPS)'
       CHARITER='Character Iteration'
       SEQUENCE='Sequence Number'
       LOGRECNO='Logical Record Number'
 
 
/*SEX BY AGE */
/*Universe:  Total Population */
 
B01001e1='Total:'
B01001e2='Male:'
B01001e3='Under 5 years'
B01001e4='5 to 9 years'
B01001e5='10 to 14 years'
B01001e6='15 to 17 years'
B01001e7='18 and 19 years'
B01001e8='20 years'
B01001e9='21 years'
B01001e10='22 to 24 years'
B01001e11='25 to 29 years'
B01001e12='30 to 34 years'
B01001e13='35 to 39 years'
B01001e14='40 to 44 years'
B01001e15='45 to 49 years'
B01001e16='50 to 54 years'
B01001e17='55 to 59 years'
B01001e18='60 and 61 years'
B01001e19='62 to 64 years'
B01001e20='65 and 66 years'
B01001e21='67 to 69 years'
B01001e22='70 to 74 years'
B01001e23='75 to 79 years'
B01001e24='80 to 84 years'
B01001e25='85 years and over'
B01001e26='Female:'
B01001e27='Under 5 years'
B01001e28='5 to 9 years'
B01001e29='10 to 14 years'
B01001e30='15 to 17 years'
B01001e31='18 and 19 years'
B01001e32='20 years'
B01001e33='21 years'
B01001e34='22 to 24 years'
B01001e35='25 to 29 years'
B01001e36='30 to 34 years'
B01001e37='35 to 39 years'
B01001e38='40 to 44 years'
B01001e39='45 to 49 years'
B01001e40='50 to 54 years'
B01001e41='55 to 59 years'
B01001e42='60 and 61 years'
B01001e43='62 to 64 years'
B01001e44='65 and 66 years'
B01001e45='67 to 69 years'
B01001e46='70 to 74 years'
B01001e47='75 to 79 years'
B01001e48='80 to 84 years'
B01001e49='85 years and over'
 
/*SEX BY AGE (WHITE ALONE) */
/*Universe:  People Who Are White Alone */
 
B01001Ae1='Total:'
B01001Ae2='Male:'
B01001Ae3='Under 5 years'
B01001Ae4='5 to 9 years'
B01001Ae5='10 to 14 years'
B01001Ae6='15 to 17 years'
B01001Ae7='18 and 19 years'
B01001Ae8='20 to 24 years'
B01001Ae9='25 to 29 years'
B01001Ae10='30 to 34 years'
B01001Ae11='35 to 44 years'
B01001Ae12='45 to 54 years'
B01001Ae13='55 to 64 years'
B01001Ae14='65 to 74 years'
B01001Ae15='75 to 84 years'
B01001Ae16='85 years and over'
B01001Ae17='Female:'
B01001Ae18='Under 5 years'
B01001Ae19='5 to 9 years'
B01001Ae20='10 to 14 years'
B01001Ae21='15 to 17 years'
B01001Ae22='18 and 19 years'
B01001Ae23='20 to 24 years'
B01001Ae24='25 to 29 years'
B01001Ae25='30 to 34 years'
B01001Ae26='35 to 44 years'
B01001Ae27='45 to 54 years'
B01001Ae28='55 to 64 years'
B01001Ae29='65 to 74 years'
B01001Ae30='75 to 84 years'
B01001Ae31='85 years and over'
 
/*SEX BY AGE (BLACK OR AFRICAN AMERICAN ALONE) */
/*Universe:  Black Or African American Alone */
 
B01001Be1='Total:'
B01001Be2='Male:'
B01001Be3='Under 5 years'
B01001Be4='5 to 9 years'
B01001Be5='10 to 14 years'
B01001Be6='15 to 17 years'
B01001Be7='18 and 19 years'
B01001Be8='20 to 24 years'
B01001Be9='25 to 29 years'
B01001Be10='30 to 34 years'
B01001Be11='35 to 44 years'
B01001Be12='45 to 54 years'
B01001Be13='55 to 64 years'
B01001Be14='65 to 74 years'
B01001Be15='75 to 84 years'
B01001Be16='85 years and over'
B01001Be17='Female:'
B01001Be18='Under 5 years'
B01001Be19='5 to 9 years'
B01001Be20='10 to 14 years'
B01001Be21='15 to 17 years'
B01001Be22='18 and 19 years'
B01001Be23='20 to 24 years'
B01001Be24='25 to 29 years'
B01001Be25='30 to 34 years'
B01001Be26='35 to 44 years'
B01001Be27='45 to 54 years'
B01001Be28='55 to 64 years'
B01001Be29='65 to 74 years'
B01001Be30='75 to 84 years'
B01001Be31='85 years and over'
 
/*SEX BY AGE (AMERICAN INDIAN AND ALASKA NATIVE ALONE) */
/*Universe:  People Who Are American Indian And Alaska Native Alone */
 
B01001Ce1='Total:'
B01001Ce2='Male:'
B01001Ce3='Under 5 years'
B01001Ce4='5 to 9 years'
B01001Ce5='10 to 14 years'
B01001Ce6='15 to 17 years'
B01001Ce7='18 and 19 years'
B01001Ce8='20 to 24 years'
B01001Ce9='25 to 29 years'
B01001Ce10='30 to 34 years'
B01001Ce11='35 to 44 years'
B01001Ce12='45 to 54 years'
B01001Ce13='55 to 64 years'
B01001Ce14='65 to 74 years'
B01001Ce15='75 to 84 years'
B01001Ce16='85 years and over'
B01001Ce17='Female:'
B01001Ce18='Under 5 years'
B01001Ce19='5 to 9 years'
B01001Ce20='10 to 14 years'
B01001Ce21='15 to 17 years'
B01001Ce22='18 and 19 years'
B01001Ce23='20 to 24 years'
B01001Ce24='25 to 29 years'
B01001Ce25='30 to 34 years'
B01001Ce26='35 to 44 years'
B01001Ce27='45 to 54 years'
B01001Ce28='55 to 64 years'
B01001Ce29='65 to 74 years'
B01001Ce30='75 to 84 years'
B01001Ce31='85 years and over'
 
/*SEX BY AGE (ASIAN ALONE) */
/*Universe:  People Who Are Asian Alone */
 
B01001De1='Total:'
B01001De2='Male:'
B01001De3='Under 5 years'
B01001De4='5 to 9 years'
B01001De5='10 to 14 years'
B01001De6='15 to 17 years'
B01001De7='18 and 19 years'
B01001De8='20 to 24 years'
B01001De9='25 to 29 years'
B01001De10='30 to 34 years'
B01001De11='35 to 44 years'
B01001De12='45 to 54 years'
B01001De13='55 to 64 years'
B01001De14='65 to 74 years'
B01001De15='75 to 84 years'
B01001De16='85 years and over'
B01001De17='Female:'
B01001De18='Under 5 years'
B01001De19='5 to 9 years'
B01001De20='10 to 14 years'
B01001De21='15 to 17 years'
B01001De22='18 and 19 years'
B01001De23='20 to 24 years'
B01001De24='25 to 29 years'
B01001De25='30 to 34 years'
B01001De26='35 to 44 years'
B01001De27='45 to 54 years'
B01001De28='55 to 64 years'
B01001De29='65 to 74 years'
B01001De30='75 to 84 years'
B01001De31='85 years and over'
 
/*SEX BY AGE (NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER ALONE) */
/*Universe:  People Who Are Native Hawaiian And Other Pacific Islander Alone */
 
B01001Ee1='Total:'
B01001Ee2='Male:'
B01001Ee3='Under 5 years'
B01001Ee4='5 to 9 years'
B01001Ee5='10 to 14 years'
B01001Ee6='15 to 17 years'
B01001Ee7='18 and 19 years'
B01001Ee8='20 to 24 years'
B01001Ee9='25 to 29 years'
B01001Ee10='30 to 34 years'
B01001Ee11='35 to 44 years'
B01001Ee12='45 to 54 years'
B01001Ee13='55 to 64 years'
B01001Ee14='65 to 74 years'
B01001Ee15='75 to 84 years'
B01001Ee16='85 years and over'
B01001Ee17='Female:'
B01001Ee18='Under 5 years'
B01001Ee19='5 to 9 years'
B01001Ee20='10 to 14 years'
B01001Ee21='15 to 17 years'
B01001Ee22='18 and 19 years'
B01001Ee23='20 to 24 years'
B01001Ee24='25 to 29 years'
B01001Ee25='30 to 34 years'
B01001Ee26='35 to 44 years'
B01001Ee27='45 to 54 years'
B01001Ee28='55 to 64 years'
B01001Ee29='65 to 74 years'
B01001Ee30='75 to 84 years'
B01001Ee31='85 years and over'
 
/*SEX BY AGE (SOME OTHER RACE ALONE) */
/*Universe:  People Who Are Some Other Race Alone */
 
B01001Fe1='Total:'
B01001Fe2='Male:'
B01001Fe3='Under 5 years'
B01001Fe4='5 to 9 years'
B01001Fe5='10 to 14 years'
B01001Fe6='15 to 17 years'
B01001Fe7='18 and 19 years'
B01001Fe8='20 to 24 years'
B01001Fe9='25 to 29 years'
B01001Fe10='30 to 34 years'
B01001Fe11='35 to 44 years'
B01001Fe12='45 to 54 years'
B01001Fe13='55 to 64 years'
B01001Fe14='65 to 74 years'
B01001Fe15='75 to 84 years'
B01001Fe16='85 years and over'
B01001Fe17='Female:'
B01001Fe18='Under 5 years'
B01001Fe19='5 to 9 years'
B01001Fe20='10 to 14 years'
B01001Fe21='15 to 17 years'
B01001Fe22='18 and 19 years'
B01001Fe23='20 to 24 years'
B01001Fe24='25 to 29 years'
B01001Fe25='30 to 34 years'
B01001Fe26='35 to 44 years'
B01001Fe27='45 to 54 years'
B01001Fe28='55 to 64 years'
B01001Fe29='65 to 74 years'
B01001Fe30='75 to 84 years'
B01001Fe31='85 years and over'
;
 
 
INPUT
 
FILEID   $ 
FILETYPE $ 
STUSAB   $ 
CHARITER $ 
SEQUENCE $ 
LOGRECNO $ 
 
B01001e1
B01001e2
B01001e3
B01001e4
B01001e5
B01001e6
B01001e7
B01001e8
B01001e9
B01001e10
B01001e11
B01001e12
B01001e13
B01001e14
B01001e15
B01001e16
B01001e17
B01001e18
B01001e19
B01001e20
B01001e21
B01001e22
B01001e23
B01001e24
B01001e25
B01001e26
B01001e27
B01001e28
B01001e29
B01001e30
B01001e31
B01001e32
B01001e33
B01001e34
B01001e35
B01001e36
B01001e37
B01001e38
B01001e39
B01001e40
B01001e41
B01001e42
B01001e43
B01001e44
B01001e45
B01001e46
B01001e47
B01001e48
B01001e49
 
B01001Ae1
B01001Ae2
B01001Ae3
B01001Ae4
B01001Ae5
B01001Ae6
B01001Ae7
B01001Ae8
B01001Ae9
B01001Ae10
B01001Ae11
B01001Ae12
B01001Ae13
B01001Ae14
B01001Ae15
B01001Ae16
B01001Ae17
B01001Ae18
B01001Ae19
B01001Ae20
B01001Ae21
B01001Ae22
B01001Ae23
B01001Ae24
B01001Ae25
B01001Ae26
B01001Ae27
B01001Ae28
B01001Ae29
B01001Ae30
B01001Ae31
 
B01001Be1
B01001Be2
B01001Be3
B01001Be4
B01001Be5
B01001Be6
B01001Be7
B01001Be8
B01001Be9
B01001Be10
B01001Be11
B01001Be12
B01001Be13
B01001Be14
B01001Be15
B01001Be16
B01001Be17
B01001Be18
B01001Be19
B01001Be20
B01001Be21
B01001Be22
B01001Be23
B01001Be24
B01001Be25
B01001Be26
B01001Be27
B01001Be28
B01001Be29
B01001Be30
B01001Be31
 
B01001Ce1
B01001Ce2
B01001Ce3
B01001Ce4
B01001Ce5
B01001Ce6
B01001Ce7
B01001Ce8
B01001Ce9
B01001Ce10
B01001Ce11
B01001Ce12
B01001Ce13
B01001Ce14
B01001Ce15
B01001Ce16
B01001Ce17
B01001Ce18
B01001Ce19
B01001Ce20
B01001Ce21
B01001Ce22
B01001Ce23
B01001Ce24
B01001Ce25
B01001Ce26
B01001Ce27
B01001Ce28
B01001Ce29
B01001Ce30
B01001Ce31
 
B01001De1
B01001De2
B01001De3
B01001De4
B01001De5
B01001De6
B01001De7
B01001De8
B01001De9
B01001De10
B01001De11
B01001De12
B01001De13
B01001De14
B01001De15
B01001De16
B01001De17
B01001De18
B01001De19
B01001De20
B01001De21
B01001De22
B01001De23
B01001De24
B01001De25
B01001De26
B01001De27
B01001De28
B01001De29
B01001De30
B01001De31
 
B01001Ee1
B01001Ee2
B01001Ee3
B01001Ee4
B01001Ee5
B01001Ee6
B01001Ee7
B01001Ee8
B01001Ee9
B01001Ee10
B01001Ee11
B01001Ee12
B01001Ee13
B01001Ee14
B01001Ee15
B01001Ee16
B01001Ee17
B01001Ee18
B01001Ee19
B01001Ee20
B01001Ee21
B01001Ee22
B01001Ee23
B01001Ee24
B01001Ee25
B01001Ee26
B01001Ee27
B01001Ee28
B01001Ee29
B01001Ee30
B01001Ee31
 
B01001Fe1
B01001Fe2
B01001Fe3
B01001Fe4
B01001Fe5
B01001Fe6
B01001Fe7
B01001Fe8
B01001Fe9
B01001Fe10
B01001Fe11
B01001Fe12
B01001Fe13
B01001Fe14
B01001Fe15
B01001Fe16
B01001Fe17
B01001Fe18
B01001Fe19
B01001Fe20
B01001Fe21
B01001Fe22
B01001Fe23
B01001Fe24
B01001Fe25
B01001Fe26
B01001Fe27
B01001Fe28
B01001Fe29
B01001Fe30
B01001Fe31
;
RUN;