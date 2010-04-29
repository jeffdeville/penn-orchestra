
CREATE SCHEMA DERIVABILITY2 ;


CREATE TABLE "DERIVABILITY2"."PM0"  (
		  "C0" INTEGER , 
		  "C1" VARCHAR(255) ,
		  "C2" INTEGER ,
		  "C3" INTEGER , 
		  "C0_LN" INTEGER NOT NULL WITH DEFAULT 1,
		  "C1_LN" INTEGER NOT NULL WITH DEFAULT 1,
		  "C2_LN" INTEGER NOT NULL WITH DEFAULT 1,
		  "C3_LN" INTEGER NOT NULL WITH DEFAULT 1 )   
		 IN "USERSPACE1" ; 

INSERT INTO "DERIVABILITY2"."PM0"  VALUES (
		  1 , 
		  'Homo Sapiens' ,
		  2 ,
		  3 , 
		  1,
		  1,
		  1,
		  1 );
		  
INSERT INTO "DERIVABILITY2"."PM0"  VALUES (
		  10 , 
		  'Primates' ,
		  NULL ,
		  30 , 
		  1,
		  1,
		  20,
		  1 );
		 