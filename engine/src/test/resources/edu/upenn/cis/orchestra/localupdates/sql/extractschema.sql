
CREATE TABLE "EXTRACTSCHEMA"."BASE"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) )   
		 IN "USERSPACE1" ; 

ALTER TABLE "EXTRACTSCHEMA"."BASE" 
	ADD CONSTRAINT "ES_BASE_PK" PRIMARY KEY
		("RID");


CREATE TABLE "EXTRACTSCHEMA"."BASE_PREV"  (
		  "RID" INTEGER , 
		  "RSTR" VARCHAR(10) )   
		 IN "USERSPACE1" ; 


CREATE TABLE "EXTRACTSCHEMA"."BASE_L_INS"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
 

CREATE TABLE "EXTRACTSCHEMA"."BASE_L_DEL"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
 

CREATE TABLE "EXTRACTSCHEMA"."BASE_R_INS"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
 

CREATE TABLE "EXTRACTSCHEMA"."BASE_R_DEL"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
 

CREATE TABLE "EXTRACTSCHEMA"."BASE_L"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
 

CREATE TABLE "EXTRACTSCHEMA"."BASE_R"  (
		  "RID" INTEGER NOT NULL , 
		  "RSTR" VARCHAR(10) NOT NULL )   
		 IN "USERSPACE1" ; 
