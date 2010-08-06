export SID=PlasmoDB_dbappserv

sqlplus -s plasmodb/orchestraplasmodb@$SID @prepareForBulkDataExtraction.sql

sqlplus -s plasmodb/orchestraplasmodb@$SID @genBulkData.sql
