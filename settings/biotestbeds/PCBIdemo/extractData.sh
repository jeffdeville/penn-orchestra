export SID=PlasmoDB

sqlplus -s plasmodb/orchestraplasmodb@$SID @prepareForBulkDataExtraction.sql

sqlplus -s plasmodb/orchestraplasmodb@$SID @genBulkData.sql

./removeAllTrailingSpaces.sh

./cleanFilesForDemo.sh