# Installer #
Since our first release will only target one DBMS (DB2), we can probably get away with a big jar file and no fancy installer. (The fanciness will come later, when there are multiple DBMSs supported. Maybe allowing people to choose paths, jdbc driver names and such.)
## Tasks ##
  * Need a Maven goal to make the distribution which will be a Zip file.
  * Confirm with Zack that that's okay.
## Questions ##
  * All of the jars or will Maven be used?
  * Does the user need to download 3rd party jars?