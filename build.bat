call mvn -Dmaven.test.skip=true source:jar install
call mvn -Dmaven.test.skip=true compile
call mvn -Dmaven.test.skip=true eclipse:eclipse