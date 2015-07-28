Giraffa is a distributed highly available file system.
Giraffa is an Apache Extras project and 
is not an official project of the Apache Software Foundation.
Giraffa project is related to Apache Hadoop and Apache HBase projects.
Giraffa is licensed under Apache License 2.0.

Requirements:

* JDK 1.6+
* Maven 3.0 or later
* ProtocolBuffer 2.5.0

Build Giraffa jar:

    mvn clean install

Build Giraffa + Project site:

    mvn clean site

    When build is complete, open ${basedir}/target/site/index.html

Build Project Site With Clover Report:

    mvn -Pclover site

    When build is complete, open ${basedir}/target/site/index.html

Please note that clover plugin instruments source files and and it should not be used
for production.

Run Giraffa Web UI in demo mode:

    mvn -Pwebdemo

to stop demo server, type "stop" in console
