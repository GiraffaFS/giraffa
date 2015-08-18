# Overview
Giraffa is
- Distributed highly available file system;
- Apache Extras project and is not an official project of the Apache Software Foundation;
- Project which is related to [Apache Hadoop] and [Apache HBase] projects;
- Licensed under Apache License 2.0;

### Requirements

* JDK 1.7+
* Apache Maven 3.0+
* ProtocolBuffer 2.5.0
* Apache HBase 1.0.0+
* Apache Hadoop 2.5.0+

### Build it
Build Giraffa jar:

    mvn clean install

Build Giraffa + Project site:

    mvn clean site

When build is complete, open ${basedir}/target/site/index.html

Build Project Site With Clover Report:

    mvn -Pclover site

When build is complete, open ${basedir}/target/site/index.html

Please note that clover plugin instruments source files and it should not be used for production.

Run Giraffa Web UI in demo mode:

    mvn -Pwebdemo

to stop demo server, type "stop" in console

[Apache Hadoop]:https://hadoop.apache.org
[Apache HBase]:http://hbase.apache.org