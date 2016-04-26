# Overview
Giraffa is
- Distributed highly available file system;
- Apache Extras project and is not an official project of the Apache Software Foundation;
- Project which is related to [Apache Hadoop] and [Apache HBase] projects;
- Licensed under Apache License 2.0;

### Requirements

* JDK 1.7+
* Gradle 2.0+
* ProtocolBuffer 2.5.0
* Apache HBase 1.0.0+
* Apache Hadoop 2.5.0+

### Build it
Build Giraffa jar:

    ./gradlew clean assemble

Build Giraffa + Tests:

    ./gradlew clean build

Build Giraffa + Javadoc:

    ./gradlew clean assemble javadoc

Build Giraffa + Code Coverage:

    ./gradlew clean build jacocoTestReport

Build Giraffa + Standalone Distribution:

    ./gradlew clean build tar

Run Giraffa Web UI in demo mode:

    ./gradlew -PmainClass=org.apache.giraffa.web.GiraffaWebDemoRunner execute

Type `stop` in the console and hit `Enter` to end the Giraffa Web UI demo.

[Apache Hadoop]:https://hadoop.apache.org
[Apache HBase]:http://hbase.apache.org