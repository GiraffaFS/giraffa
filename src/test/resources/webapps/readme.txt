The only reason why webapps present in this folder is to make hadoop web UI correctly work
under MiniCluster in test environment.

Due to a bug in extraction process, files are placed under wrong folder:

/tmp/Jetty_localhost_44828_hdfs____.43pt20/webapp/webapps/hdfs

instead of

/tmp/Jetty_localhost_44828_hdfs____.43pt20/webapp

this results in a wrong initialization of WebAppContext and as a consequence HDFS UI never comes up.