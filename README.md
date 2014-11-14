hbase_test
==========
Simple test to compare HBase scan speed with text scan speed.
This test reads entire records, but then filters and prints only the selected record

to run against hbase:

mvn exec:java -Dexec.mainClass=RunHBase -Dexec.args="-t <table name/path> -f <selected family>  -q <selected qualifier> -v <selected value>"

to run against text:

mvn exec:java -Dexec.mainClass=RunText -Dexec.args="-d '<delimiter>' -p <path to file> -i <selected index> -v selected value"


For example:

[root@ucs-node18 hbase_test]# mvn exec:java -Dexec.mainClass=RunText -Dexec.args="-d '|' -p /user/steven/mdb/customer/customer.tbl -i 0 -v 1"
[INFO] Scanning for projects...
[INFO]
[INFO] Using the builder org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder with a thread count of 1
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building hbaseTest 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.3.2:java (default-cli) @ hbaseTest ---
[WARNING] Warning: killAfter is now deprecated. Do you need it ? Please comment on MEXEC-6.
WARNING: org.apache.hadoop.log.EventCounter is deprecated. Please use org.apache.hadoop.log.metrics.EventCounter in all the log4j.properties files.
14/11/14 07:32:06 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|
Read 1500000 records in 2.136973 seconds. 701927.380278 records per second[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 5.543 s
[INFO] Finished at: 2014-11-14T07:32:08+00:00
[INFO] Final Memory: 16M/1963M
[INFO] ------------------------------------------------------------------------
[root@ucs-node18 hbase_test]# mvn exec:java -Dexec.mainClass=RunHBase -Dexec.args="-t /user/steven/mdb/customer_table -f f -q q -v 1"
[INFO] Scanning for projects...
[INFO]
[INFO] Using the builder org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder with a thread count of 1
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building hbaseTest 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.3.2:java (default-cli) @ hbaseTest ---
[WARNING] Warning: killAfter is now deprecated. Do you need it ? Please comment on MEXEC-6.
WARNING: org.apache.hadoop.log.EventCounter is deprecated. Please use org.apache.hadoop.log.metrics.EventCounter in all the log4j.properties files.
14/11/14 07:32:18 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
keyvalues={1/f:a/1415947582740/Put/vlen=18/ts=0, 1/f:b/1415947582740/Put/vlen=17/ts=0, 1/f:c/1415947582740/Put/vlen=2/ts=0, 1/f:d/1415947582740/Put/vlen=15/ts=0, 1/f:e/1415947582740/Put/vlen=6/ts=0, 1/f:f/1415947582740/Put/vlen=8/ts=0, 1/f:g/1415947582740/Put/vlen=62/ts=0, 1/f:h/1415947582740/Put/vlen=0/ts=0}
41556.579220 records/sec
61853.563465 records/sec
62749.984073 records/sec
62273.110165 records/sec
62809.892519 records/sec
8699.000593 records/sec
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 31.931 s
[INFO] Finished at: 2014-11-14T07:32:47+00:00
[INFO] Final Memory: 19M/1878M
[INFO] ------------------------------------------------------------------------


RunHBase can also pass in a local path to a split file which contains one key per row. The test will then run n+1 (where n = number of keys) threads in parallel,
breaking the scan up according to the keys.

[root@ucs-node18 hbase_test]# cat /tmp/splits
1429183
498024

[root@ucs-node18 hbase_test]# mvn exec:java -Dexec.mainClass=RunHBase -Dexec.args="-t /user/steven/mdb/customer_table -f f -q q -v 1 -s /tmp/splits"
[INFO] Scanning for projects...
[INFO]
[INFO] Using the builder org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder with a thread count of 1
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building hbaseTest 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.3.2:java (default-cli) @ hbaseTest ---
[WARNING] Warning: killAfter is now deprecated. Do you need it ? Please comment on MEXEC-6.
WARNING: org.apache.hadoop.log.EventCounter is deprecated. Please use org.apache.hadoop.log.metrics.EventCounter in all the log4j.properties files.
14/11/14 07:34:17 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
keyvalues={1/f:a/1415947582740/Put/vlen=18/ts=0, 1/f:b/1415947582740/Put/vlen=17/ts=0, 1/f:c/1415947582740/Put/vlen=2/ts=0, 1/f:d/1415947582740/Put/vlen=15/ts=0, 1/f:e/1415947582740/Put/vlen=6/ts=0, 1/f:f/1415947582740/Put/vlen=8/ts=0, 1/f:g/1415947582740/Put/vlen=62/ts=0, 1/f:h/1415947582740/Put/vlen=0/ts=0}
119257.778371 records/sec
171086.762240 records/sec
9538.943865 records/sec
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 16.946 s
[INFO] Finished at: 2014-11-14T07:34:31+00:00
[INFO] Final Memory: 19M/1878M
[INFO] ------------------------------------------------------------------------
