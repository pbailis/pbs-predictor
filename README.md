pbs-predictor
===========

Implementation-independent PBS predictor.

Simple demo of [PBS Predictions](http://pbs.cs.berkeley.edu/#demo) currently using simple RTT/2 traces from Cassandra via JMX.

Currently undocumented work-in-progress with extensible latency trace classes.

Profile a Cassandra node on `127.0.0.1` with JMX port `7100`:
`java -jar target/predictor-1.0-SNAPSHOT.jar 127.0.0.1 7100`
