## Running REEF Application on YARN Environment

####Prerequisite
* Hadoop (>=2.2.0) installed
* REEF is installed on all Clusters

####Run HelloReefYarn
You can find [`HelloReefYarn`](https://github.com/Microsoft-CISL/REEF/blob/master/reef-examples/src/main/java/com/microsoft/reef/examples/hello/HelloReefYarn.java) application under [`examples`](https://github.com/Microsoft-CISL/REEF/tree/master/reef-examples). You can run this example with [`helloreefyarn.sh`](script/helloreefyarn.sh) script. This shell script shows how we set a command to run a REEF Application on YARN clusters.

You need to specify where the runtime JAR file exists, configuration for logging, which package the main function is in, and so on. Note that when you set `CLASSPATH`, `YARN_CONF_DIR` should appear in front of any other directories.

With those variables, you establish a command to execute the application.

```
CMD="java -cp ${HADOOP_HOME}/conf:${REEF_JAR}:${HELLO_JAR}:${CLASSPATH} $LOGGING_CONFIG ${HELLO_MAIN} $*"
```

`echo $CMD` runs the application with the configuration above. You may redirect the result to `/dev/null` because we don't need to see it on the screen.

```
#$CMD
$CMD > /dev/null
#$CMD # 2> /dev/null
```
