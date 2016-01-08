REEF_JAR=`echo ${REEF_HOME}/reef-runtime-yarn/target/reef-runtime-yarn-*-SNAPSHOT.jar`

LOGGING_CONFIG='-Djava.util.logging.config.class=com.microsoft.reef.util.logging.Config'

YARN_ROOT_LOGGER=INFO,console

HELLO_MAIN=com.microsoft.reef.examples.hello.HelloReefYarn
HELLO_JAR=`echo ${REEF_HOME}/reef-examples/target/reef-examples-*-SNAPSHOT-shaded.jar`

YARN_CONF_DIR=${YARN_HOME}/etc/hadoop
CLASSPATH=${YARN_CONF_DIR}:${YARN_HOME}/share/hadoop/common/lib/*:${YARN_HOME}/share/hadoop/common/*:${YARN_HOME}/contrib/capacity-scheduler/*.jar:${YARN_HOME}/share/hadoop/hdfs:${YARN_HOME}/share/hadoop/hdfs/lib/*:${YARN_HOME}/share/hadoop/hdfs/*:${YARN_HOME}/share/hadoop/yarn/lib/*:${YARN_HOME}/share/hadoop/yarn/*:${YARN_HOME}/share/hadoop/mapreduce/lib/*:${YARN_HOME}/share/hadoop/mapreduce/*:${CLASSPATH}

CMD="java -cp ${HADOOP_HOME}/conf:${REEF_JAR}:${HELLO_JAR}:${CLASSPATH} $LOGGING_CONFIG ${HELLO_MAIN} $*"
echo $CMD

#$CMD
$CMD > /dev/null
#$CMD # 2> /dev/null
