#Surf

Surf is still under development. You can try out some of Surf's features, by following the instructions below.

## Using Surf

The usage below assumes that all of Surf's components are running on the local node.

### Run Surf cache management commands from CLI

From `$SURF_HOME` use run.sh. To clear the cache:

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd clear
```

To load an entry into the cache, run e.g.,

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd load -path /user/bcho/test/README.md
```

This will load data from the given path in HDFS into the cache. Surf must be running, as well as a properly configured HDFS.

### Access Surf FileSystem interface using Hadoop dfs command line

Add configuration at `$HADOOP_HOME/etc/hadoop/core-site.xml`:

```
<configuration>
	<property>
		<name>fs.surf.impl</name>
		<value>org.apache.reef.inmemory.client.CachedFS</value>
	</property>
</configuration>
```

Then run the dfs command while adding the surf jar to the command line. For example, run the -ls command:

```
HADOOP_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar $HADOOP_HOME/bin/hdfs dfs -Dfs.defaultFS=surf://localhost:9001 -ls
```

Note, the actual listing of files is delegated to HDFS. Make sure HDFS is running as well.

### Run Spark job with Surf

Add configuration at `$SURF_HOME/conf/core-site.xml` (Same configuration as Hadoop dfs command line):

```
<configuration>
	<property>
		<name>fs.surf.impl</name>
		<value>org.apache.reef.inmemory.client.CachedFS</value>
	</property>
</configuration>
```

Add Surf's jar to Spark's classpath in `$SURF_HOME/conf/spark-env.sh`:

```
export SPARK_CLASSPATH=/Users/bcho/snu/reef/surf/reef-inmemory/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar:$SPARK_CLASSPATH
```

Even a simple job will fail right now, because the open() method has not been implemented. To run a simple job, and experience failure:

```
./bin/run-example HdfsTest HdfsTest surf://localhost:9001/user/bcho/README.md
```

Make sure to provide a file path that is actually in HDFS (and not just loaded using the `CLI` above). You should see a `UnsupportedOperationException` thrown by `CachedFS`.
