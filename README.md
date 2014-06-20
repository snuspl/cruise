#Surf

Surf is still under development.

## Using Surf

The usage below assumes that all of Surf is running on the local node.

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

### Access surf FileSystem interface using hadoop dfs command line

Set the configuration at `$HADOOP_HOME/etc/hadoop/core-site.xml`:

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
HADOOP_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar $HADOOP_HOME/bin/hdfs dfs -Dfs.defaultFS=surf://localhost:9000 -ls
```

Note, -ls will just delegate to HDFS. Make sure HDFS is running as well.
