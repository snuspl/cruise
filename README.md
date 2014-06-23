#Surf

Surf is still under development. You can try out some of Surf's features, by following the instructions below.

## Starting Surf

Surf must be run along with a base FS. Here, we give an example of running with HDFS.

### Launch Surf

Surf can be launched using`run.sh`. E.g., to launch while specifying the base FS's address:

```
./run.sh org.apache.reef.inmemory.Launch -dfs_address "hdfs://localhost:9000"
```

### Launch HDFS

If you already have an HDFS instance running, the address for that instance should be provided above. If not, a simple way to start a working (though not persistent) HDFS instance is by using the Hadoop MiniCluster. Start a MiniCluster with a specific NameNode port, by running from `$HADOOP_HOME`: 

```
HADOOP_CLASSPATH=share/hadoop/yarn/test/hadoop-yarn-server-tests-2.2.0-tests.jar ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.2.0-tests.jar minicluster -nnport 9000
```

More details can be found in the [Hadoop documentation](http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-common/CLIMiniCluster.html).

## Using Surf

By extending Hadoop's `FileSystem` abstract class, tools and frameworks that were built to use HDFS can be configured to run Surf. In this way, Surf runs as a transparent caching layer above HDFS. A cache management CLI is also provided for manual adjustment of the cache, but this will not be the main production interface.

The usage below assumes that all of Surf's components are running on the local node.

### Run Surf cache management commands from CLI

From `$SURF_HOME` use run.sh. To clear the cache:

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd clear
```

The `load` command can be used to explicitly load a path in the base FS into the cache. If you don't already have a file in HDFS, copy one in by running from `$HADOOP_HOME`:

```
bin/hdfs dfs -Dfs.defaultFS=hdfs://localhost:9000 -copyFromLocal ./share/doc/hadoop/hdfs/LICENSE.txt
```

To load an entry into the cache, supply the full path (replacing `{name}` with the username):

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd load -path /user/{name}/LICENSE.txt
```

This will load data from the given path in HDFS into the cache.

### Access Surf FileSystem interface using Hadoop dfs command line

Add configuration at `$HADOOP_HOME/etc/hadoop/core-site.xml` to specify the `SurfFS` as the implementation of the `surf://` scheme, and give Surf the base HDFS address:

```
<configuration>
  <property>
    <name>fs.surf.impl</name>
    <value>org.apache.reef.inmemory.client.SurfFS</value>
  </property>
  <property>
    <name>surf.basefs</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

Then run the dfs command while adding the surf jar to the command line. For example, run the -ls command:

```
HADOOP_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar $HADOOP_HOME/bin/hdfs dfs -Dfs.defaultFS=surf://localhost:9001 -ls
```

Note, the actual listing of files is delegated to HDFS. Make sure HDFS is running as well.

### Run Spark job with Surf

Add the same `core-site.xml` configuration above to the configuration at `$SURF_HOME/conf/core-site.xml`. Add Surf's jar to Spark's classpath in `$SURF_HOME/conf/spark-env.sh`:

```
export SPARK_CLASSPATH=/Users/readme/surf/reef-inmemory/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar:$SPARK_CLASSPATH
```

Even a simple job will fail right now, because the `open()` method has not been implemented. To run a simple job, and experience failure:

```
./bin/run-example HdfsTest HdfsTest surf://localhost:9001/user/{name}/LICENSE.txt
```

Make sure to provide a file path that is actually in HDFS. You should see a `UnsupportedOperationException` thrown on `SurfFS.open()`.
