#Surf

Surf is still under development. You can try out some of Surf's features, by following the instructions below.

## Starting Surf

Surf must be run along with a base FS. Here, we give an example of running with HDFS.

### Launch Surf

Surf can be launched using`run.sh`. E.g., to launch while specifying the base FS's address:

```
./run.sh org.apache.reef.inmemory.Launch -dfs_address "hdfs://localhost:9000"
```
* You can run the application on YARN runtime, by adding `-local "false"` argument. By default, it runs on local runtime.

### Launch HDFS

If you already have an HDFS instance running, the address for that instance should be provided above. If not, a simple way to start a working (though not persistent) HDFS instance is by using the Hadoop MiniCluster. Start a MiniCluster with a specific NameNode port, by running from `$HADOOP_HOME`: 

```
HADOOP_CLASSPATH=share/hadoop/yarn/test/hadoop-yarn-server-tests-2.2.0-tests.jar ./bin/hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.2.0-tests.jar minicluster -nnport 9000
```

More details can be found in the [Hadoop documentation](http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-common/CLIMiniCluster.html).

## Using Surf

By extending Hadoop's `FileSystem` abstract class, tools and frameworks that were built to use HDFS can be configured to run Surf. In this way, Surf runs as a transparent caching layer above HDFS. A cache management CLI is also provided for manual adjustment of the cache, but this will not be the main production interface.

The usage below assumes that all of Surf's components are running on `localhost`.

### Run Surf cache management commands from CLI

From `$SURF_HOME` use run.sh. To clear the cache:

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd clear
```

The `load` command can be used to explicitly load a path in the base FS into the cache. If you don't already have a file in HDFS, copy one in by running from `$HADOOP_HOME`:

```
bin/hdfs dfs -Dfs.defaultFS=hdfs://localhost:9000 -copyFromLocal ./share/doc/hadoop/hdfs/LICENSE.txt
```

To load an entry into the cache, supply the full path (replacing `{name}` with your username):

```
./run.sh org.apache.reef.inmemory.cli.CLI -cmd load -path /user/{name}/LICENSE.txt
```

This will load data from the given path in HDFS into the cache.

### Access Surf FileSystem interface using Hadoop dfs command line

Add configuration at `$HADOOP_HOME/etc/hadoop/core-site.xml` to specify the `SurfFS` class as the implementation of the `surf://` scheme, and give Surf the base HDFS address:

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

Running the dfs command with Surf requires some additional configuration to be passed in on the command line. The jar must be added to `HADOOP_CLASSPATH`. The address is specified by configuring `fs.defaultFS`. The address can be specified as a well-known address, e.g. `surf://localhost:18000`, or by using Surf's YARN job identifier, prefixed with `yarn.`: `surf://yarn.reef-job-InMemory`.

For example, run the -ls command:

```
HADOOP_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar $HADOOP_HOME/bin/hdfs dfs -Dfs.defaultFS=surf://yarn.reef-job-InMemory -ls
```

Note, the actual listing of files is delegated to HDFS. Reading data on HDFS through Surf stores that data in memory on the Surf nodes. For example, run the -copyToLocal command twice:

```
HADOOP_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar $HADOOP_HOME/bin/hdfs dfs -Dfs.defaultFS=surf://yarn.reef-job-InMemory -copyToLocal /user/{name}/LICENSE.txt
```

The first time the command is run, the data on HDFS is loaded onto Surf, and then copied to the local filesystem. The second time, the data previously loaded into memory on Surf is immediately copied to the local filesystem.


### Run Spark job with Surf

Spark must be built to work with Hadoop Yarn 2.2.0. Detailed information on building Spark can be found [within the Spark documentation](http://spark.apache.org/docs/latest/building-with-maven.html). The specific command used for 2.2.0 is:

```
mvn -Pyarn -Phadoop-2.2 -Dhadoop.version=2.2.0 -DskipTests clean package
```

Once Spark is built, it must be configured. Add the same `core-site.xml` configuration for the dfs command line (above) to the configuration at `$SPARK_HOME/conf/core-site.xml`. Then, add Surf's jar to Spark's classpath in `$SPARK_HOME/conf/spark-env.sh`:

```
export SPARK_CLASSPATH=$SURF_HOME/target/reef-inmemory-1.0-SNAPSHOT-shaded.jar:$SPARK_CLASSPATH
```

An example job with a `surf://` path will succeed: 

```
./bin/run-example HdfsTest HdfsTest surf://localhost:9001/user/{name}/LICENSE.txt
```

## Configuring Surf

Some of the ways in which to configure Surf are covered here.

### Configure Replication Policy

A replication policy is given as a list of rules, and the default action for when none of the rules can be applied. Rules consist of a conjunction of conditions which all must be satistifed, and the action that is taken when all conditions are met.

When a file is added to Surf, the replication rules are considered one-by-one, in the order they are listed, until an action is taken.

To specify a replication policy, create a policy file in json, and then pass it in as a command line argument in the form `-replication_rules conf/replication.json`. The policy can also be listed and uploaded via the CLI, using the arguments `-cmd replication-list` and `-cmd replication-upload -path <path>`, respectively.

An example json file looks like:

```json
{
  "rules" : [
    {
      "id" : "daily-small",
      "conditions" : [
        {
          "type"     : "path",
          "operator" : "recursive",
          "operand"  : "/daily/"
        },
        {
          "type"     : "size",
          "operator" : "lt",
          "operand"  : "128M"
        }
      ],
      "action" : {
        "factor" : 4,
        "pin"    : true
      }
    },
    {
      "id" : "hourly",
      "conditions" : [
        {
          "type"     : "path",
          "operator" : "recursive",
          "operand"  : "/hourly/"
        }
      ],
      "action" : {
        "factor" : -1,
        "pin"    : false
      }
    }
  ],
  "default" : {
    "factor" : 2,
    "pin"    : false
  }
}
```

The current types of conditions available are:

- Path conditions -- NOTE: all rules should start with a path condition
  - type: `path`
  - operator: {`exact`, `recursive`}
  - operand: (path)
- Size conditions
  - type: `size`
  - operator: {`lt`, `leq`, `gt`, `geq`}
  - operand: (size in raw bytes or (num){k, m, g}
- In the near future, filters will also be provided:
  - type: `filter`
  - operator: {`include`, `exclude`}
  - operand: glob -- following rsync include/exclude rules