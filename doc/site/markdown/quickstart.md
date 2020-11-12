<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Quickstart

Below are instructions to quickly set up an environment to test Omid in your local machine.

## Requirements

1. Maven 3.x
2. Java 7
3. HBase 0.98
4. Protobuf 2.5.0

## TSO Setup

### 1. Download and Install the Required HBase Version
You can find HBase distributions in [this page](http://www.apache.org/dyn/closer.cgi/hbase/).
Then start HBase in [standalone mode](https://hbase.apache.org/book.html#quickstart).

### 2. Clone the [Omid repository](https://github.com/apache/incubator-omid) and Build the TSO Package:

```sh
$ git clone git@github.com:apache/incubator-omid.git
$ cd omid
$ mvn clean install -Phbase-1 (for HBase 1.x versions)
or
$ mvn clean install -Phbase-2 (for HBase 2.x versions)
```
This will generate a binary package containing all dependencies for the TSO in tso-server/target/tso-server-\<VERSION\>-bin.tar.gz. 

**Be aware** Unit tests use HBase mini cluster, it typically fails to start if you are on VPN, thus unit test fail.
Unit tests coverage is also quite extensive and take a while to run on each build (~15min at the moment of writing). So, consider using
`mvn clean install -DskipTests` to speed temporal builds. Note that `-Dmaven.test.skip=true` [is NOT an equivalent](http://ericlefevre.net/wordpress/2008/02/21/skipping-tests-with-maven/).

As an alternative to clone the project, you can download the required version for the TSO tar.gz package from the [release repository](https://dist.apache.org/repos/dist/release/incubator/omid/).

You can also see the [build history here](https://github.com/apache/incubator-omid/tags).

### 3. Extract the TSO Package

```sh
$ tar zxvf tso-server-<VERSION>-bin.tar.gz
$ cd tso-server-<VERSION>
```

### 4. Create Omid Tables
Ensure that the setting for hbase.zookeeper.quorum in conf/hbase-site.xml points to your zookeeper instance, and create the 
Timestamp Table and the Commit Table using the omid.sh script included in the bin directory of the tso server:
      
```sh
$ bin/omid.sh create-hbase-commit-table -numRegions 16
$ bin/omid.sh create-hbase-timestamp-table
```

These two tables are required by Omid and they must not be accessed by client applications.

### 5. Start the TSO Server

```sh
$ bin/omid.sh tso
```

This starts the TSO server that in turn will connect to HBase to store information in HBase. By default the TSO listens on 
port 54758. If you want to change the TSO configuration, you can modify the contents in the conf/omid.conf file.

## HBase Client Usage

### 1. Create a New Java Application
Use your favorite IDE an create a new project.

### 2. Add hbase-client Dependency
Choose the right version of the hbase-client jar. For example, in a Maven-based app add the following dependency in the
pom.xml file:

```xml
<dependency>
   <groupId>org.apache.omid</groupId>
   <artifactId>omid-hbase-client-hbase1.x</artifactId>
   <version>1.0.1</version>
</dependency>
```

### 3. Start Coding Using the Omid Client Interfaces
In Omid there are two client interfaces: `TTable` and `TransactionManager` (_These interfaces will likely change slightly 
in future._):

1. The `TransactionManager` is used for creating transactional contexts, that is, transactions. A builder is provided in 
the `HBaseTransactionManager` class in order to get the TransactionManager interface.

2. `TTable` is used for putting, getting and scanning entries in a HBase table. TTable's
interface is similar to the standard `HTableInterface`, and only requires passing the transactional context as a
first parameter in the transactional aware methods (e.g. `put(Transaction tx, Put put)`)

## Example Application

Below is provided a sample application accessing data transactionally. Its a dummy application that writes two cells in two 
different rows of a table in a transactional context, but is enough to show how the different Omid client APIs are used. A 
detailed explanation of the client interfaces can be found in the [Basic Examples](basic-examples.html) section.

```java
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.testng.annotations.Test;

public class OmidExample {

    public static final byte[] family = Bytes.toBytes("MY_CF");
    public static final byte[] qualifier = Bytes.toBytes("MY_Q");

    public static void main(String[] args) throws Exception {

        try (TransactionManager tm = HBaseTransactionManager.newInstance();
             Connection conn = ConnectionFactory.createConnection();
             TTable txTable = new TTable(conn, "MY_TX_TABLE")) {

            Transaction tx = tm.begin();

            Put row1 = new Put(Bytes.toBytes("EXAMPLE_ROW1"));
            row1.addColumn(family, qualifier, Bytes.toBytes("val1"));
            txTable.put(tx, row1);

            Put row2 = new Put(Bytes.toBytes("EXAMPLE_ROW2"));
            row2.addColumn(family, qualifier, Bytes.toBytes("val2"));
            txTable.put(tx, row2);

            tm.commit(tx);

        }

    }

}
``` 

To run the application, make sure `core-site.xml` and `hbase-site.xml` for your HBase cluster are present in your 
CLASSPATH. The default configuration settings for the Omid client are loaded from the `default-hbase-omid-client-config.yml` 
file. In order to change the client default settings, you can either; 1) put your specific configuration settings 
in a file named `hbase-omid-client-config.yml` and include it in the CLASSPATH; or 2) do it programmatically in 
the application code by creating an instance of the `HBaseOmidClientConfiguration` class and passing it in the 
creation of the transaction manager:

```java
    import org.apache.hadoop.hbase.client.Connection;
    import org.apache.hadoop.hbase.client.ConnectionFactory;
    import org.apache.hadoop.hbase.util.Bytes;
    import org.apache.omid.transaction.HBaseOmidClientConfiguration;
    import org.apache.omid.transaction.HBaseTransactionManager;
    import org.apache.omid.transaction.TTable;
    import org.apache.omid.transaction.TransactionManager;
    import org.apache.omid.tso.client.OmidClientConfiguration;

    public class OmidExample {
        
        public static void main(String[] args) throws Exception {
            HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
            omidClientConfiguration.setConnectionType(OmidClientConfiguration.ConnType.DIRECT);
            omidClientConfiguration.setConnectionString("my_tso_server_host:54758");
            omidClientConfiguration.setRetryDelayInMs(3000);
    
            try (TransactionManager tm = HBaseTransactionManager.newInstance(omidClientConfiguration);
                 Connection conn = ConnectionFactory.createConnection();
                 TTable txTable = new TTable(conn, "MY_TX_TABLE")) {
                
            }
        }

    }

```

Also, you will need to create a HBase table "MY_TX_TABLE", with column family "MY_CF", and with `TTL` disabled and
`VERSIONS` set to `Integer.MAX_VALUE`. For example using the HBase shell:

```
create 'MY_TX_TABLE', {NAME => 'MY_CF', VERSIONS => '2147483647', TTL => '2147483647'}
```

This example assumes non-secure communication with HBase. If your HBase cluster is secured with Kerberos, you will need to 
use the `UserGroupInformation` API to log in securely.

## The Omid Compactor Coprocessor

Omid includes a jar with an HBase coprocessor for performing data cleanup that operates during compactions, both minor and
major. Specifically, it does the following:

* Cleans up garbage data from aborted transactions
* Purges deleted cells. Omid deletes work by placing a special tombstone marker in cells. The compactor
  detects these and actually purges data when it is safe to do so (i.e. when there are no committable transactions
  that may read the data).
* 'Heals' committed cells for which the writer failed to write shadow cells.

To deploy the coprocessor, the coprocessor jar must be placed in a location (typically on HDFS) that is accessible
by HBase region servers. The coprocessor may then be enabled on a transactional table by the following steps in the HBase shell:

**1) Disable the table**

```
disable 'MY_TX_TABLE'
```

**2) Put coprocessor jar in hbase lib dir and restart hbase**

```
cp hbase-coprocessor/target/omid-hbase-coprocessor-1.0.1.jar $HBASE_HOME/lib
```

**3) Add a coprocessor specification to the table via a "coprocessor" attribute. The coprocessor spec may (and usually will)
also include the name of the Omid commit table**

```
alter 'MY_TX_TABLE', METHOD => 'table_att', 'coprocessor'=>'|org.apache.omid.transaction.OmidCompactor|1001|omid.committable.tablename=OMID_COMMIT_TABLE'
```

**4) Add an "OMID_ENABLED => true" flag to any column families which the co-processor should work on**

```
alter 'MY_TX_TABLE', { NAME => 'MY_CF', METADATA =>  {'OMID_ENABLED' => 'true'}}
```

**5) Re-enable the table**

```
enable 'MY_TX_TABLE'
```

## Server Side Filtering
Omid can offload the snapshot filtering while get/scan operations to an Hbase coprocessor.
To use this coprocessor follow these steps:

**1) Disable the table**

```
disable 'MY_TX_TABLE'
```

**2) Put coprocessor jar in hbase lib dir and restart hbase**

```
cp hbase-coprocessor/target/omid-hbase-coprocessor-1.0.1.jar $HBASE_HOME/lib
```

**3) Add a coprocessor specification to the table via a "coprocessor" attribute. The coprocessor spec may (and usually will)
also include the name of the Omid commit table**

```
alter 'MY_TX_TABLE', METHOD => 'table_att', 'coprocessor'=>'|org.apache.omid.transaction.OmidSnapshotFilter|1001|omid.committable.tablename=OMID_COMMIT_TABLE'
```

**4) Re-enable the table**

```
enable 'MY_TX_TABLE'
```

**5) In hbase-site.xml enable server side filtering. The omid client should have this in its classpath**
```
    <property>
        <name>omid.server.side.filter</name>
        <value>true</value>
    </property>
```
