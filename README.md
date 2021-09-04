<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Doris Connectors
Refactor Doris Connectors from [Apache Doris Extension](https://github.com/apache/incubator-doris/tree/master/extension).
Extract the read and stream load common code to module doris-connector-base, 
and improve sink performance with flink streaming/batch side.

## 1. License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## 2. Build
 Depend on doris-fe-common module to read data through thrift service, need to pre-build [Apache Doris FE](https://github.com/apache/incubator-doris/tree/master/fe).
### 2.1 Scala-2.11
```bash
mvn clean install -DskipTests
``` 
### 2.2 Scala-2.12
```bash
mvn clean install -DskipTests -PScala-2.12
``` 

## 3. Maven projects
For Maven projects, add the following dep to your pom.
```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>doris-connector-flink</artifactId>
    <version>${doris-connector-flink.version}</version>
</dependency>
```
```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>doris-connector-spark</artifactId>
    <version>${doris-connector-spark.version}</version>
</dependency>
```

## 4. Install
Copy doris-connector-flink-*.jar/doris-connector-spark-*.jar to flink/spark libs folder

## 5.Use case

### 5.1 Flink
More flink sql table properties, refer the class org.apache.doris.connectors.flink.table.DorisOptions

#### Flink Sql (doris)
```sql
create table supplier_sink(
  s_suppkey int,
  s_name string,
  s_address string,
  s_city string,
  s_nation string,
  s_region string,
  s_phone string,
  PRIMARY KEY (s_suppkey) NOT ENFORCED
) WITH (
'connector' = 'doris',
'doris.fenodes' = '$fenodes',
'doris.username' = '$username',
'doris.password' = '$password',
'doris.table.identifier' = 'poc.dim_supplier_sink',
'doris.sink.batch.size' = '1000'
);

insert into supplier_sink
values
(100000, 'flink', 'flink', 'shanghai', 'china', 'asian', '123456'),
(100001, 'sql', 'sql', 'shanghai', 'china', 'asian', '123456')

select * from supplier_sink
```

#### Flink Source
```scala
val sourceTableDdl =
  s"""
     |create table supplier(
     |  s_suppkey int,
     |  s_name string,
     |  s_address string,
     |  s_city string,
     |  s_nation string,
     |  s_region string,
     |  s_phone string,
     |  PRIMARY KEY (s_suppkey) NOT ENFORCED
     |) WITH (
     |'connector' = 'doris',
     |'doris.fenodes' = '$fenodes',
     |'doris.username' = '$username',
     |'doris.password' = '$password',
     |'doris.table.identifier' = 'poc.dim_supplier'
     |)
     |""".stripMargin

val sinkTableDdl =
  s"""
     |create table supplier_sink(
     |  s_suppkey int,
     |  s_name string,
     |  s_address string,
     |  s_city string,
     |  s_nation string,
     |  s_region string,
     |  s_phone string,
     |  PRIMARY KEY (s_suppkey) NOT ENFORCED
     |) WITH (
     |'connector' = 'doris',
     |'doris.fenodes' = '$fenodes',
     |'doris.username' = '$username',
     |'doris.password' = '$password',
     |'doris.table.identifier' = 'poc.dim_supplier_sink',
     |'doris.sink.buffer-flush.max-rows' = '10000',
     |'doris.sink.buffer-flush.interval' = '1s',
     |'doris.sink.max-retries' = '3',
     |'doris.sink.file.format.type' = 'csv'
     |)
     |""".stripMargin

val envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env, envSettings)
env.setParallelism(1)

tableEnv.executeSql(sourceTableDdl)
tableEnv.executeSql("select * from supplier").print()
```

#### Flink Sink
```scala
tableEnv.executeSql(sourceTableDdl)
tableEnv.executeSql(sinkTableDdl)
tableEnv.executeSql("insert into supplier_sink select * from supplier").print()
```

### 5.2 Spark
```scala
val spark = SparkSession
  .builder()
  .appName(getClass.getSimpleName)
  .master("local[2]")
  .getOrCreate()

// read
val sourceMaps = Map[String, String](
  "doris.fenodes" -> DORIS_FENODES,
  "doris.user" -> DORIS_USER,
  "doris.password" -> "",
  "doris.table.identifier" -> "poc.dim_supplier",
  "doris.request.timeout.ms" -> "60000",
  "doris.exec.mem.limit" -> "10000000000")
spark.read.format("doris").options(sourceMaps).load().show(200)

// write
val sinkMaps = Map[String, String](
  "doris.fenodes" -> DORIS_FENODES,
  "doris.user" -> DORIS_USER,
  "doris.password" -> "",
  "doris.table.identifier" -> "poc.dim_supplier_sink",
  "doris.request.timeout.ms" -> "60000",
  "doris.exec.mem.limit" -> "10000000000")
spark
  .sql("select 1 as s_suppkey, 'Supplier#000000001' as s_name, 'sdrGnXCDRcfriBvY0KL,i' as s_address, 'PERU     0' as s_city, 'PERU' as s_nation, 'AMERICA' as s_region, '27-989-741-2988' as s_phone")
  .write.format("doris").mode("append").options(sinkMaps).save()
```