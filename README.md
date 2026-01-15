# Data Warehouse Project

A Maven-based Scala project for data warehouse development with Apache Spark, Hive, and Iceberg integration.

## Project Overview

This project provides a development environment for building data warehouse applications that can connect to:
- **Hadoop HDFS** (version 3.3.6)
- **Apache Hive** (version 3.1.3)
- **Apache Spark** (version 3.3.4)
- **Apache Iceberg** (version 1.6.1)

## Technology Stack

- **Java**: 1.8
- **Scala**: 2.12.21
- **Build Tool**: Maven
- **Big Data Framework**: Spark 3.3.4
- **Data Catalog**: Hive 3.1.3
- **Table Format**: Iceberg 1.6.1

## Project Structure

```
data-warehouse/
├── pom.xml                                 # Maven project configuration
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/esup/datawarehouse/
│   │   │       ├── SparkJobApp.scala      # Main Spark application
│   │   │       └── utils/
│   │   │           └── ConfigLoader.scala  # Configuration utilities
│   │   └── resources/
│   │       ├── hadoop-conf/                # Hadoop cluster configurations
│   │       │   ├── core-site.xml
│   │       │   ├── hdfs-site.xml
│   │       │   ├── yarn-site.xml
│   │       │   └── hive-site.xml
│   │       ├── spark-defaults.conf         # Spark default configurations
│   │       └── log4j.properties            # Logging configuration
│   └── test/
│       └── scala/                          # Test source directory
└── README.md
```

## Server Configuration

The project is configured to connect to the following cluster:

### HDFS
- **NameNode**: hdfs://p-bdm-app02:8020
- **Replication Factor**: 3

### YARN
- **Resource Manager**: p-bdm-app03

### Hive Metastore
- **URI**: thrift://p-bdm-app03:9083
- **Warehouse**: hdfs://p-bdm-app02:8020/user/hive/warehouse

### Spark
- **Master**: yarn
- **Deploy Mode**: cluster
- **History Server**: p-bdm-app01:18080

## Prerequisites

Before building and running this project, ensure you have:

1. **Java JDK 1.8** installed
2. **Maven 3.x** installed
3. **Network access** to the Hadoop cluster servers:
   - p-bdm-app01, p-bdm-app02, p-bdm-app03
   - p-bdm-mysql01

## Building the Project

### Compile the project

```bash
mvn clean compile
```

### Package the project

```bash
mvn clean package
```

This will create:
- `target/data-warehouse-1.0-SNAPSHOT.jar` - Main JAR
- `target/data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar` - JAR with all dependencies
- `target/data-warehouse-1.0-SNAPSHOT-shaded.jar` - Shaded JAR

## Running in IntelliJ IDEA

### Setup Steps

1. **Import the project**:
   - Open IntelliJ IDEA
   - File → Open → Select the project directory
   - Wait for Maven to download dependencies

2. **Configure Scala SDK**:
   - File → Project Structure → Global Libraries
   - Add Scala SDK 2.12.21 if not already present

3. **Configure Run Configuration**:
   - Run → Edit Configurations → Add New → Application
   - Main class: `com.esup.datawarehouse.SparkJobApp`
   - VM options (for local testing):
     ```
     -Dspark.master=local[*]
     -Dspark.sql.warehouse.dir=/tmp/spark-warehouse
     ```

4. **For cluster connection**:
   - Ensure network connectivity to cluster servers
   - Update `/etc/hosts` with cluster server IPs if needed:
     ```
     <IP> p-bdm-app01
     <IP> p-bdm-app02
     <IP> p-bdm-app03
     <IP> p-bdm-mysql01
     ```

## Submitting to Spark Cluster

### Submit the application to YARN cluster

```bash
spark-submit \
  --class com.esup.datawarehouse.SparkJobApp \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.app.name=dp-spark-job \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1,org.apache.iceberg:iceberg-hive-runtime:1.6.1 \
  target/data-warehouse-1.0-SNAPSHOT.jar
```

### Submit with resource configuration

```bash
spark-submit \
  --class com.esup.datawarehouse.SparkJobApp \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 3 \
  --conf spark.app.name=dp-spark-job \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1,org.apache.iceberg:iceberg-hive-runtime:1.6.1 \
  target/data-warehouse-1.0-SNAPSHOT.jar
```

## Development Guide

### Creating New Spark Jobs

1. Create a new Scala object in `src/main/scala/com/esup/datawarehouse/`
2. Extend or copy the pattern from `SparkJobApp.scala`
3. Use the SparkSession builder with the required Iceberg configurations
4. Build and submit to the cluster

Example:

```scala
package com.esup.datawarehouse

import org.apache.spark.sql.SparkSession

object MyCustomJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my-custom-job")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .enableHiveSupport()
      .getOrCreate()

    // Your data processing logic here
    
    spark.stop()
  }
}
```

### Working with Iceberg Tables

```scala
// Create an Iceberg table
spark.sql("""
  CREATE TABLE IF NOT EXISTS my_database.my_iceberg_table (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(created_at))
""")

// Insert data
spark.sql("""
  INSERT INTO my_database.my_iceberg_table 
  VALUES (1, 'example', current_timestamp())
""")

// Query data
val df = spark.sql("SELECT * FROM my_database.my_iceberg_table")
df.show()
```

## Configuration Files

All configuration files are located in `src/main/resources/`:

- **hadoop-conf/core-site.xml**: HDFS and Hadoop core configuration
- **hadoop-conf/hdfs-site.xml**: HDFS-specific settings
- **hadoop-conf/yarn-site.xml**: YARN resource manager settings
- **hadoop-conf/hive-site.xml**: Hive metastore configuration
- **spark-defaults.conf**: Spark default configurations including Iceberg settings
- **log4j.properties**: Logging configuration

## Troubleshooting

### Common Issues

1. **Connection refused to Hive Metastore**:
   - Check network connectivity to p-bdm-app03:9083
   - Verify Hive metastore service is running

2. **HDFS connection issues**:
   - Verify HDFS namenode is accessible at p-bdm-app02:8020
   - Check firewall rules

3. **Class not found errors**:
   - Ensure all dependencies are included in the JAR
   - Use the jar-with-dependencies or shaded JAR for submission

4. **Kerberos authentication** (if enabled):
   - Configure `spark.yarn.keytab` and `spark.yarn.principal`
   - Add Kerberos configuration to resources

## License

This project is for internal use in the data warehouse development environment.

## Contact

For questions or issues, please contact the data platform team.