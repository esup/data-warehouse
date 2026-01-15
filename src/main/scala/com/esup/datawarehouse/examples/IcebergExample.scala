package com.esup.datawarehouse.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Example Iceberg operations for data warehouse development
 */
object IcebergExample {

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    try {
      println("=" * 80)
      println("Iceberg Operations Example")
      println("=" * 80)

      // Example 1: Create database
      createDatabase(spark)

      // Example 2: Create Iceberg table
      createIcebergTable(spark)

      // Example 3: Insert sample data
      insertSampleData(spark)

      // Example 4: Query data
      queryData(spark)

      // Example 5: Update data
      updateData(spark)

      // Example 6: Time travel query
      timeTravelQuery(spark)

      // Example 7: Show table metadata
      showTableMetadata(spark)

      println("\n" + "=" * 80)
      println("All examples completed successfully!")
      println("=" * 80)

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * Create Spark Session with Iceberg support
   */
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("iceberg-example")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri", "thrift://p-bdm-app03:9083")
      .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://p-bdm-app02:8020/user/hive/warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Create a database for examples
   */
  def createDatabase(spark: SparkSession): Unit = {
    println("\n>>> Creating database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_examples")
    println("Database 'iceberg_examples' created")
  }

  /**
   * Create an Iceberg table
   */
  def createIcebergTable(spark: SparkSession): Unit = {
    println("\n>>> Creating Iceberg table...")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS iceberg_examples.sales_data (
        transaction_id BIGINT,
        product_name STRING,
        quantity INT,
        price DECIMAL(10, 2),
        transaction_date DATE,
        created_at TIMESTAMP
      )
      USING iceberg
      PARTITIONED BY (days(transaction_date))
      TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.metadata.compression-codec' = 'gzip'
      )
    """)
    println("Table 'iceberg_examples.sales_data' created")
  }

  /**
   * Insert sample data into the table
   */
  def insertSampleData(spark: SparkSession): Unit = {
    println("\n>>> Inserting sample data...")
    
    import spark.implicits._
    
    val sampleData = Seq(
      (1L, "Laptop", 2, 1200.00, "2024-01-15"),
      (2L, "Mouse", 5, 25.50, "2024-01-15"),
      (3L, "Keyboard", 3, 75.00, "2024-01-16"),
      (4L, "Monitor", 1, 350.00, "2024-01-16"),
      (5L, "USB Cable", 10, 5.99, "2024-01-17")
    )

    val df = sampleData.toDF("transaction_id", "product_name", "quantity", "price", "transaction_date_str")
      .withColumn("transaction_date", to_date(col("transaction_date_str")))
      .withColumn("created_at", current_timestamp())
      .drop("transaction_date_str")

    df.writeTo("iceberg_examples.sales_data").append()
    
    println(s"Inserted ${sampleData.size} records")
  }

  /**
   * Query data from the table
   */
  def queryData(spark: SparkSession): Unit = {
    println("\n>>> Querying data...")
    
    val df = spark.sql("""
      SELECT transaction_id, product_name, quantity, price, transaction_date
      FROM iceberg_examples.sales_data
      ORDER BY transaction_date, transaction_id
    """)
    
    df.show(false)
  }

  /**
   * Update data in the table
   */
  def updateData(spark: SparkSession): Unit = {
    println("\n>>> Updating data (Iceberg MERGE)...")
    
    spark.sql("""
      MERGE INTO iceberg_examples.sales_data AS target
      USING (
        SELECT 1 AS transaction_id, 1250.00 AS new_price
      ) AS source
      ON target.transaction_id = source.transaction_id
      WHEN MATCHED THEN UPDATE SET target.price = source.new_price
    """)
    
    println("Updated transaction_id 1 with new price")
  }

  /**
   * Time travel query to see historical data
   */
  def timeTravelQuery(spark: SparkSession): Unit = {
    println("\n>>> Time travel query...")
    
    // Get snapshots
    val snapshots = spark.sql("""
      SELECT snapshot_id, committed_at, operation
      FROM iceberg_examples.sales_data.snapshots
      ORDER BY committed_at DESC
      LIMIT 5
    """)
    
    println("Recent snapshots:")
    snapshots.show(false)
  }

  /**
   * Show table metadata and statistics
   */
  def showTableMetadata(spark: SparkSession): Unit = {
    println("\n>>> Table metadata...")
    
    // Show table properties
    println("Table properties:")
    spark.sql("SHOW TBLPROPERTIES iceberg_examples.sales_data").show(false)
    
    // Show table files
    println("\nTable files:")
    val files = spark.sql("""
      SELECT file_path, file_size_in_bytes, record_count
      FROM iceberg_examples.sales_data.files
      LIMIT 5
    """)
    files.show(false)
  }
}
