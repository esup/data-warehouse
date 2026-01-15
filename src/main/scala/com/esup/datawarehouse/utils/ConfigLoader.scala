package com.esup.datawarehouse.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Configuration utility for loading Hadoop and Hive configurations
 */
object ConfigLoader {

  /**
   * Load Hadoop configuration from resource files
   */
  def loadHadoopConfiguration(): Configuration = {
    val conf = new Configuration()
    
    // Load configuration files from resources
    val resourcePath = getClass.getClassLoader.getResource("hadoop-conf")
    if (resourcePath != null) {
      val confPath = resourcePath.getPath
      
      // Load core-site.xml
      val coreSitePath = new Path(s"$confPath/core-site.xml")
      conf.addResource(coreSitePath)
      
      // Load hdfs-site.xml
      val hdfsSitePath = new Path(s"$confPath/hdfs-site.xml")
      conf.addResource(hdfsSitePath)
      
      // Load yarn-site.xml
      val yarnSitePath = new Path(s"$confPath/yarn-site.xml")
      conf.addResource(yarnSitePath)
      
      println(s"Hadoop configuration loaded from: $confPath")
    } else {
      println("Warning: hadoop-conf directory not found in resources")
    }
    
    conf
  }

  /**
   * Load Hive configuration
   */
  def loadHiveConfiguration(): Configuration = {
    val conf = loadHadoopConfiguration()
    
    val resourcePath = getClass.getClassLoader.getResource("hadoop-conf/hive-site.xml")
    if (resourcePath != null) {
      val hiveSitePath = new Path(resourcePath.getPath)
      conf.addResource(hiveSitePath)
      println(s"Hive configuration loaded from: ${resourcePath.getPath}")
    } else {
      println("Warning: hive-site.xml not found in resources")
    }
    
    conf
  }

  /**
   * Print configuration summary
   */
  def printConfiguration(conf: Configuration): Unit = {
    println("\n" + "=" * 80)
    println("Configuration Summary")
    println("=" * 80)
    println(s"fs.defaultFS: ${conf.get("fs.defaultFS", "NOT SET")}")
    println(s"yarn.resourcemanager.hostname: ${conf.get("yarn.resourcemanager.hostname", "NOT SET")}")
    println(s"hive.metastore.uris: ${conf.get("hive.metastore.uris", "NOT SET")}")
    println("=" * 80 + "\n")
  }
}
