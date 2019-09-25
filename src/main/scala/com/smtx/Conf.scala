package com.smtx

//=============================================================================
//	Conf object
//      - Get configuration properties from hdfs .conf file
//=============================================================================

// standard library imports
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Properties
import java.io.InputStreamReader
// related third party imports
/* Hadoop */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream}
/* Spark */
import org.apache.spark.SparkContext
// local application/library specific imports


class MyConfig(fileNameOption: Option[String] = None) {
     
  val config = fileNameOption.fold(
                  ifEmpty = ConfigFactory.load() )(
                  file => ConfigFactory.load(file) )
 
  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}

object Conf {
    
    val myConfig = new MyConfig()
    
    def getHDFSfile(value:String):FSDataInputStream = {
        val sc = SparkContext.getOrCreate()
        val hadoopConfig: Configuration = sc.hadoopConfiguration
        val fs: FileSystem = FileSystem.get(hadoopConfig)
        fs.open(new Path(value))
    }
    
    def readConfigFromHDFS(value:String):Config = {
        //read config from hdfs    
        val reader = new InputStreamReader(getHDFSfile(value))
        val config = try {
          ConfigFactory.parseReader(reader)
        } finally {
          reader.close()
        }
        return config
    }
}
