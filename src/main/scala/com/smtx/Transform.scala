package com.smtx

// standard library imports
import scala.math.BigDecimal
// related third party imports
/* Logger */
import org.slf4j.{Logger, LoggerFactory}
/* Spark SQL */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Column, DataFrame}

class Transform() extends Serializable{

    def roundAt(x:Int)(n:Option[Any]) = n match {
        case Some(n:Int) => BigDecimal(n).setScale(x, BigDecimal.RoundingMode.HALF_UP).toInt
        case Some(n:Float) => BigDecimal(n).setScale(x, BigDecimal.RoundingMode.HALF_UP).toFloat
        case Some(n:Double) => BigDecimal(n).setScale(x, BigDecimal.RoundingMode.HALF_UP).toDouble
        case _  => throw new IllegalArgumentException("[class:Transform][def:roundAt] Argument 'n' with datatype not numeric!");
    }
    
    def foo(item:String):Seq[_] = {
        try {
            val bar = Seq(
                item.substring(0, 4).trim.toInt,
                item.substring(4, 6).trim.toInt,
                item.substring(6, 13).trim.toInt,
                item.substring(13, 18).trim.toInt,
                item.substring(18, 23).trim.toInt,
                item.substring(23, 24).trim,
                item.substring(24, 32).trim.toLong,
                roundAt(2)(Some(item.substring(32, 43).trim.toFloat/100)),
                item.substring(43, 51).trim.toLong,
                roundAt(2)(Some(item.substring(51, 62).trim.toFloat/100)),
                item.substring(62, 70).trim.toLong,
                roundAt(2)(Some(item.substring(70, 81).trim.toFloat/100)),
                item.substring(81, 89).trim.toLong,
                roundAt(2)(Some(item.substring(89, 100).trim.toFloat/100)),
                item.substring(100, 108).trim.toLong,
                roundAt(2)(Some(item.substring(108, 119).trim.toFloat/100)),
                item.substring(119, 127).trim.toLong,
                roundAt(2)(Some(item.substring(127, 138).trim.toFloat/100)),
                item.substring(138, 146).trim.toLong,
                roundAt(4)(Some(item.substring(146, 153).trim.toFloat/10000)),
                item.substring(153, 160).trim.toInt,
                item.substring(160, 161).trim,
                item.substring(161, item.length()-2).trim,
                item.substring(item.length()-2, item.length()-1).trim
            )
            //LoggerFactory.getLogger("myLogger").info(bar.toString)
            bar
        }
        catch {
            case e: NumberFormatException => {
                throw new NumberFormatException("[class:Transform][def:foo] Failed to convert to number. " + e.getMessage()) 
            }
            case e: StringIndexOutOfBoundsException => {
                throw new StringIndexOutOfBoundsException("[class:Transform][def:foo] String with wrong length. " + e.getMessage()) 
            }
            case e: Exception => {
                LoggerFactory.getLogger("myLogger").error(e.printStackTrace.toString)            
            }
            Seq(
                0, 0, 0, 0, 0, "", 0L, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0, "", "", ""
            )
        }
    }
    
    def get(record:String):Row = {
        try {
            //ac01_dsdpm66o_hadoop_m20180412202535|...
            val registro = """^ac01_(.*)_m(.{14})\|(.{400})""".r
            val timestamp = Seq(registro.findFirstMatchIn(record).toList(0).group(2).trim.toLong)
            val item = registro.findFirstMatchIn(record).toList(0).group(3)
            Row.fromSeq(timestamp ++ foo(item))
        }
        catch {
            case e: IndexOutOfBoundsException => {
                throw new IndexOutOfBoundsException("[class:Transform][def:get] record: " + record)
            }
            case e: Exception => {
                LoggerFactory.getLogger("myLogger").error(e.printStackTrace.toString)
            }
            Row(
                0L, 0, 0, 0, 0, 0, "", 0L, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0, "", "", ""
            )
        }
    }
    
    def write(df:DataFrame, pathFile:String, nameFile:String) = {
        df.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .mode("overwrite")
        .save(pathFile+"/"+nameFile+".csv")
    }
    
}
