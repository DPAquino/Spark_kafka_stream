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

class Transform2() extends Serializable{

    def roundAt(x:Int)(n:Float):Float = {
        try{ BigDecimal(n).setScale(x, BigDecimal.RoundingMode.HALF_UP).toFloat }
        catch { case e: Exception => { 0.0f } }
    }
    def getInt(x:String):Int = {
        try{ x.toInt } 
        catch { case e: Exception => { 0 } }
    }
    def getFloat(x:String):Float = {
        try{ x.toFloat } 
        catch { case e: Exception => { 0.0f } }
    }
    def getLong(x:String):Long = {
        try{ x.toLong }
        catch { case e: Exception => { 0L } }
    }
    
    def foo(item:String):Seq[_] = {
        try {
            val registro = """^(.{4})(.{2})(.{7})(.{5})(.{5})(.{1})(.{8})(.{11})(.{8})(.{11})(.{8})(.{11})(.{8})(.{11})(.{8})(.{11})(.{8})(.{11})(.{8})(.{7})(.{7})(.{1})(.*)(.{1})""".r
            val bar = registro.findFirstMatchIn(item).toList(0)
            Seq(
          /*4*/ getInt(bar.group(1)),
          /*2*/ getInt(bar.group(2)),
          /*7*/ getInt(bar.group(3)),
          /*5*/ getInt(bar.group(4)),
          /*5*/ getInt(bar.group(5)),
          /*1*/ bar.group(6).trim,
          /*8*/ getLong(bar.group(7)),
          /*11*/roundAt(2)(getFloat(bar.group(8))/100),
          /*8*/ getLong(bar.group(9)),
          /*11*/roundAt(2)(getFloat(bar.group(10))/100),
          /*8*/ getLong(bar.group(11)),
          /*11*/roundAt(2)(getFloat(bar.group(12))/100),
          /*8*/ getLong(bar.group(13)),
          /*11*/roundAt(2)(getFloat(bar.group(14))/100),
          /*8*/ getLong(bar.group(15)),
          /*11*/roundAt(2)(getFloat(bar.group(16))/100),
          /*8*/ getLong(bar.group(17)),
          /*11*/roundAt(2)(getFloat(bar.group(18))/100),
          /*8*/ getLong(bar.group(19)),
          /*7*/ roundAt(2)(getFloat(bar.group(20))/10000),
          /*7*/ getInt(bar.group(21)),
          /*1*/ bar.group(22).trim,
          /***/ bar.group(23).trim,
          /*1*/ bar.group(24).trim
            )
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
                0, 0, 0, 0, 0, "", 0L, 0.0f, 
                0L, 0.0f, 0L, 0.0f, 0L, 0.0f, 
                0L, 0.0f, 0L, 0.0f, 0L, 0.0f, 
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
                0L, 0, 0, 0, 0, 0, "", 0L, 0.0f, 
                0L, 0.0f, 0L, 0.0f, 0L, 0.0f, 
                0L, 0.0f, 0L, 0.0f, 0L, 0.0f, 
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
