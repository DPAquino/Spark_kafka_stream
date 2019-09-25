package com.smtx
//=============================================================================
//  Spark App to ingest data for AC01.CADASTRO
//    This app receive 4 arguments
//    1) String : the path to checkpoint in the hdfs ["/smtx/ac01/cadastro/checkpoint"]
//=============================================================================

// standard library imports
import scala.util.parsing.json.JSON
// related third party imports
/* Compiled config file */
import com.typesafe.config.ConfigFactory
/* Logger */
import org.slf4j.{Logger, LoggerFactory}
/* Spark */
import org.apache.spark.{SparkConf, SparkContext}
/* Spark SQL */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Column, DataFrame}
/* Spark Streaming*/
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
/* Kafka */
import kafka.serializer.StringDecoder
/* Kudu */
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduPredicate
import org.apache.kudu.client.KuduScanner
import org.apache.kudu.client.KuduSession
import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.Upsert
/* Hive */
import org.apache.spark.sql.hive.HiveContext
// local application/library specific imports
import com.smtx._

object App {

    def main(args : Array[String]) {
        val log = LoggerFactory.getLogger("GCA360")
        //log.info("[*] args(0):" + args(0))

        // Context initialization
        val conf = new SparkConf()
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.backpressure.pid.minRate", Conf.myConfig.envOrElseConfig("spark.streaming.backpressure.pid.minRate"))
        .set("spark.streaming.backpressure.initialRate", Conf.myConfig.envOrElseConfig("spark.streaming.backpressure.initialRate"))
        .set("spark.streaming.kafka.maxRatePerPartition", Conf.myConfig.envOrElseConfig("spark.streaming.kafka.maxRatePerPartition"))
        .set("spark.streaming.receiver.maxRate", Conf.myConfig.envOrElseConfig("spark.streaming.receiver.maxRate"))

        val sc = SparkContext.getOrCreate(conf)

        // Cluster conf
        val conf_kuduMasters = Conf.myConfig.envOrElseConfig("kudu.masters")
        val conf_zookeeperQuorum = Conf.myConfig.envOrElseConfig("zookeeper.quorum")
        val conf_kafkaBrokers = Conf.myConfig.envOrElseConfig("kafka.brokers")

        // Application conf
        val conf_streamingInterval = Conf.myConfig.envOrElseConfig("spark.streaming.interval")
        val conf_kuduTable = Conf.myConfig.envOrElseConfig("kudu.table")
        val conf_kuduDB = Conf.myConfig.envOrElseConfig("kudu.database")
        val conf_kafkaTopic = Conf.myConfig.envOrElseConfig("kafka.topic")
        val conf_kafkaGroup = Conf.myConfig.envOrElseConfig("kafka.group")
        val conf_kafkaOffset = Conf.myConfig.envOrElseConfig("kafka.offset")
        val conf_table = "impala::"+conf_kuduDB+"."+conf_kuduTable

        log.info("[*] conf_kuduMasters:" + conf_kuduMasters)
        log.info("[*] conf_zookeeperQuorum:" + conf_zookeeperQuorum)
        log.info("[*] conf_kafkaBrokers:" + conf_kafkaBrokers)
        log.info("[*] conf_streamingInterval:" + conf_streamingInterval)
        log.info("[*] conf_kafkaTopic:" + conf_kafkaTopic)
        log.info("[*] conf_kafkaGroup:" + conf_kafkaGroup)
        log.info("[*] conf_kafkaOffset:" + conf_kafkaOffset)
        log.info("[*] conf_table:"+ conf_table)

        val ssc = new StreamingContext(sc, Seconds(conf_streamingInterval.toLong)) 
        val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
        import sqlContext.implicits._

        log.info("[*] Lets Get Started!")

        // Spark streamming checkpoint
        //ssc.checkpoint(args(0))

        // Create an instance of a KuduContext
        val kuduContext = new KuduContext(conf_kuduMasters, ssc.sparkContext)
        val hiveContext = new HiveContext(ssc.sparkContext)

        // Kafka parameters
        val kafkaParams: Map[String, String] =
            Map(  
                "metadata.broker.list" -> conf_kafkaBrokers,
                "zookeeper.connect" -> conf_zookeeperQuorum,
                "group.id" -> conf_kafkaGroup,
                "auto.offset.reset" -> conf_kafkaOffset
            )

        log.info("[*] Iniciando streaming - gpa!")

        // Set log to show just ERRORs
        //ssc.sparkContext.setLogLevel("ERROR") 

        // Create direct kafka stream with brokers and topics
        val dstream1 =   
            KafkaUtils
            .createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc, kafkaParams, Set(conf_kafkaTopic)
            )

        // Main routine
        dstream1
        .map{r => r._2}
        .foreachRDD{ 
            rdd =>    
                try{
                
                    val schema = 
                        StructType( 
                            Seq( 
                                StructField(name = "rtimestamp", dataType = LongType, nullable = true),
                                StructField(name = "loja", dataType = IntegerType, nullable = true), 
                                StructField(name = "rv", dataType = IntegerType, nullable = true),
                                StructField(name = "plu", dataType = IntegerType, nullable = true),
                                StructField(name = "fabricante", dataType = IntegerType, nullable = true),
                                StructField(name = "produto", dataType = IntegerType, nullable = true),
                                StructField(name = "politica_preco", dataType = StringType, nullable = true),
                                StructField(name = "data_fora_linha", dataType = LongType, nullable = true),
                                StructField(name = "preco_venda_1_atu", dataType = FloatType, nullable = true),
                                StructField(name = "data_validade_preco_venda_1_atu", dataType = LongType, nullable = true),
                                StructField(name = "preco_venda_2_ant_1", dataType = FloatType, nullable = true),
                                StructField(name = "dt_validade_preco_venda_2_ant_1", dataType = LongType, nullable = true),
                                StructField(name = "preco_venda_3_ant_2", dataType = FloatType, nullable = true),
                                StructField(name = "dt_validade_preco_venda_3_ant_2", dataType = LongType, nullable = true),
                                StructField(name = "preco_sugestao_1_atu", dataType = FloatType, nullable = true),
                                StructField(name = "dt_validade_preco_sugestao_1_atu", dataType = LongType, nullable = true),
                                StructField(name = "preco_sugestao_2_ant_1", dataType = FloatType, nullable = true),
                                StructField(name = "dt_validade_preco_sugestao_2_ant_1", dataType = LongType, nullable = true),
                                StructField(name = "preco_sugestao_3_ant_2", dataType = FloatType, nullable = true),
                                StructField(name = "dt_validade_preco_sugestao_3_ant_2", dataType = LongType, nullable = true),
                                StructField(name = "fator_calculo", dataType = FloatType, nullable = true),
                                StructField(name = "plu_original", dataType = IntegerType, nullable = true),
                                StructField(name = "peso_variavel", dataType = StringType, nullable = true),
                                StructField(name = "filler", dataType = StringType, nullable = true),
                                StructField(name = "indicador_exclusao", dataType = StringType, nullable = true)
                            ) 
                        )                        
                   
                    val T = new Transform2()
                    // Convert RDD[String] to DataFrame
                    val streamDF = 
                        rdd
                        .map{
                            record => 
                                T.get(record)
                        }
                   
                    val kuduDF = sqlContext.createDataFrame(streamDF, schema)
                    kuduContext.upsertRows(kuduDF, conf_table)

                }
                catch {
                    case e: Exception => {
                        log.error("[dstream1] " + rdd.toString)
                        log.error(e.printStackTrace.toString)
                    }
                }
        }

        // Run streaming
        ssc.start()
        ssc.awaitTermination()

    }
}
