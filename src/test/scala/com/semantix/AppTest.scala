package com.smtx

import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}
/* Spark */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.LocalSparkContext
/* Spark SQL */
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Column, DataFrame}

import com.smtx._

trait SharedSparkContext extends BeforeAndAfterAll {
    self:Suite =>
        @transient private var _sc:SparkContext = _
        @transient private var _sqlContext: SQLContext = _
    
        def sc: SparkContext = _sc
        def sqlContext: SQLContext = _sqlContext

        var conf = new SparkConf(false)
        
        override def beforeAll(){
            _sc = new SparkContext("local[2]", "Test Suites", conf)
            _sc.setLogLevel("ERROR") 
            _sqlContext = new SQLContext(_sc)
            super.beforeAll()
        }
        
        override def afterAll(){
            try {
                LocalSparkContext.stop(_sc)
                _sc = null
            } finally {
                super.afterAll()
            }
        }
}

class AppTest extends FunSuite with SharedSparkContext{

    test("Some test") { 
        val textFile = sc.textFile("src/test/mock/test.txt")
        val llist = textFile.collect()
        assert(llist(0) === "todo")
    }

    test("[Transform][def:roundAt] Unit test") {
        val T = new Transform2()
        val testValue = 1.95834793487.toFloat
        val expected = 1.96.toFloat
        val teste = T.roundAt(2)(testValue)
        assert(teste === expected)
    }

    test("[Transform][def:get] IndexOutOfBoundsException") {
        val T = new Transform2()
        val testValue = "batatabatatabatata"
        val thrown = intercept[IndexOutOfBoundsException] {
            val teste = T.get(testValue)
        }
        assert(thrown.getMessage === "[class:Transform][def:get] record: batatabatatabatata")
    }
    
    test("[Transform][def:get] Stress test") {
        val T = new Transform2()
        val testValue = "batatabatatabatata"
        val expected = 
            Row(
                0, 0, 0, 0, 0, "", 0, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0L, 0.0.toFloat, 0L, 0.0.toFloat, 0L, 0.0.toFloat, 
                0, "", "", "", 0L
            )
        val thrown = intercept[IndexOutOfBoundsException] {
            val teste = T.get(testValue)
            assert(teste === expected)
        }
    }
    
    test("[Transform][def:write] Unit Test") {
        val T = new Transform2()
        val textFile = sc.textFile("src/test/mock/ac01_teste_m20180510000000.txt")
        val llist = 
            textFile
            .map{
              record => 
                Row.fromSeq(T.foo(record))
            }
        
        val cadastro_schema =
            StructType( 
              Seq( 
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

        val kuduDF = sqlContext.createDataFrame(llist, cadastro_schema)
        T.write(kuduDF, "src/test/mock/expected", "cadastro_expected")
    }   
    
    test("[Transform][def:write] LabHom Test") {
        val T = new Transform2()
        val textFile = sc.textFile("src/test/mock/LabHom/ac01_loja1359_m20180516000000.txt")
        val timestamp = 20180516000000L
        val llist = 
            textFile
            .map{
              record => 
                Row.fromSeq(T.foo(record) :+ timestamp)
            }
        
        val cadastro_schema =
            StructType( 
              Seq( 
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
                StructField(name = "indicador_exclusao", dataType = StringType, nullable = true),
                StructField(name = "rtimestamp", dataType = LongType, nullable = true)
              ) 
            )

        val kuduDF = sqlContext.createDataFrame(llist, cadastro_schema)
        T.write(kuduDF, "src/test/mock/LabHom/expected", "cadastro_expected")
    }
}
