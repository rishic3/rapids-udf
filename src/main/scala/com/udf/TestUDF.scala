package com.udf

import ai.rapids.cudf._
import com.nvidia.spark.RapidsUDF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class TestUDF extends Function1[String, String] with RapidsUDF with Serializable {
  override def apply(v1: String): String = {
    "cpu_processed"
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    ColumnVector.fromStrings(Array.fill(numRows)("gpu_processed"): _*)
  }
}

object TestUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("leak repro test")
      .master("local[1]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.driver.extraJavaOptions", "-Dai.rapids.refcount.debug=true -ea")
      .config("spark.executor.extraJavaOptions", "-Dai.rapids.refcount.debug=true -ea")
      .config("spark.rapids.memory.gpu.pool", "NONE")
      .getOrCreate()
    
    try {
      val testData = (0 until 100).map(i => Row(s"test_row_$i"))
      val schema = StructType(Array(StructField("input", StringType, true)))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
      
      println("test df:")
      df.show(5)

      spark.udf.register("test_udf", new TestUDF())

      df.createOrReplaceTempView("test_table")
      val resultDf = spark.sql("SELECT input, test_udf(input) as result FROM test_table")
      resultDf.collect()

      println("results:")
      resultDf.show(5)

    } finally {
      spark.stop()
    }
  }
}