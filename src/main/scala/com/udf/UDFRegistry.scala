package com.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import com.udf._

object UDFRegistry {
    def registerUDF(spark: SparkSession, udfName: String): Unit = {
        spark.udf.register(udfName, new TestUDF())
    }
}
