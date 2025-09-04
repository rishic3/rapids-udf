# Failed test
# Generated on: 2025-09-03 18:05:26
# ===================================================
import logging
from typing import Any, Callable, List, Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from scala_registration_utils import registerScalaUDF

import argparse
from pyspark.errors.exceptions.base import PySparkAssertionError
from pyspark.testing import assertSchemaEqual, assertDataFrameEqual

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)


def create_test_data(spark: SparkSession) -> DataFrame:
    # TODO
    test_data = None
    schema = None

    return spark.createDataFrame(test_data, schema)

def register_udf(
    spark: SparkSession,
    udf_name: str,
    class_path: str,
) -> None:
    registerScalaUDF(spark, udf_name, class_path)

def execute_udf(spark: SparkSession, udf_name: str, test_df: DataFrame) -> DataFrame:
    test_df.createOrReplaceTempView("test_table")

    # TODO
    udf_result_df = spark.sql("")
    return udf_result_df

def run_test(spark: SparkSession) -> None:
    test_df = create_test_data(spark)
    test_df = test_df.repartition(1)

    # TODO
    register_udf(spark, udf_name="", class_path="")
    udf_result_df = execute_udf(spark, udf_name="", test_df=test_df)

    print("UDF result:")
    udf_result_df.show(truncate=False)

    test_df.createOrReplaceTempView("test_table")

    # TODO
    register_udf(spark, udf_name="", class_path="")
    rapids_udf_result_df = execute_udf(spark, udf_name="", test_df=test_df)

    print("RapidsUDF result:")
    rapids_udf_result_df.show(truncate=False)

    assertSchemaEqual(actual=rapids_udf_result_df.schema, expected=udf_result_df.schema)
    assertDataFrameEqual(actual=rapids_udf_result_df, expected=udf_result_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug-memory-leaks", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("test udf")
        .master("local[*]")
        .config("spark.jars", 
            "target/cuaether-assistant-udfs-1.0.0.jar,"
            "~/.cache/cuaether-assistant/jars/rapids-4-spark_2.12-25.08.0.jar"
        )
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.driver.extraJavaOptions",
            "-Dai.rapids.refcount.debug=true -ea" if args.debug_memory_leaks else ""
        )
        .config("spark.executor.extraJavaOptions",
            "-Dai.rapids.refcount.debug=true -ea" if args.debug_memory_leaks else ""
        )
        .config("spark.rapids.memory.gpu.pool", "NONE")
        .config("spark.rapids.sql.explain", "NONE")
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        run_test(spark)
    except PySparkAssertionError as e:
        logger.error("PySparkAssertion failed")
        raise e
    except Exception as e:
        logger.error(f"Exception occurred: {type(e).__name__}")
        raise e
    finally:
        spark.stop()
