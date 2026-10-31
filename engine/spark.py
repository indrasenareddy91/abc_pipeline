"""SparkSession factory: Glue's session when running in AWS Glue, otherwise local[2]."""
from __future__ import annotations

import os

# Java 17/21: Spark 3.5 needs these module opens (spark-submit adds most of them; harmless if repeated).
_JAVA_OPENS = " ".join(
    f"--add-opens=java.base/{pkg}=ALL-UNNAMED"
    for pkg in ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
                "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
                "sun.security.action", "sun.util.calendar"]
) + " -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"

# settings that make results identical locally and in Glue
_COMMON_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.ansi.enabled": "false",                     # bad casts -> NULL (we check them), never crash
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",      # one explicit format per value shape
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    "spark.sql.sources.partitionOverwriteMode": "static",
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",   # batch_id=20261001 stays a string
}

_glue_context = None


def get_spark(app_name: str = "cdt"):
    """Return a SparkSession. In Glue reuse SparkContext/GlueContext; locally build local[2]."""
    global _glue_context
    try:
        from awsglue.context import GlueContext  # type: ignore
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        _glue_context = _glue_context or GlueContext(sc)
        spark = _glue_context.spark_session
    except ImportError:
        from pyspark.sql import SparkSession
        os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
        builder = (SparkSession.builder.appName(app_name)
                   .master(os.environ.get("SPARK_MASTER", "local[2]"))
                   .config("spark.ui.enabled", "false")
                   .config("spark.ui.showConsoleProgress", "false")   # keep STEP_* lines clean
                   .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                   .config("spark.driver.extraJavaOptions", _JAVA_OPENS)
                   .config("spark.executor.extraJavaOptions", _JAVA_OPENS))
        for k, v in _COMMON_CONF.items():
            builder = builder.config(k, v)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    for k, v in _COMMON_CONF.items():
        spark.conf.set(k, v)
    return spark
