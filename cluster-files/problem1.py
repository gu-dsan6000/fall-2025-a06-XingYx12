#!/usr/bin/env python3

import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count, rand, upper


# Configure logging with basicConfig
logging.basicConfig(
    level=logging.INFO,  # Set the log level to INFO
    # Define log message format
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem1_Log Level Distribution_Cluster")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        .getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark

def solve_problem1(spark: SparkSession, input_path: str, output_path: str):
    """Main computation for log level distribution."""
    df = spark.read.text(input_path)
    print(f"✅ Read {df.count():,} lines from input_path = {input_path}")
    df.show(3, truncate=False)
    start_time = time.time()
    logger.info(f"Reading log data from: {input_path}")
    line_count = spark.read.text(input_path).count()
    print(f" Found {line_count:,} lines in input path: {input_path}")
    #  Read log files
    logs_df = spark.read.text(f"{input_path}").withColumnRenamed("value", "log_entry")
    
    #  Extract log level
    logs_df = logs_df.withColumn(
    "log_level",
    regexp_extract(
        upper(col("log_entry")),
        r"(INFO|WARN|ERROR|DEBUG)(?:\s|:|,|$)",
        1
    )
)

    #  Filter valid log levels
    valid_df = logs_df.filter(col("log_level") != "")
    print(f"✅ Valid log lines after regex: {valid_df.count():,}")
    #  Count per log level
    counts_df = valid_df.groupBy("log_level").agg(count("*").alias("count"))
    counts_output = f"{output_path}/problem1_counts.csv"
    counts_df.orderBy(col("count").desc()).write.csv(
        counts_output, header=True, mode="overwrite"
    )
    logger.info(f"Wrote log level counts to: {counts_output}")

    #  Sample 10 log entries
    sample_df = valid_df.orderBy(rand()).limit(10)
    sample_output = f"{output_path}/problem1_sample.csv"
    sample_df.select("log_entry", "log_level").write.csv(
        sample_output, header=True, mode="overwrite"
    )
    logger.info(f"Wrote 10-sample logs to: {sample_output}")

    #  Summary stats
    total_lines = logs_df.count()
    total_valid = valid_df.count()
    unique_levels = counts_df.count()
    counts_local = {r['log_level']: r['count'] for r in counts_df.collect()}
    total_counts = sum(counts_local.values())

    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_valid:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]
    for lvl, cnt in counts_local.items():
        pct = (cnt / total_counts * 100) if total_counts > 0 else 0
        summary_lines.append(f"  {lvl:<6}: {cnt:>10,} ({pct:6.2f}%)")

    summary_output = f"{output_path}/problem1_summary.txt"
    summary_rdd = spark.sparkContext.parallelize(summary_lines)
    summary_rdd.saveAsTextFile(summary_output)
    logger.info(f"Wrote summary text to: {summary_output}")

    elapsed = time.time() - start_time
    logger.info(f"Problem 1 completed in {elapsed:.2f} seconds.")


def main():
    """Main function for Problem 1 - Cluster Version."""

    logger.info("Starting Problem 1: Log Level Distributions (Cluster Mode)")
    print("=" * 70)
    print("PROBLEM 1: Log Level Distribution (CLUSTER MODE)")
    print("=" * 70)

    # Get master URL from command line or environment variable
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        # Try to get from environment variable
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided")
            print("Usage: python problem1_cluster.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    overall_start = time.time()
    input_path = "s3a://yx390-assignment-spark-cluster-logs/data/**"
    output_path = "s3a://yx390-assignment-spark-cluster-logs/output/"
    # Create Spark session
    logger.info("Initializing Spark session for cluster execution")
    spark = create_spark_session(master_url)

    

    # Solve Problem 1
    try:
        solve_problem1(spark, input_path, output_path)
        print("\n Problem 1 completed successfully!")
        success = True
    except Exception as e:
        logger.exception(f"Error in Problem 1: {e}")
        print(f" Error during analysis: {e}")
        success = False

    # Clean up
    logger.info("Stopping Spark session")
    spark.stop()

    # Overall timing
    overall_end = time.time()
    total_time = overall_end - overall_start
    logger.info(f"Total execution time: {total_time:.2f} seconds")

    print("\n" + "=" * 70)
    if success:
        print(" PROBLEM 1 COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {total_time:.2f} seconds")
        print("\nFiles saved to:")
        print("  - s3://$yx390-assignment-spark-cluster-logs/output/")
        print("\nNext steps:")
    else:
        print("❌ Problem 1 failed - check error messages above")
    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
