#!/usr/bin/env python3
import os
import time
import logging
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, countDistinct
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(process)d,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s"
)
logger = logging.getLogger(__name__)

def create_spark():
    spark = (
        SparkSession.builder
        .appName("Problem2 Analysis")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .getOrCreate()
    )
    logger.info("Spark session created successfully")
    return spark

def run_spark_analysis(spark, input_path, output_dir):
    logger.info(f"Reading log data from: {input_path}")
    df = spark.read.text(input_path)
    parsed_df = df.select(
        when(
            regexp_extract("value", r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1) != "",
            regexp_extract("value", r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
        ).otherwise(
            regexp_extract("value", r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", 1)
        ).alias("timestamp"),
        regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("level"),
        regexp_extract("value", r"application[_0-9]+", 0).alias("application_id"),
        regexp_extract("value", r"cluster[_0-9]+", 0).alias("cluster_id"),
        col("value").alias("message")
    ).filter(col("timestamp") != "")
    if parsed_df.count() == 0:
        logger.warning("No valid log entries found.")
        return
    timeline_path = os.path.join(output_dir, "problem2_timeline.csv")
    parsed_df.select("timestamp", "cluster_id", "application_id", "level") \
        .write.mode("overwrite").csv(timeline_path, header=True)
    logger.info(f"Saved timeline to {timeline_path}")
    cluster_summary = (
        parsed_df.groupBy("cluster_id")
        .agg(countDistinct("application_id").alias("application_count"))
        .orderBy(col("application_count").desc())
    )
    summary_path = os.path.join(output_dir, "problem2_cluster_summary.csv")
    cluster_summary.write.mode("overwrite").csv(summary_path, header=True)
    logger.info(f"Saved cluster summary to {summary_path}")
    total_clusters = cluster_summary.count()
    total_apps = cluster_summary.agg({"application_count": "sum"}).collect()[0][0]
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
    print(f"\nTotal unique clusters: {total_clusters}")
    print(f"Total applications: {total_apps}")
    print(f"Average applications per cluster: {avg_apps:.2f}\n")
    print("Most heavily used clusters:")
    for row in cluster_summary.limit(10).collect():
        print(f"  {row['cluster_id']}: {row['application_count']} applications")
    stats_file = os.path.join(output_dir, "problem2_stats.txt")
    with open(stats_file, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for row in cluster_summary.limit(10).collect():
            f.write(f"  {row['cluster_id']}: {row['application_count']} applications\n")
    logger.info(f"Wrote stats to {stats_file}")

def load_first_part(path):
    for root, _, files in os.walk(path):
        for file in files:
            if file.startswith("part-") and file.endswith(".csv"):
                file_path = os.path.join(root, file)
                try:
                    return pd.read_csv(file_path, on_bad_lines="skip")
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
    raise FileNotFoundError(f"No valid CSV part files found in {path}")

def generate_visualizations(output_dir):
    logger.info("Generating visualizations...")
    timeline_dir = os.path.join(output_dir, "problem2_timeline.csv")
    summary_dir = os.path.join(output_dir, "problem2_cluster_summary.csv")
    try:
        timeline_df = load_first_part(timeline_dir)
        summary_df = load_first_part(summary_dir)
    except FileNotFoundError as e:
        logger.error(str(e))
        return
    if "timestamp" in timeline_df.columns:
        plt.figure(figsize=(10, 4))
        plt.hist(timeline_df["timestamp"], bins=30)
        plt.title("Log Message Distribution Over Time")
        plt.xlabel("Timestamp")
        plt.ylabel("Frequency")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "timeline_histogram.png"))
        plt.close()
        logger.info("Saved timeline histogram")
    if "cluster_id" in summary_df.columns and "application_count" in summary_df.columns:
        plt.figure(figsize=(8, 4))
        plt.bar(summary_df["cluster_id"], summary_df["application_count"])
        plt.title("Applications per Cluster")
        plt.xlabel("Cluster ID")
        plt.ylabel("Application Count")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "cluster_usage_bar_chart.png"))
        plt.close()
        logger.info("Saved cluster usage bar chart")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="s3a://yx390-assignment-spark-cluster-logs/data/*/*", help="Input path for log data")
    parser.add_argument("--output", default="data/output", help="Output directory")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark analysis and only generate visualizations")
    args = parser.parse_args()
    start = time.time()
    os.makedirs(args.output, exist_ok=True)
    if not args.skip_spark:
        spark = create_spark()
        run_spark_analysis(spark, args.input, args.output)
        spark.stop()
    generate_visualizations(args.output)
    end = time.time()
    logger.info(f"Problem 2 completed in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
