#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, upper, rand, count

def main():
    if len(sys.argv) < 2:
        print("Usage: python cluster_s3_diagnose.py <MASTER_PRIVATE_IP>")
        sys.exit(1)

    master_ip = sys.argv[1]
    input_path = "s3a://yx390-assignment-spark-cluster-logs/data/*/*"
    output_path = "s3a://yx390-assignment-spark-cluster-logs/test_output_diagnose"

    print("=" * 80)
    print("🔍 Spark Cluster – S3 Data Diagnostic Test")
    print(f"Master: spark://{master_ip}:7077")
    print(f"Input Path: {input_path}")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("Cluster_S3_Diagnose")
        .master(f"spark://{master_ip}:7077")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )

    try:

         # Check whether input path matches any files

        print("\n Checking input path...")
        try:
            df = spark.read.text(input_path)
            total_lines = df.count()
            print(f"✅ Found {total_lines:,} total lines in input path.")
            if total_lines == 0:
                print("⚠️ WARNING: Path matched but no lines read (possible wildcard issue).")
        except Exception as e:
            print(f"❌ Failed to read input path: {e}")
            total_lines = 0

        if total_lines > 0:
            print("📄 Sample 3 lines from S3:")
            df.show(3, truncate=False)


        #  Check if regex extraction works correctly
      
        print("\n[2️⃣] Checking regex extraction for log levels...")
        logs_df = df.withColumnRenamed("value", "log_entry")
        logs_df = logs_df.withColumn(
            "log_level",
            regexp_extract(
                upper(col("log_entry")),
                r"(INFO|WARN|ERROR|DEBUG)(?:\s|:|,|$)",
                1,
            )
        )
        valid_df = logs_df.filter(col("log_level") != "")
        total_valid = valid_df.count()
        if total_valid == 0:
            print("⚠️ WARNING: Regex did not match any log lines.")
            print("👉 Possible cause: log format differs or regex too strict.")
        else:
            print(f" Matched {total_valid:,} log lines with levels.")
            valid_df.groupBy("log_level").agg(count("*").alias("count")).show()

     
        # test writing output back to S3
      
        print("\n[3️⃣] Checking output write to S3...")
        try:
            sample_df = valid_df.orderBy(rand()).limit(10)
            sample_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
            print(f" Successfully wrote sample output to: {output_path}")
        except Exception as e:
            print(f"❌ ERROR writing to output path: {e}")

   
        # Summarize diagnostic results
   
        print("\n" + "=" * 80)
        print("Diagnostic Summary:")
        if total_lines == 0:
            print("No data read from input path → Check S3 path or wildcard pattern.")
        elif total_valid == 0:
            print("❌ Regex did not match any logs → Adjust regex pattern for log format.")
        else:
            print(" Data read and parsed successfully → Output should contain valid results.")
        print("=" * 80)

    finally:
        spark.stop()
        print("\n Test complete.")

if __name__ == "__main__":
    main()
