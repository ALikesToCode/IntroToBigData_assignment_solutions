#!/usr/bin/env python3
"""
Local smoke test for the image classification UDF (batch mode).

Usage:
  python week-9/local_smoke_test.py --pairs gs://bucket1,path/to/img1.jpg gs://bucket2,path/to/img2.png
  or
  python week-9/local_smoke_test.py --file pairs.txt

Each pair is either two args (bucket,object) or a single gs://bucket/object URI.
Requires: google-cloud-storage, tensorflow, pillow
"""
import argparse
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from streaming_image_classifier import classify_image_udf  # reuse UDF


def parse_uri(uri: str):
    if uri.startswith("gs://"):
        u = urlparse(uri)
        bucket = u.netloc
        name = u.path.lstrip("/")
        return bucket, name
    raise ValueError(f"Unsupported URI: {uri}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", nargs="*", default=[], help="List of gs://bucket/object URIs")
    ap.add_argument("--file", default=None, help="File with one gs://bucket/object per line")
    ap.add_argument("--model-gcs-path", default=None, help="Optional model path in GCS")
    args = ap.parse_args()

    uris = list(args.pairs)
    if args.file:
        with open(args.file) as f:
            uris.extend([line.strip() for line in f if line.strip()])

    rows = []
    for uri in uris:
        b, n = parse_uri(uri)
        rows.append((b, n))

    spark = SparkSession.builder.appName("LocalImageSmokeTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(rows, schema=T.StructType([
        T.StructField("bucket", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
    ]))

    out = df.withColumn("pred", classify_image_udf(F.col("bucket"), F.col("name"), F.lit(args.model_gcs_path))) \
            .select("bucket", "name",
                    F.col("pred.label").alias("label"),
                    F.col("pred.score").alias("score"),
                    F.col("pred.model_name").alias("model_name"),
                    F.col("pred.error").alias("error"))

    out.show(truncate=False)


if __name__ == "__main__":
    main()

