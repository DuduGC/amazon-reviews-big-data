from pyspark import pipelines as dp
from pyspark.sql import functions as F


REVIEWS_PATH = (
    "/Volumes/workspace/amazon_reviews/"
    "raw_files/reviews/"
)


@dp.table(
    name="bronze_reviews_raw",
    comment="Avaliacoes brutas Amazon Reviews 2023 - All Beauty"
)
def bronze_reviews_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(REVIEWS_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modified_at"
            ),
            F.current_timestamp().alias("_ingested_at")
        )
    )

# Caminho de origem dos produtos
PRODUCTS_PATH = (
    "/Volumes/workspace/amazon_reviews/"
    "raw_files/products/"
)


@dp.table(
    name="bronze_products_raw",
    comment="Metadados brutos Amazon Reviews 2023 - All Beauty"
)
def bronze_products_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(PRODUCTS_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modified_at"
            ),
            F.current_timestamp().alias("_ingested_at")
        )
    )