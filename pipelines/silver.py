from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Colunas originais do Parquet de avaliações
REVIEW_COLUMNS = [
    "rating",
    "title",
    "text",
    "images",
    "asin",
    "parent_asin",
    "user_id",
    "timestamp",
    "helpful_vote",
    "verified_purchase",
]


@dp.materialized_view(
    name="silver_reviews_clean",
    comment="Avaliacoes sem duplicatas exatas e com timestamp padronizado",
    spark_conf={"spark.sql.session.timeZone": "UTC"}
)
@dp.expect_all({
    "rating_valido":
        "rating IS NOT NULL AND rating BETWEEN 1 AND 5",

    "helpful_vote_valido":
        "helpful_vote IS NOT NULL AND helpful_vote >= 0",

    "timestamp_valido":
        "timestamp IS NOT NULL AND timestamp > 0"
})
def silver_reviews_clean():

    df_bronze = spark.read.table(
        "bronze_reviews_raw"
    )

    df_silver = (
        df_bronze
        .select(*REVIEW_COLUMNS)
        .dropDuplicates()
        .withColumn(
            "review_timestamp_utc",
            F.timestamp_millis("timestamp")
        )
    )

    return df_silver


@dp.materialized_view(
    name="silver_products_clean",
    comment="Produtos padronizados com preços numericos e regras de qualidade"
)
@dp.expect_all({
    "parent_asin_valido":
        "parent_asin IS NOT NULL AND trim(parent_asin) <> ''",

    "average_rating_valido":
        "average_rating IS NOT NULL AND average_rating BETWEEN 0 AND 5",

    "rating_number_valido":
        "rating_number IS NOT NULL AND rating_number >= 0",

    "preco_interpretavel":
        "price_status <> 'invalido'"
})
def silver_products_clean():

    df_bronze = spark.read.table(
        "bronze_products_raw"
    )

    # Preservar as colunas originais de Products
    df = df_bronze.select(
        "main_category",
        "title",
        "average_rating",
        "rating_number",
        "features",
        "description",
        F.col("price").alias("price_raw"),
        "images",
        "videos",
        "store",
        "categories",
        "details",
        "parent_asin",
        "bought_together",
        "subtitle",
        "author"
    )

    # Padronizar o texto utilizado na conversao
    price_text = F.trim(F.col("price_raw"))

    # Identificar indicadores de preco ausente
    price_missing = (
        F.col("price_raw").isNull()
        | F.lower(price_text).isin("none", "null", "")
    )

    # Aceitar valores com ate duas casas decimais
    price_format_valid = price_text.rlike(
        r"^\d+(\.\d{1,2})?$"
    )

    # Converter sem provocar falha por formato invalido
    df = df.withColumn(
        "price",
        F.when(
            ~price_missing & price_format_valid,
            F.expr(
                "try_cast(trim(price_raw) AS DECIMAL(12,2))"
            )
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # Registrar a situacao do preco
    df = df.withColumn(
        "price_status",
        F.when(price_missing, F.lit("ausente"))
         .when(F.col("price").isNotNull(), F.lit("valido"))
         .otherwise(F.lit("invalido"))
    )

    return df
