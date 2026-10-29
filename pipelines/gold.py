from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_product_metrics",
    comment="Metricas por produto calculadas das avaliacoes Silver"
)
def gold_product_metrics():

    # Ler as avaliacoes limpas da Silver
    df_reviews = spark.read.table(
        "silver_reviews_clean"
    )

    # Agrupar avaliacoes pelo identificador do produto
    df_metricas = (
        df_reviews
        .groupBy("parent_asin")
        .agg(
            F.count("*").alias(
                "reviews_count"
            ),

            F.round(
                F.avg("rating"), 2
            ).alias(
                "average_review_rating"
            ),

            F.sum(
                F.when(
                    F.col("verified_purchase") == True,
                    1
                ).otherwise(0)
            ).alias(
                "verified_review_count"
            ),

            F.sum("helpful_vote").alias(
                "total_helpful_votes"
            )
        )

        # Calcular o percentual de compras verificadas
        .withColumn(
            "verified_review_pct",
            F.round(
                F.col("verified_review_count")
                * F.lit(100.0)
                / F.col("reviews_count"),
                2
            )
        )

        # Definir a ordem final das colunas
        .select(
            "parent_asin",
            "reviews_count",
            "average_review_rating",
            "verified_review_count",
            "verified_review_pct",
            "total_helpful_votes"
        )
    )

    return df_metricas


@dp.materialized_view(
    name="gold_monthly_review_metrics",
    comment="Metricas mensais UTC das avaliacoes Amazon Reviews",
    spark_conf={"spark.sql.session.timeZone": "UTC"}
)
def gold_monthly_review_metrics():

    # Ler as avaliacoes deduplicadas da Silver
    df_reviews = spark.read.table(
        "silver_reviews_clean"
    )

    # Agrupar as avaliacoes por mes UTC
    df_monthly = (
        df_reviews
        .withColumn(
            "review_month",
            F.date_trunc(
                "month",
                F.col("review_timestamp_utc")
            ).cast("date")
        )
        .groupBy("review_month")
        .agg(
            F.count("*").alias(
                "reviews_count"
            ),

            F.countDistinct("parent_asin").alias(
                "reviewed_products_count"
            ),

            F.round(
                F.avg("rating"), 2
            ).alias(
                "average_review_rating"
            ),

            F.sum(
                F.when(
                    F.col("verified_purchase") == True,
                    1
                ).otherwise(0)
            ).alias(
                "verified_review_count"
            )
        )
        .withColumn(
            "verified_review_pct",
            F.round(
                F.col("verified_review_count")
                * F.lit(100.0)
                / F.col("reviews_count"),
                2
            )
        )
        .select(
            "review_month",
            "reviews_count",
            "reviewed_products_count",
            "average_review_rating",
            "verified_review_count",
            "verified_review_pct"
        )
    )

    return df_monthly


@dp.materialized_view(
    name="gold_genie_product_analytics",
    comment=(
        "Produtos e metricas de avaliacoes para "
        "consultas analiticas no Databricks Genie"
    )
)
def gold_genie_product_analytics():

    # Dados cadastrais e precos dos produtos
    df_products = spark.read.table(
        "silver_products_clean"
    ).alias("p")

    # Metricas agregadas das avaliacoes
    df_metrics = spark.read.table(
        "gold_product_metrics"
    ).alias("m")

    # Preservar todos os produtos
    df_genie = (
        df_products
        .join(
            df_metrics,
            F.col("p.parent_asin") == F.col("m.parent_asin"),
            "left"
        )
        .select(
            F.col("p.parent_asin").alias("parent_asin"),

            F.col("p.title").alias("product_title"),

            F.col("p.main_category").alias("main_category"),

            F.col("p.price").alias("price"),

            F.col("p.price_status").alias("price_status"),

            F.col("p.average_rating").alias(
                "source_average_rating"
            ),

            F.col("p.rating_number").alias(
                "source_rating_number"
            ),

            F.coalesce(
                F.col("m.reviews_count"),
                F.lit(0)
            ).alias("reviews_count"),

            F.col("m.average_review_rating").alias(
                "average_review_rating"
            ),

            F.coalesce(
                F.col("m.verified_review_count"),
                F.lit(0)
            ).alias("verified_review_count"),

            F.col("m.verified_review_pct").alias(
                "verified_review_pct"
            ),

            F.coalesce(
                F.col("m.total_helpful_votes"),
                F.lit(0)
            ).alias("total_helpful_votes")
        )
    )

    return df_genie
