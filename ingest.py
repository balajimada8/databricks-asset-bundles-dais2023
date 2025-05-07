from os import path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, to_date, col, count, sha2, concat_ws



# Bronze Layer
@dlt.table(
    comment="Raw Medium post data ingested from CSV"
)
@dlt.expect("No null links", "link IS NOT NULL")
def medium_raw():
    csv_path = "dbfs:/FileStore/fe_medium_posts_raw.csv"
    return spark.read.csv(csv_path, header=True)


# Silver Layer - Clean and deduplicate
@dlt.table(
    comment="Cleaned and deduplicated Medium posts"
)
@dlt.expect_or_drop("Valid publish date", "published_on IS NOT NULL AND published_on != ''")
def medium_clean():
    df = dlt.read("medium_raw")
    df = df.filter(df.link.isNotNull() & (df.link != 'null'))

    # Clean author name
    df = df.withColumn("author", regexp_replace("author", "\\([^()]*\\)", "").trim())

    # Normalize publish date
    df = df.withColumn("publish_date", to_date("published_on"))

    """ Generate a unique hash for deduplication
    df = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))
    df = df.dropDuplicates(["row_hash"])"""

    return df


# Silver Layer - Recent posts view
@dlt.view
def recent_posts():
    return dlt.read("medium_clean").filter(col("publish_date") >= "2023-01-01")


# Gold Layer - Post summary per author
@dlt.table(
    comment="Aggregated number of recent posts per author"
)
def author_post_counts():
    return (
        dlt.read("recent_posts")
        .groupBy("author")
        .agg(count("*").alias("post_count"))
        .orderBy(col("post_count").desc())
    )
