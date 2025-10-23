import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import col, upper, trim, when, to_date
from awsglue.dynamicframe import DynamicFrame

# Get job args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default DQ rules
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# ===============================
# 1️.Load Raw CSV Data from S3
# ===============================

alb_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://svmhamurugar/SVMHA_land/albums.csv"], "recurse": True},
    transformation_ctx="alb_node"
)

art_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://svmhamurugar/SVMHA_land/artists.csv"], "recurse": True},
    transformation_ctx="art_node"
)

tra_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://svmhamurugar/SVMHA_land/track.csv"], "recurse": True},
    transformation_ctx="tra_node"
)

# ===============================
# 2️.Convert DynamicFrames → DataFrames for Transformations
# ===============================
alb_sdf = alb_df.toDF()
art_sdf = art_df.toDF()
tra_sdf = tra_df.toDF()

# ===============================
# 3️.Data Cleaning & Transformation
# ===============================

# Remove duplicates and trim whitespace
alb_sdf = alb_sdf.dropDuplicates().withColumn("album_name", trim(col("album_name")))
art_sdf = art_sdf.dropDuplicates().withColumn("artist_name", trim(col("artist_name")))
tra_sdf = tra_sdf.dropDuplicates().withColumn("track_name", trim(col("track_name")))

# Convert release_date column to proper date format
if "release_date" in alb_sdf.columns:
    alb_sdf = alb_sdf.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

# Filter out invalid or null artist names or track IDs
art_sdf = art_sdf.filter(col("artist_name").isNotNull())
tra_sdf = tra_sdf.filter(col("track_id").isNotNull())

# Standardize text fields to uppercase for consistency
alb_sdf = alb_sdf.withColumn("album_name", upper(col("album_name")))
art_sdf = art_sdf.withColumn("artist_name", upper(col("artist_name")))
tra_sdf = tra_sdf.withColumn("track_name", upper(col("track_name")))

# ===============================
# 4️.Join Tables
# ===============================
join_art_alb = art_sdf.join(alb_sdf, art_sdf["id"] == alb_sdf["artist_id"], "inner")
join_all = tra_sdf.join(join_art_alb, tra_sdf["artist_id"] == join_art_alb["artist_id"], "inner")

# ===============================
# 5️.Drop Unnecessary Columns
# ===============================
final_df = join_all.drop("id", "artist_id", "album_id")

# ===============================
# 6️.Apply Data Quality Rules
# ===============================
final_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_dyf")

EvaluateDataQuality().process_rows(
    frame=final_dyf,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_Context", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# ===============================
# 7️.Write Transformed Data Back to S3 (Data Warehouse Layer)
# ===============================
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://svmhamurugar/SVMHA_Final/",
        "compression": "snappy",
        "partitionKeys": ["release_date"]
    },
    transformation_ctx="AmazonS3_Final"
)

job.commit()
