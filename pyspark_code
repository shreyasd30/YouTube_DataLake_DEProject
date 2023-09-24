import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

predicate_pushdown = "region in ('ca','gb','us')"

# Script generated for node Source_datastore_bucket
Source_datastore_bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://de-youtubedatalake-us-east-1-dev/youtube/raw_statistics/"],
        "recurse": True,
    },
    transformation_ctx="Source_datastore_bucket_node1",
    push_down_predicate = predicate_pushdown
)

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(
    frame=Source_datastore_bucket_node1,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "bigint", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "bigint", "views", "bigint"),
        ("likes", "bigint", "likes", "bigint"),
        ("dislikes", "bigint", "dislikes", "bigint"),
        ("comment_count", "bigint", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string")
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

# Script generated for node Target_datastore_bucket
Target_datastore_bucket_node3 = glueContext.getSink(
    path="s3://de-youtubedatalake-us-east-1-cleansed-ver/youtube/raw_statistics/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="Target_datastore_bucket_node3",
)
Target_datastore_bucket_node3.setCatalogInfo(
    catalogDatabase="db_youtube_cleaned", catalogTableName="cleaned_csv_to_parquet"
)

Resolve_choice1 = ResolveChoice.apply(frame = ChangeSchema_node2, choice = "make_struct", transformation_ctx = "Resolve_choice1")

dropnullfields1 = dropnullfields.apply(frame=Resolve_choice1, transformation_ctx= "dropnullfields1")

Target_datastore = dropnullfields1.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(Target_datastore, glueContext, "df_final_output")
Target_datastore_final = glueContext.write_dyanamic_frame.from_options(frame = df_final_output, connection_type = "s3",
connection_options = {"path":"s3://de-youtubedatalake-us-east-1-dev/youtube/raw_statistics/", "partitionKeys": ["region"]},
format = "parquet", transformation_ctx = "Target_datastore_final")
job.commit()
