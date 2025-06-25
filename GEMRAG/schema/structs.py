from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

heading_struct = StructType([
    StructField("num", StringType()),
    StructField("title", StringType())
])
chunk_schema = StructType([
    StructField("file_path", StringType()),
    StructField("dataset_name", StringType()),
    StructField("chunk_id", IntegerType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("subject", StringType()),
    StructField("keywords", StringType()),
    StructField("creation_date", StringType()),
    StructField("mod_date", StringType()),
    StructField("page_numbers", ArrayType(IntegerType())),
    StructField("text", StringType()),
    StructField("heading_context", MapType(StringType(), heading_struct)),
    StructField("hcpcs_codes", ArrayType(StringType())),
    StructField("hcpcs_descriptions", ArrayType(StringType())),
    StructField("cpt_codes", ArrayType(StringType())),
    StructField("cpt_descriptions", ArrayType(StringType())),
    StructField("icd10_codes", ArrayType(StringType())),
    StructField("icd10_descriptions", ArrayType(StringType())),
    StructField("modifier_codes", ArrayType(StringType())),
    StructField("modifier_descriptions", ArrayType(StringType()))
])


raw_schema = StructType([
    StructField("file_path", StringType()),
    StructField("dataset_name", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("subject", StringType()),
    StructField("keywords", StringType()),
    StructField("creation_date", StringType()),
    StructField("mod_date", StringType()),
    StructField("text", StringType())
])