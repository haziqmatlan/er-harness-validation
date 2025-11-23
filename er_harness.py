# Databricks notebook source
# MAGIC %md
# MAGIC ### MD5 Generation 
# MAGIC To generate:
# MAGIC - key_columns (Join of _source_primary_key_ & _target_primary_key_ )
# MAGIC - concat_columns (Join of all columns)
# MAGIC - MD5 hashing of all concat_columns

# COMMAND ----------

from typing import Text, List

def generate_md5_concatenate_columns(key_column: Text, view_or_table: Text):
    try:
        all_columns = spark.sql(f"select * from {view_or_table}").drop('human_review', 'match_status_er', 'match_status_manual', 'error_matrix').columns
        key_columns = key_column.split(",")

        # Join both key_columns (source_primary_key, target_primary_key)
        all_columns_string = "\n||'-'||".join([f"nvl(cast({x} as string), '')" for x in all_columns])
        key_columns_string = "\n||'-'||".join([f"nvl(cast({x} as string), '')" for x in key_columns])
        query = f"""
            select *, {key_columns_string} as key_columns,
            md5( {all_columns_string}) as md5_hash_concat_column
            from {view_or_table}
            """

        df = spark.sql(query)        
        return df

    except Exception as e:
        print(f"An unexpected error occurred while generating MD5 concatenation: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### map_match_mismatch_records
# MAGIC To identify Match/Mismatch comparison result of ER Output (_eo) Vs Truth File (_tf):
# MAGIC 1. by comparing 1:1 matching of hash column:
# MAGIC     - joined by key_columns (source_primary_key & target_primary_key)
# MAGIC     - compare hash - src_tgt_md5_hash_column_eo Vs src_tgt_md5_hash_column_tf
# MAGIC 2. by comparing each of columns

# COMMAND ----------

from pyspark.sql import functions as F

def map_match_mismatch_records(source_dataframe, target_dataframe, drop_columns): 
    """ 
    1 to 1 comparison - between source (_eo) & target dataframe (_tf)    
        Match: key_column & hash_columns are all matched
        Mismatch: Only key_column is matched, hash_columns are NOT matched
    """
    hash_df = (
        source_dataframe.join(target_dataframe, source_dataframe.src_tgt_key_columns_eo == target_dataframe.src_tgt_key_columns_tf, how="outer") \
        .withColumn("validation_status", when(source_dataframe.src_tgt_md5_hash_column_eo == target_dataframe.src_tgt_md5_hash_column_tf, 'Match').otherwise('Mismatch') ) \
        ).drop(*drop_columns)

    # Re-initialize with original df
    hash_df_compare = hash_df

    # Columns to exclude comparison
    exclude_col = ["validation_status", "human_review_tf", "src_tgt_md5_hash_column_eo", "src_tgt_md5_hash_column_tf", "match_status_er_eo", "match_status_manual_tf", "error_matrix_eo", "error_matrix_tf"]

    """ 
    column-level comparison - each columns and find differences of *_eo Vs *_tf
        1: both column match
        0: column mismatch
    """
    for col in hash_df.columns:
        if col not in exclude_col: 
            if "_eo" in col:
                # To prevent iteration of same column of _tf
                target_col = col.replace("_eo", "_tf")

                if col in ["source_first_name_eo", "source_last_name_eo"]:
                    """
                    Applying token-based matching by split compound names and compare tokens:
                        Example: "Kirby-Jones" → ["Kirby", "Jones"]
                    """
                    # Normalize: remove non-letters, split into tokens
                    df_clean = hash_df_compare \
                        .withColumn(f"{col}_token",F.split(F.regexp_replace(F.col(col), "[^a-z]", " "),"\\s+") ) \
                        .withColumn(f"{target_col}_token",F.split(F.regexp_replace(F.col(target_col), "[^a-z]", " "),"\\s+") ) 

                    # Compare token by checking whether the names share a common part (Ex: jones)
                    hash_df_compare = df_clean.withColumn(
                        f"pattern_{col}",
                        when( F.expr(f"size( array_intersect({col}_token, {target_col}_token)) > 0"), '1')
                        .otherwise('0')
                        ).drop(f"{col}_token", f"{target_col}_token")
                else:
                    """
                    Direct 1:1 comparison of columns
                    """
                    hash_df_compare = hash_df_compare \
                        .withColumn(
                            f"pattern_{col}", when( (hash_df[col].isNull() & hash_df[target_col].isNull()) | (hash_df[col] == hash_df[target_col]), '1') \
                            .otherwise('0')
                            )

    # To fetch only mismatch_codes column & exclude validation_status 
    concat_col = [ hash_df_compare[f"{i}"] for i in hash_df_compare.columns if "pattern_" in i and "validation_status" not in i ]

    # Concat all mismatch codes columns
    compared_column = hash_df_compare \
        .withColumn("mismatches_code", concat(*concat_col, lit("") ) ) \
        .withColumn("extra_or_missing_flag", lit(None)) \
        .drop(*concat_col)

    # Filter between match & mismatch records
    match_df = compared_column.where(compared_column.validation_status == "Match")
    mismatch_df = compared_column \
        .where(
            (compared_column.validation_status == "Mismatch") & (compared_column.source_primary_key_eo.isNotNull()) & (compared_column.source_primary_key_tf.isNotNull())
            )
    return match_df, mismatch_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### map_extra_records
# MAGIC To identify any records that exist in ER Output (_eo) but not in Truth File (_tf), vice versa.

# COMMAND ----------

def map_extra_records(
    source_df,
    target_df,
    key_column_source,
    key_column_target,
    repopulate_columns,
    drop_columns,
):
    # To find extra records in source dataframe
    extra_record_df = (
        source_df.join(
            target_df,
            source_df[key_column_source] == target_df[key_column_target],
            how="left_anti",
        )
    ).withColumn(
        "validation_status",
        when(source_df[key_column_source].isNotNull(), "Mismatch").otherwise("Match"),
    )

    # Repopulate all target_dataframe columns as None (since no match record found)
    for col in repopulate_columns:
        extra_record_df = extra_record_df.withColumn(col, lit(None))

    extra_record_df = (
        extra_record_df.withColumn(
            "mismatches_code",
            when(
                extra_record_df.validation_status == "Mismatch", "000000000000000000000"
            ).otherwise(None),
        )
        .withColumn("validation_status", lit("Mismatch"))
        .drop(*drop_columns)
    )
    extra_record_df = extra_record_df.withColumn(
        "extra_or_missing_flag",
        when(extra_record_df.source_primary_key_tf.isNull(), False).otherwise(True),
    )
    return extra_record_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename_columns

# COMMAND ----------

def rename_columns(df):
    for col in df.columns:
        if '_tf' in col:
            new_col = col.replace('_tf', '')
            df = df.withColumnRenamed(col, new_col)
        else:
            new_col = col.replace('_eo', '')
            df = df.withColumnRenamed(col, new_col)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### get_mismatching_columns
# MAGIC - To identify which columns correspond to 0 bits in the string (indicating mismatches).
# MAGIC - Then, returns the list of those mismatch column, along with the percentage of total columns that are mismatched.

# COMMAND ----------

def get_mismatching_columns(binary_string, lookup_lib):
    # Convert binary string to list of integers
    binary_list = [int(bit) for bit in binary_string]

    # Find corresponding values from lookup dictionary where the value is 0
    unmatching_columns = [lookup_lib[i] for i, j in enumerate(binary_list) if j == 0]
    col_size = len(unmatching_columns)
    lib_size = len(lookup_lib)
    # Calculate the deviation and convert to percentage
    deviation = (len(unmatching_columns) / len(lookup_lib)) * 100
    # converting float to int for percentage.
    deviation = int(deviation)

    return unmatching_columns, deviation

# COMMAND ----------

# MAGIC %md
# MAGIC ### get_status_mismatch_type
# MAGIC To read the mismatch binary code and convert to a readable description: mismatch_type

# COMMAND ----------


from pyspark.shell import spark

def get_status_mismatch_type(binary_string, extra_or_missing_flag, lookup_lib):
# extra_or_missing_flag is a boolean flag. False = 'Extra Record in ER Output' and True = 'Record Missing from ER Output'
# Check how many mismatch column found and assigning final status.      
    try:
        binary_list = list(binary_string)  

        if '0' in binary_list:
            final_status = 'Mismatch'
        else:
            final_status = 'Match'

        # To check whether we_pid column found in the mismatch column dictionary (it positions at 0).
        # If yes, capture 0 else handle is exception with assignment value of 1.
        indexing = lookup_lib.index('target_entity_id')
        mismatch = binary_list[indexing]

        mismatch_col = binary_list.count('0')

        if final_status == 'Match' and extra_or_missing_flag == None:
            # This mismatch type is for match record on the mismatch_type value.
            get_mismatch_type = 'NA'
        elif mismatch == '0' and final_status == 'Mismatch' and extra_or_missing_flag == None and mismatch_col == 1:
            # This mismatch type is for only we_pid mismatch.
            get_mismatch_type = 'Only WE_PID mismatch'
        elif mismatch == '0' and final_status == 'Mismatch' and extra_or_missing_flag == None and mismatch_col > 1:
            # This mismatch type is for only we_pid mismatch.
            get_mismatch_type = 'WE_PID and other column mismatch'
        elif mismatch == '1' and final_status == 'Mismatch' and extra_or_missing_flag == None and mismatch_col >= 1:
            # This mismatch type is for we_pid and other column(s) found mismatches.
            get_mismatch_type = 'Other columns mismatch'
        elif final_status == 'Mismatch' and extra_or_missing_flag == False:
            get_mismatch_type = 'Extra Record in ER Output'
        elif final_status == 'Mismatch' and extra_or_missing_flag == True:
            get_mismatch_type = 'Record Missing from ER Output'
        else:
            get_mismatch_type = 'Unknown Error'

        return final_status, get_mismatch_type
    except KeyError:
        return (None, None)


# COMMAND ----------

# MAGIC %md
# MAGIC ### lookup lib

# COMMAND ----------

"""
This is a lookup library of columns used to compare each of Source Vs Target

Function used: get_mismatching_columns()
"""

config_lookup_dict = {
  "my_list": [
    "source_primary_key",
    "target_primary_key",
    "source_first_name",
    "target_first_name",
    "source_last_name",
    "target_last_name",
    "source_address_line",
    "target_address_line",
    "source_city",
    "target_city",
    "source_middle_name",
    "target_middle_name",
    "source_postal_code",
    "target_postal_code",
    "source_state",
    "target_state",
    "source_suffix",
    "target_suffix",
    "target_entity_id",
    "er_score",
    "er_rank",
    "match_status_er",
    "match_status_manual",
    "error_matrix"
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test data

# COMMAND ----------

selected_columns = [
    "source_primary_key",
    "target_primary_key",
    "source_first_name",
    "target_first_name",
    "source_last_name",
    "target_last_name",
    "source_address_line",
    "target_address_line",
    "source_city",
    "target_city",
    "source_middle_name",
    "target_middle_name",
    "source_postal_code",
    "target_postal_code",
    "source_state",
    "target_state",
    "source_suffix",
    "target_suffix",
    "target_entity_id",
    "er_score",
    "er_rank",
    "first_name_score",
    "last_name_score",
    "middle_name_score",
    "city_score",
    "address_line_score",
    "state_score",
    "postal_code_score",
    "suffix_score",
]
selected_columns = ", ".join(selected_columns)

# To identify Match/Mismatch types
er_dataframe = spark.sql(
    f"""
    SELECT {selected_columns}, 
    CASE 
        WHEN er_rank = 1 AND
        (
            (
                er_score >= 0.8 AND
                suffix_score > 0 
            ) OR
            (
                er_score > 0.6 AND
                first_name_score >= 0.12 AND
                last_name_score >= 0.12 AND
                middle_name_score >= 0.02 AND
                city_score >= 0.08 AND
                address_line_score > 0.14 AND
                state_score >= 0.05 AND
                postal_code_score >= 0.11 AND
                suffix_score > 0
            ) 
        )
        THEN 'Matched' ELSE 'NOT Matched'    
    END AS match_status_er   
    FROM altrata_data_lake_poc_dev.synthetic_er_harness.er_test_pam_matching_pairs_sorted_by_overall_score 
                             """
)
truth_dataframe = spark.sql(f"""
    SELECT {selected_columns}, 
    CASE 
        -- 🟥 Step 1: Handle exception pairs first (for testing purpose)
        WHEN 
            (source_primary_key = '4f81ac70-491d-4d07-86c1-1026563c0458' AND target_primary_key = 'd9a4bd4e-3b15-42dc-a9c9-c5590ae5895c') 
            OR (source_primary_key = '9644f1a1-9140-4b1d-aeba-61c1ff11068f' AND target_primary_key = '0922ddad-651b-4d62-9f54-8bf005d40da3') 
            OR (source_primary_key = '0e32e4f7-da08-4817-8281-0ba70d2767e9' AND target_primary_key = '890d5f8e-df8f-4d08-9c91-5701d1b13e63') 
        THEN 
            CASE 
                -- Flip the normal logic: To handle match_status columns that have different result for truth dataframe
                WHEN er_rank = 1 AND
                    (
                        (er_score >= 0.8 AND suffix_score > 0)
                        OR
                        (
                            er_score > 0.6 AND
                            first_name_score >= 0.12 AND
                            last_name_score >= 0.12 AND
                            middle_name_score >= 0.02 AND
                            city_score >= 0.08 AND
                            address_line_score > 0.14 AND
                            state_score >= 0.05 AND
                            postal_code_score >= 0.11 AND
                            suffix_score > 0
                        )
                    )
                THEN 'NOT Matched'
                ELSE 'Matched'
            END

        -- 🟩 Step 2: Normal logic for all other records
        WHEN er_rank = 1 AND
            (
                (er_score >= 0.8 AND suffix_score > 0)
                OR
                (
                    er_score > 0.6 AND
                    first_name_score >= 0.12 AND
                    last_name_score >= 0.12 AND
                    middle_name_score >= 0.02 AND
                    city_score >= 0.08 AND
                    address_line_score > 0.14 AND
                    state_score >= 0.05 AND
                    postal_code_score >= 0.11 AND
                    suffix_score > 0
                )
            )
        THEN 'Matched'
        ELSE 'NOT Matched'
    END AS match_status_manual,
    human_review 
    FROM altrata_data_lake_poc_dev.synthetic_er_harness.er_test_truth
""")

# Alias the dataframes
er_df = er_dataframe.alias("er")
truth_df = truth_dataframe.alias("truth")

# To identify error matrix
source_dataframe = er_df \
    .join(truth_df, on=["source_primary_key", "target_primary_key"], how="left") \
    .withColumn(
        "error_matrix",
        F.when((F.col("er.match_status_er") == "Matched") & (F.col("truth.match_status_manual") == "Matched"), "TP")
         .when((F.col("er.match_status_er") == "NOT Matched") & (F.col("truth.match_status_manual") == "NOT Matched"), "TN")
         .when((F.col("er.match_status_er") == "Matched") & (F.col("truth.match_status_manual") == "NOT Matched"), "FP")
         .when((F.col("er.match_status_er") == "NOT Matched") & (F.col("truth.match_status_manual") == "Matched"), "FN")
    ) \
    .select(*[F.col(f"er.{col}") for col in er_dataframe.columns], "error_matrix") \
    .dropDuplicates(["source_primary_key", "target_primary_key"])

# Re-intialize the target dataframe
target_dataframe = truth_df     

source_dataframe.createOrReplaceTempView("source_data_view")
target_dataframe.createOrReplaceTempView("target_data_view")

# Fill empty value with 'null'
source_dataframe = source_dataframe.fillna("null").replace("", "null")
target_dataframe = target_dataframe.fillna("null").replace("", "null")

# source_dataframe.display()
# target_dataframe.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Script

# COMMAND ----------

from datetime import datetime
from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    when,
    max,
    trim,
    concat,
    udf,
    concat_ws,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    StringType,
    IntegerType,
)

date_time_obj = datetime.utcnow()
batch_id = date_time_obj.strftime("%Y%m%d%H%M%S")
lookup_lib = config_lookup_dict["my_list"]

key_column = "source_primary_key, target_primary_key"

source_dataframe = generate_md5_concatenate_columns(
    key_column=key_column, view_or_table="source_data_view"
)
source_dataframe = (
    source_dataframe.withColumnRenamed("key_columns", "src_tgt_key_columns")
    .withColumnRenamed("md5_hash_concat_column", "src_tgt_md5_hash_column")
)
# Rename all Source columns to _eo (er output)
for col in source_dataframe.columns:
    source_dataframe = source_dataframe.withColumnRenamed(col, f"{col}_eo")

target_dataframe = generate_md5_concatenate_columns(
    key_column=key_column, view_or_table="target_data_view"
)
target_dataframe = (
    target_dataframe.withColumnRenamed("key_columns", "src_tgt_key_columns")
    .withColumnRenamed("md5_hash_concat_column", "src_tgt_md5_hash_column")
)
# Rename all Target columns _tf (truth file)
for col in target_dataframe.columns:
    target_dataframe = target_dataframe.withColumnRenamed(col, f"{col}_tf")

""" To identify between Match & Mismatch records """
# Match & Mismatch records
drop_columns = [
    "src_tgt_key_columns_tf",
    "src_tgt_key_columns_eo"
]
match_df, mismatch_df = map_match_mismatch_records(
    source_dataframe, target_dataframe, drop_columns=drop_columns
)

""" To identify any extra records from either ER Output or Truth Table """
# Extra records validation
source_extra_record_df = map_extra_records(
    source_dataframe,
    target_dataframe,
    key_column_source="src_tgt_key_columns_eo",
    key_column_target="src_tgt_key_columns_tf",
    repopulate_columns=target_dataframe.columns,
    drop_columns=[
        "src_tgt_key_columns_eo",
        "src_tgt_key_columns_tf"
    ],
)
target_extra_record_df = map_extra_records(
    target_dataframe,
    source_dataframe,
    key_column_source="src_tgt_key_columns_tf",
    key_column_target="src_tgt_key_columns_eo",
    repopulate_columns=source_dataframe.columns,
    drop_columns=[
        "src_tgt_key_columns_eo",
        "src_tgt_key_columns_tf"
        ],
)

union_df = (
    match_df.unionByName(mismatch_df)
    .unionByName(source_extra_record_df)
    .unionByName(target_extra_record_df)
    .distinct()
)

'''  Calling get_mismatching_columns() - To get mismatch columns and calculate mismatch deviation (%)  '''
# Define the get_mismatching_columns() schema for the UDF return type
deviation_schema = StructType([
    StructField("mismatches_columns", ArrayType(StringType()), True),
    StructField("mismatch_deviation", IntegerType(), True)
])
# Register the UDF
get_mismatching_columns_udf = udf(lambda x: get_mismatching_columns(x, lookup_lib), deviation_schema)

# Apply the UDF to the DataFrame
mismatching_col = union_df.withColumn("result", get_mismatching_columns_udf(union_df.mismatches_code))
mismatching_col = mismatching_col \
    .withColumn("mismatches_columns", concat_ws(',', mismatching_col.result.mismatches_columns) ) \
    .withColumn("mismatch_deviation", mismatching_col.result.mismatch_deviation ) \
    .drop("result")

''' Calling get_status_mismatch_type() - To read the mismatch binary code and convert to readable description: mismatch_type '''
# Define the get_status_mismatch_type() schema for the UDF return type
mismatch_type_schema = StructType([
    StructField("mismatch",  StringType(), True),
    StructField("mismatch_type",  StringType(), True)
])

# Register the UDF
get_status_mismatch_type_udf = udf(lambda x, y: get_status_mismatch_type(x, y, lookup_lib), mismatch_type_schema)

# Apply the UDF to the DataFrame
status_mismatch = mismatching_col.withColumn("result", get_status_mismatch_type_udf(mismatching_col.mismatches_code, mismatching_col.extra_or_missing_flag))
status_mismatch = status_mismatch \
    .withColumn("mismatch", concat_ws(',', status_mismatch.result.mismatch) ) \
    .withColumn("mismatch_type", status_mismatch.result.mismatch_type ) \
    .withColumnRenamed("human_review_tf", "human_review").drop("result", "mismatch") # Renamed column & drop unnecessary columns

''' Cleaning for final_df '''
# Get the list of columns to drop (except match_status_er)
columns_to_drop = [col for col in status_mismatch.columns if col.endswith('_tf') and col != 'match_status_manual_tf']

# Drop the columns
cleaned_df = status_mismatch.drop(*columns_to_drop)

# Rename the columns and update batch_id
rename_col_df = rename_columns(cleaned_df)
add_batchid_df = rename_col_df.withColumn("batch_id", lit(batch_id))

final_df = add_batchid_df

# ----------
print("final_df: ")
final_df.select("source_primary_key", "target_primary_key", "target_entity_id", "human_review", 
                "match_status_er", "match_status_manual", "error_matrix",
                "mismatches_code", "mismatches_columns", "mismatch_deviation", "mismatch_type", "validation_status",
                "batch_id"
                ).display()
# ----------
