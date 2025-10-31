from deltalake import DeltaTable, write_deltalake

from databricks.connect import DatabricksSession

from delta import *
from pyspark.sql.functions import *
import duckdb
import pandas as pd
import os
import shutil
import math
import glob

CATALOG = 'duckdblabs_testing.'
SCHEMA = 'main.'

def sanitize_path(input: str):
    return input.replace('/', '_').replace(':', '_').replace('-', '_')

def generate_test_data_pyspark(base_path, name, current_path, input_path, delete_predicate = False, partition_column = None, mapping_mode = None):
    """
    generate_test_data_pyspark generates some test data using pyspark and duckdb

    :param current_path: the test data path
    :param input_path: the path to an input parquet file
    :return: describe what it returns
    """

    full_path = 's3://duckdb-databricks-testing-2/duckdblabs_testing/main' + '/' + current_path

    try:
        ## SPARK SESSION
        spark = DatabricksSession.builder.serverless().profile('DEFAULT').getOrCreate()

        ## CONFIG
        delta_table_path = full_path + '/delta_lake'
        ## CREATE DIRS
        os.makedirs(delta_table_path, exist_ok=True)

        original_input_path = input_path[4:]
        input_path = 's3://duckdb-databricks-testing-2/thijs-tmp-test-data-3/' + original_input_path
        print(input_path)
        # DATA GENERATION
        df = spark.read.parquet(input_path)

        temp_table = f"{CATALOG}{SCHEMA}temp_{sanitize_path(original_input_path.split('.parquet')[0])}"
        df.write.format("delta").saveAsTable(temp_table, overwrite=True)

        if (partition_column):
            spark.sql(f"CREATE OR REPLACE TABLE {name} USING delta LOCATION '{delta_table_path}' PARTITIONED BY ({partition_column}) AS SELECT * FROM {temp_table}")
        else:
            spark.sql(f"CREATE OR REPLACE TABLE {name} USING delta LOCATION '{delta_table_path}' AS SELECT * FROM {temp_table}")

        if mapping_mode == 'name' or mapping_mode == 'id':
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7', 'delta.columnMapping.mode' = '{mapping_mode}');")
        elif mapping_mode is None:
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7');")
        else:
            raise f"Unknown mapping mode: {mapping_mode}"

        ## CREATE
        ## CONFIGURE USAGE OF DELETION VECTORS
        if (delete_predicate):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")

        ## ADDING DELETES
        deltaTable = DeltaTable.forPath(spark, delta_table_path)
        if delete_predicate:
            deltaTable.delete(delete_predicate)

    except:
        if (os.path.isdir(full_path)):
            shutil.rmtree(full_path)
        raise

def generate_test_data_pyspark_by_queries(base_path, name, current_path, base_query, queries, mapping_mode = None):
    """
    schema_evolve_pyspark_deltatable generates some test data using pyspark and duckdb

    :param current_path: the test data path
    :param input_path: the path to an input parquet file
    :return: describe what it returns
    """

    full_path = 's3://duckdb-databricks-testing-2/duckdblabs_testing/main' + '/' + current_path

    try:
        ## SPARK SESSION
        spark = DatabricksSession.builder.serverless().profile('DEFAULT').getOrCreate()

        ## CONFIG
        delta_table_path = full_path + '/delta_lake'

        ## CREATE DIRS
        os.makedirs(delta_table_path, exist_ok=True)

        ## DATA GENERATION
        # df = spark.read.parquet(input_path)
        # df.write.format("delta").mode("overwrite").save(delta_table_path)

        if mapping_mode == 'name' or mapping_mode == 'id':
            spark.sql(
                f"CREATE OR REPLACE TABLE {name} USING delta TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5', 'delta.columnMapping.mode' = '{mapping_mode}', 'delta.enableTypeWidening' = 'true') LOCATION '{delta_table_path}' AS {base_query};")
        elif mapping_mode is None:
            spark.sql(f"CREATE OR REPLACE TABLE {name} USING delta TBLPROPERTIES ('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5', 'delta.enableTypeWidening' = 'true') LOCATION '{delta_table_path}' AS {base_query};")
        else:
            raise f"Unknown mapping mode: {mapping_mode}"

        for query in queries:
            spark.sql(query)

    except:
        if (os.path.isdir(full_path)):
            shutil.rmtree(full_path)
        raise


__all__ = ["generate_test_data_pyspark_by_queries", "generate_test_data_pyspark"]