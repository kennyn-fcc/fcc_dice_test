import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, col, lit


sys.path.insert(0, os.getcwd())

from link_entity_builder import LinkBuilder
from config import Config

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

@pytest.fixture
def config(tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    input_path.mkdir()
    output_path.mkdir()
    return Config(base_input_path=str(input_path), base_output_path=str(output_path))

def test_generate_hash_key(spark, config):
    # Create a mock DataFrame
    data = [("1",), ("2",), ("3",)]
    columns = ["user_registration_id"]
    df = spark.createDataFrame(data, columns)

    # Initialize LinkBuilder
    builder = LinkBuilder(
        input_link_entity_name="user_plan",
        output_link_entity_name="user_registration_plan",
        link_entity_1="user_registration",
        link_entity_1_id_field="user_registration_id",
        link_entity_2="plan",
        link_entity_2_id_field="plan_id",
        spark=spark,
        config=config
    )

    # Generate hash key
    result_df = builder._generate_hash_key(df, "user_registration", "user_registration_id")

    # Verify the hash key column is generated correctly
    expected_df = df.withColumn(
        "user_registration_hash_key",
        sha2(concat(col("user_registration_id"), lit("user_registration_plan")), 256)
    )
    assert result_df.collect() == expected_df.collect()