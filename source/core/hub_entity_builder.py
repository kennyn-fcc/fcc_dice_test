import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, col, lit
from .config import Config


class HubEntityBuilder:
    def __init__(self, entity_name: str, spark: SparkSession, config: Config, id_field: str = "id"):
        if not id_field.isidentifier():
            raise ValueError("ID field must be a valid column name.")
        self.spark = spark
        self.config = config
        self.entity_name = entity_name
        self.id_field = id_field
        self.input_path = os.path.join(self.config.get_input_path(), f"{entity_name}.csv")
        self.output_path = os.path.join(self.config.get_output_path(), f"hub_{entity_name}")

    def with_input_path(self, path: str):
        self.input_path = path
        return self

    def with_output_path(self, path: str):
        self.output_path = path
        return self

    def _validate_ids(self, df):
        return df.filter((col(self.id_field).isNotNull()) & (col(self.id_field) != ""))

    def _generate_hash_key(self, df):
        return df.withColumn("hash_key", sha2(concat(col(self.id_field), lit(self.entity_name)), 256))

    def build(self):
        if not self.input_path:
            raise ValueError(f"{self.input_path} is not valid.")

        # Read the input CSV
        df = self.spark.read.csv(self.input_path, header=True)

        # Validate IDs
        df = self._validate_ids(df)

        # Generate hash keys
        df = self._generate_hash_key(df)

        # Select the required columns and write the output CSV
        df.select(self.id_field, "hash_key").write.csv(self.output_path, header=True, mode="overwrite")