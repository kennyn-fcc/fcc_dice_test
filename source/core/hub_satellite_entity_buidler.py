from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from typing import List
import os
from .config import Config
from .field_mapping import FieldMapping

class HubSatelliteBuilder:
    def __init__(self, entity_name: str, spark: SparkSession, config: Config, id_field: str, field_mappings: List[FieldMapping]):
        if not id_field.isidentifier():
            raise ValueError("ID field must be a valid column name.")
        self.spark = spark
        self.config = config
        self.entity_name = entity_name
        self.id_field = id_field
        self.field_mappings = field_mappings
        self.input_path = os.path.join(self.config.get_input_path(), f"{entity_name}.csv")
        self.output_path = os.path.join(self.config.get_output_path(), f"hub_satellite_{entity_name}")

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

    def _apply_field_mappings(self, df):
        for mapping in self.field_mappings:
            if mapping.data_type == "String":
                data_type = StringType()
            elif mapping.data_type == "Integer":
                data_type = IntegerType()
            elif mapping.data_type == "Float":
                data_type = FloatType()
            elif mapping.data_type == "Date":
                data_type = DateType()
            else:
                raise ValueError(f"Unsupported data type: {mapping.data_type}")
            df = df.withColumn(mapping.output_field, col(mapping.input_field).cast(data_type))
        return df

    def build(self):
        if not self.input_path:
            raise ValueError(f"{self.input_path} is not valid.")

        # Read the input CSV
        df = self.spark.read.csv(self.input_path, header=True)

        # Validate IDs
        df = self._validate_ids(df)

        # Generate hash keys
        df = self._generate_hash_key(df)

        # Apply field mappings
        df = self._apply_field_mappings(df)

        # Select the required columns and write the output CSV
        output_columns = ["hash_key"] + [mapping.output_field for mapping in self.field_mappings]
        df.select(output_columns).write.csv(self.output_path, header=True, mode="overwrite")