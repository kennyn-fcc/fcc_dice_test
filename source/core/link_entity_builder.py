from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, col, lit
import os
from .config import Config


class LinkBuilder:
    def __init__(self, input_link_entity_name: str, output_link_entity_name: str, link_entity_1: str, link_entity_1_id_field: str,
                 link_entity_2: str, link_entity_2_id_field: str, spark: SparkSession, config: Config):
        self.output_link_entity_name = output_link_entity_name
        self.input_link_entity_name = input_link_entity_name
        self.link_entity_1 = link_entity_1
        self.link_entity_1_id_field = link_entity_1_id_field
        self.link_entity_2 = link_entity_2
        self.link_entity_2_id_field = link_entity_2_id_field
        self.spark = spark
        self.config = config
        self.input_path = os.path.join(self.config.get_input_path(), f"{input_link_entity_name}.csv")
        self.output_path = os.path.join(self.config.get_output_path(), f"link_{output_link_entity_name}")

    def _generate_hash_key(self, df, entity_name, id_field):
        return df.withColumn(f"{entity_name}_hash_key", sha2(concat(col(id_field), lit(self.output_link_entity_name)), 256))

    def build(self):
        if not self.input_path:
            raise ValueError(f"{self.input_path} is not valid.")

        # Read the input CSV
        df = self.spark.read.csv(self.input_path, header=True)

        # Generate hash keys for the two link fields
        df = self._generate_hash_key(df, self.link_entity_1, self.link_entity_1_id_field)
        df = self._generate_hash_key(df, self.link_entity_2, self.link_entity_2_id_field)

        # Select only the hash keys and write the output CSV
        output_columns = [f"{self.link_entity_1}_hash_key", f"{self.link_entity_2}_hash_key"]
        df.select(output_columns).write.csv(self.output_path, header=True, mode="overwrite")