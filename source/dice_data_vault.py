from pyspark.sql import SparkSession
from core.config import Config
from core.hub_entity_builder import HubEntityBuilder
from core.hub_satellite_entity_buidler import HubSatelliteBuilder
from core.link_entity_builder import LinkBuilder
from core.field_mapping import FieldMapping

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("PySpark Reading and Analyzing Data")
        .getOrCreate()
    )

    config = Config(base_input_path="../source_data", base_output_path="../data_vault")

    # Hub entities
    users_builder = HubEntityBuilder(entity_name="user", id_field="user_id", spark=spark, config=config)
    users_builder.build()

    plan_builder = HubEntityBuilder(entity_name="plan", id_field="plan_id", spark=spark, config=config)
    plan_builder.build()

    user_registration_builder = HubEntityBuilder(entity_name="user_registration", id_field="user_registration_id",
                                                 spark=spark, config=config)
    user_registration_builder.build()

    play_session_builder = HubEntityBuilder(entity_name="user_play_session", id_field="play_session_id", spark=spark, config=config)
    play_session_builder.build()

    user_payment_builder = HubEntityBuilder(entity_name="user_payment_detail", id_field="payment_detail_id", spark=spark,
                                            config=config)
    user_payment_builder.build()

    # HubSatellites

    user_hs_field_mappings = [
        FieldMapping(input_field="ip_address", output_field="ip_address", data_type="String"),
        FieldMapping(input_field="social_media_handle", output_field="social_media_handle", data_type="String"),
        FieldMapping(input_field="email", output_field="email", data_type="String")
    ]

    user_satellite_builder = HubSatelliteBuilder(
        entity_name="user",
        spark=spark,
        config=config,
        id_field="user_id",
        field_mappings=user_hs_field_mappings
    )
    user_satellite_builder.build()

    user_reg_hs_field_mappings = [
        FieldMapping(input_field="username", output_field="username", data_type="String"),
        FieldMapping(input_field="email", output_field="email", data_type="String"),
        FieldMapping(input_field="first_name", output_field="first_name", data_type="String"),
        FieldMapping(input_field="last_name", output_field="last_name", data_type="String")
    ]

    user_reg_satellite_builder = HubSatelliteBuilder(
        entity_name="user_registration",
        spark=spark,
        config=config,
        id_field="user_registration_id",
        field_mappings=user_reg_hs_field_mappings
    )
    user_satellite_builder.build()

    user_play_session_hs_field_mappings = [
        FieldMapping(input_field="start_datetime", output_field="start_datetime", data_type="String"),
        FieldMapping(input_field="end_datetime", output_field="end_datetime", data_type="String"),
        FieldMapping(input_field="channel_code", output_field="channel_code", data_type="String"),
        FieldMapping(input_field="status_code", output_field="status_code", data_type="String"),
        FieldMapping(input_field="total_score", output_field="total_score", data_type="String")

    ]

    user_play_session_satellite_builder = HubSatelliteBuilder(
        entity_name="user_play_session",
        spark=spark,
        config=config,
        id_field="play_session_id",
        field_mappings=user_play_session_hs_field_mappings
    )
    user_play_session_satellite_builder.build()

    user_payment_hs_field_mappings = [
        FieldMapping(input_field="payment_method_code", output_field="payment_method_code", data_type="String"),
        FieldMapping(input_field="payment_method_value", output_field="payment_method_value", data_type="String"),
        FieldMapping(input_field="payment_method_expiry", output_field="payment_method_expiry", data_type="String"),

    ]

    user_payment_session_satellite_builder = HubSatelliteBuilder(
        entity_name="user_payment_detail",
        spark=spark,
        config=config,
        id_field="payment_detail_id",
        field_mappings=user_payment_hs_field_mappings
    )
    user_play_session_satellite_builder.build()


    # Create a LinkBuilder instance
    link_builder = LinkBuilder(
        input_link_entity_name="user_plan",
        output_link_entity_name="user_registration_plan",
        link_entity_1="user_registration",
        link_entity_1_id_field="user_registration_id",
        link_entity_2="plan",
        link_entity_2_id_field="plan_id",
        spark=spark,
        config=config
    )

    # Build the link entity
    link_builder.build()