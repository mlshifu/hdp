import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F


def get_spark_data_type(data_type):
    """
    Map string data types to Spark data types.
    """
    if data_type.lower() == "string":
        return StringType()
    elif data_type.lower() == "integer":
        return IntegerType()
    elif data_type.lower() == "timestamp":
        return TimestampType()
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


def build_spark_schema(schema_config):
    """
    Build Spark schema from the target schema configuration in the JSON.
    """
    fields = [
        StructField(column['target_column'], get_spark_data_type(column['target_type']), True)
        for column in schema_config
    ]
    return StructType(fields)


def main(config_path):
    # Load configuration from JSON
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
    except Exception as e:
        raise Exception(f"Error reading configuration file: {e}")

    # Extract Oracle and output details
    oracle_config = config.get("oracle", {})
    output_config = config.get("output", {})
    schema_config = config.get("schema", {}).get("columns", [])

    # Validate configuration
    if not oracle_config or not output_config or not schema_config:
        raise ValueError("Invalid configuration: Missing Oracle, Output, or Schema details")

    oracle_url = oracle_config.get("url")
    oracle_user = oracle_config.get("user")
    oracle_password = oracle_config.get("password")
    oracle_table = oracle_config.get("table")
    where_clause = oracle_config.get("where_clause", "")
    src_partition_column = oracle_config.get("src_partition_column")

    hdfs_path = output_config.get("path")
    output_format = output_config.get("format", "parquet")  # Default to Parquet
    partition_column = output_config.get("partition_column")

    if not (oracle_url and oracle_user and oracle_password and oracle_table and hdfs_path):
        raise ValueError("Missing required Oracle or Output configuration parameters")

    # Build the target Spark schema
    target_schema = build_spark_schema(schema_config)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("OracleToHDFSWithPartitioning") \
        .getOrCreate()

    # Read data from Oracle
    try:
        jdbc_read_options = {
            "url": oracle_url,
            "dbtable": f"({oracle_table} WHERE {where_clause})" if where_clause else oracle_table,
            "user": oracle_user,
            "password": oracle_password
        }

        # If source partition column is provided, use it for partitioning
        if src_partition_column:
            jdbc_read_options.update({
                "partitionColumn": src_partition_column,
                "lowerBound": "0",  # Set a reasonable lower bound based on your data
                "upperBound": "1000",  # Set a reasonable upper bound based on your data
                "numPartitions": "10"  # Number of partitions to split the read operation
            })

        df = spark.read \
            .format("jdbc") \
            .options(**jdbc_read_options) \
            .load()
    except Exception as e:
        raise Exception(f"Error reading data from Oracle: {e}")

    # Select and rename source columns to target columns
    try:
        # Create a mapping of source columns to target columns
        select_expr = [
            F.col(column['src_column']).cast(get_spark_data_type(column['target_type'])).alias(column['target_column'])
            for column in schema_config
        ]

        # Apply the mapping
        df = df.select(*select_expr)
    except Exception as e:
        raise Exception(f"Error applying column mapping: {e}")

    # Write data to HDFS with partitioning
    try:
        if partition_column:
            df.write \
                .format(output_format) \
                .mode("overwrite") \
                .partitionBy(partition_column) \
                .save(hdfs_path)
        else:
            df.write \
                .format(output_format) \
                .mode("overwrite") \
                .save(hdfs_path)

        print(f"Data successfully written to HDFS at {hdfs_path}")
    except Exception as e:
        raise Exception(f"Error writing data to HDFS: {e}")

    # Stop SparkSession
    spark.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: spark-submit --master <MASTER> oracle_to_hdfs_with_partitioning.py <CONFIG_PATH>")
        sys.exit(1)

    config_file = sys.argv[1]
    main(config_file)
