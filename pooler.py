"""
Pooling handled missing value
# will be enhanced later
"""

from func import *
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    data_col_name = (
        "mv_handled_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"
    handled_col_name = "anomaly_handled_data"

    # Load data
    handled_df = load_data(app_conf)

    # Pool
    pooled_df = pool(handled_df, time_col_name)

    # Save pooled data to dwh
    save_pooled_data_to_dwh(
        ts=pooled_df,
        app_conf=app_conf,
        time_col_name=time_col_name,
        data_col_name=data_col_name,
        handled_col_name=handled_col_name
    )
    # todo: store offline report
    # todo: PRIORITY HIGH: enhance task and all relative logic to deal with already marked df or seperatly save unmarked or etc.
    spark.stop()
