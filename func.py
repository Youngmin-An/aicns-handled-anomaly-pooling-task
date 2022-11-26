"""
Function level adapters
"""
import os
import pendulum
from pendulum import DateTime
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
import logging

__all__ = [
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_data",
    "pool",
    "save_pooled_data_to_dwh"
]


def get_conf_from_evn():
    """
    Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]

        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_data(app_conf) -> DataFrame:
    """

    :param feature_id:
    :return:
    """
    logger = SparkSession.getActiveSession()._jvm.org.apache.log4j.LogManager.getLogger(
        __name__
    )
    table_name = "cleaned_handled_anomaly"
    # Inconsistent cache
    # https://stackoverflow.com/questions/63731085/you-can-explicitly-invalidate-the-cache-in-spark-by-running-refresh-table-table
    SparkSession.getActiveSession().sql(f"REFRESH TABLE {table_name}")
    query = f"""
    SELECT v.*
        FROM (
            SELECT *, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {table_name}
            WHERE feature_id={app_conf['FEATURE_ID']}
            ) v 
        WHERE v.date  >= {app_conf['start'].format('YYYYMMDD')} AND v.date <= {app_conf['end'].format('YYYYMMDD')} 
    """
    logger.info("load handled data query: " + query)
    handled_df = SparkSession.getActiveSession().sql(query)
    logger.info(handled_df.show())
    return handled_df.drop("date")


def pool(handled_df: DataFrame, time_col_name: str) -> DataFrame:
    """

    :param handled_df:
    :return:
    """
    # todo: when refactoring, migrate this logic
    return __simple_pooling(handled_df, time_col_name)


def __simple_pooling(handled_df: DataFrame, time_col_name: str) -> DataFrame:
    """
    Simple pooling handled value method by LOCF Imputation
    :param handled_df:
    :return:
    """
    return handled_df.filter(F.col("anomaly_handling_strategy") == "ExponentialMovingAverage").sort(time_col_name)


def __append_partition_cols(ts: DataFrame, time_col_name: str, data_col_name: str, handled_col_name: str, feature_id: str):
    return (
        ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000))
        .select(
            time_col_name,
            data_col_name,
            handled_col_name,
            "count",
            "is_missingvalue_count",
            "is_unmarked_missingvalue_count",
            "is_anomaly",
            "anomaly_handling_strategy",
            F.lit(feature_id).alias("feature_id"),
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        )
        .sort(time_col_name)
    )


def save_pooled_data_to_dwh(
    ts: DataFrame, app_conf, time_col_name: str, data_col_name: str, handled_col_name: str
):
    """

    :param ts:
    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :param handled_col_name:
    :return:
    """
    # todo: transaction
    logger = SparkSession.getActiveSession()._jvm.org.apache.log4j.LogManager.getLogger(
        __name__
    )
    table_name = "cleaned_pooled_handled_anomaly"
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE, {handled_col_name} DOUBLE, count INT, is_missingvalue_count INT, is_unmarked_missingvalue_count INT, is_anomaly BOOLEAN, anomaly_handling_strategy CHAR(30)) PARTITIONED BY (feature_id CHAR(10), year int, month int, day int) STORED AS PARQUET"
    logger.debug(f"creating table query: {query}")
    SparkSession.getActiveSession().sql(query)

    period = pendulum.period(app_conf["start"], app_conf["end"])

    # Create partition columns(year, month, day) from timestamp
    partition_df = __append_partition_cols(
        ts, time_col_name, data_col_name, handled_col_name, app_conf["FEATURE_ID"]
    )

    for date in period.range("days"):
        # Drop Partition for immutable task
        query = f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(feature_id={app_conf['FEATURE_ID']}, year={date.year}, month={date.month}, day={date.day})"
        logger.debug(f"Partition check query: {query}")
        SparkSession.getActiveSession().sql(query)
    # Save
    partition_df.write.format("hive").mode("append").insertInto(table_name)
