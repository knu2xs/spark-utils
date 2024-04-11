"""
This is a stubbed out test file designed to be used with PyTest, but can 
easily be modified to support any testing framework.
"""

from pathlib import Path
import sys

# get paths to useful resources - notably where the src directory is
self_pth = Path(__file__)
dir_test = self_pth.parent
dir_prj = dir_test.parent
dir_src = dir_prj / "src"

# insert the src directory into the path and import the projct package
sys.path.insert(0, str(dir_src))
import spark_utils


def test_get_spark_session_defaults():
    from pyspark.sql import SparkSession
    from geoanalytics import auth_info

    spark = spark_utils.get_spark_session()
    assert isinstance(spark, SparkSession)
    assert auth_info().toPandas().set_index("name").loc["authorized"].iloc[0] == "true"
