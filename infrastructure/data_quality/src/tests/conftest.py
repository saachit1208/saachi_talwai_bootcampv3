import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local[2]") \
      .appName("chispa") \
      .getOrCreate()