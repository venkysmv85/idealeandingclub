import pytest
from libs.Utils import get_spark_session

@pytest.fixture()
#1. defining fixtures
def spark():
#    return get_spark_session("LOCAL")
    "Creating Spark Session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session # Release the resources
    spark_session.stop()

@pytest.fixture()
def expected_results(spark):
    "give the expected result"
    result_schema = "state string, count int"
    return spark.read \
            .format("csv") \
            .schema(result_schema) \
            .load("data/test_result/state_aggregate.csv")