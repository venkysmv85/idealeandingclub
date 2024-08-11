import pytest
from libs.ConfigReader import get_app_config
from libs.Utils import get_spark_session
from libs.DataReader import read_orders, read_customers
from libs.DataManipulation import filter_closed_orders, count_orders_state,filter_orders_generic

#2 . Moved it to conftest.py
#@pytest.fixture()
#1. defining fixtures
#def spark():
#    return get_spark_session("LOCAL")

#def test_read_customer():
#1. after fixtures
def test_read_customer(spark):
#    spark = get_spark_session("LOCAL")
    customer_count = read_customers(spark,"LOCAL").count()
    assert customer_count == 12435

#def test_read_order():
#1. after fixtures
def test_read_order(spark):
#    spark = get_spark_session("LOCAL")
    order_count = read_orders(spark,"LOCAL").count()
    assert order_count == 68884

@pytest.mark.transformation()
def test_filter_closed_count(spark):
    order_df = read_orders(spark, "LOCAL")
    filter_closed_count = filter_closed_orders(order_df).count()
    assert filter_closed_count == 7556

@pytest.mark.transformation()
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.skip
def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark,"LOCAL")
    actual_result = count_orders_state(customers_df)
    assert actual_result.collect() == expected_results.collect()

@pytest.mark.generic()
@pytest.mark.parametrize(
    "status,count",
    [
        ("CLOSED",7556),
        ("PENDING_PAYMENT",15030),
        ("COMPLETE",22900)
    ]


)
def test_check_count(spark,status,count):
    order_df = read_orders(spark, "LOCAL")
    filter_closed_count = filter_orders_generic(order_df,status).count()
    assert filter_closed_count == count
#Run COmmands
#/Users/venkateshsangepu/IdeaProjects/RetailAnalysis/venv/bin/Python -m pytest -m "not transformation" -v