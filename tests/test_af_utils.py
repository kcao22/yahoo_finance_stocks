import os

from apps.af_utils import get_dag_name


def test_get_dag_name():
    # expect ingress_yahoo_daily_stocks
    mock_path = os.path.join(
        "dags", "constructors", "ingress", "yahoo", "daily_stocks.py"
    )
    assert get_dag_name(mock_path) == "ingress_yahoo_daily_stocks"


def test_get_dag_name_utility():
    # expect utility_cleanup
    mock_path = os.path.join("dags", "constructors", "utility", "cleanup.py")
    assert get_dag_name(mock_path) == "utility_cleanup"
    mock_path = os.path.join("dags", "constructors", "utility", "cleanup.py")
    assert get_dag_name(mock_path) == "utility_cleanup"
