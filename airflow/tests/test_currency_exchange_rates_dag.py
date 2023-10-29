from airflow.models import DagBag

#A basic test to check for import errors
dag_id = 'retrieve_currency_exchange_rates_dag'

def test_no_import_errors():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id=dag_id)
    
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag is not None

    