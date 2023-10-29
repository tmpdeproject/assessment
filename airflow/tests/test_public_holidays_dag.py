from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='retrieve_bank_holidays_dag')
    
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag is not None

    