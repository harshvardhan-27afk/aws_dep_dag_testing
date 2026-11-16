

from dag_parser.dynamic.operators import DAG, PythonOperator, BashOperator


def start_fn(**kwargs):
    print("start: kicking off the diamond")


with DAG(
    dag_id="py_bash_test",
    schedule=None,
    start_date="2026-01-01",
    tags=["p_tester"],
    catchup=False,
) as dag:

    a = PythonOperator(task_id="a", python_callable=start_fn)

    b = BashOperator(task_id="b", bash_command="echo 'task_b: running'")
    c = BashOperator(task_id="c", bash_command="echo 'task_c: running'")

    

    [a,b] >> c
