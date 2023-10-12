from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@task
def check_dependencies():
    import importlib

    required_packages = ["numpy", "pyspark", "win32com", "openpyxl"]
    missing_packages = []

    for package in required_packages:
        try:
            importlib.import_module(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        raise ImportError(f"Bibliotecas não encontradas: {', '.join(missing_packages)}")


@dag(
    schedule_interval="0 7 * * *",
    start_date=datetime(2023, 10, 12),
    catchup=False,
    tags=["data"],
)
def data_processing_dag():
    check = check_dependencies()

    fetch_data_task = BashOperator(
        task_id="fetch_data",
        bash_command="python fetch_data.py",
    )

    process_data_task = BashOperator(
        task_id="process_data",
        bash_command="python process_data.py",
    )

    check >> fetch_data_task >> process_data_task


data_processing_dag_instance = data_processing_dag()
