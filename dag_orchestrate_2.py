from airflow.sdk import dag, task
import os
@dag

def second_orchestrateor_dag():

    @task.python
    def first_task():
        print("thsi is first task")

    @task.python
    def second_task():
        print("thsi is second task")

    @task.python
    def third_task():
        os.makedirs(os.path.dirname("/opt/airflow/logs/data/output_2.txt"), exist_ok=True )

        with open("/opt/airflow/logs/data/output_2.txt", 'w') as f:
            f.write(f"Data fecthed successfully") 


    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

second_orchestrateor_dag()