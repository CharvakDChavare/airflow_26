from airflow.sdk import dag, task

@dag

def first_dag():

    @task.python
    def first_task():
        print("thsi is first task")

    @task.python
    def second_task():
        print("thsi is second task")

    @task.python
    def third_task():
            print("thsi is third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

first_dag()