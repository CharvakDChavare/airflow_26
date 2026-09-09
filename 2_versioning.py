from airflow.sdk import dag, task

@dag

def versioning_dag():

    @task.python
    def first_task():
        print("thsi is first task")

    @task.python
    def second_task():
        print("thsi is second task")

    @task.python
    def third_task():
            print("thsi is third task, DAG FIRST VERSION")

    @task.python
    def fourth_task():
         print("this is fourth task, DAG SECOND VERSION")

    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()

    first >> second >> third >> fourth

versioning_dag()