from airflow.sdk import dag, task
# from airflow.operators.bash import BashOperator
@dag

def operators_dag():

    @task.python
    def first_task():
        print("thsi is first task")
    
    @task.python
    def second_task():
        print("thsi is second task")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"



   # bash_task = BashOperator(
    #task_id="bash_task",
    #bash_command="script.sh",
    #) 

    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    #bash = bash_task

    first >> second >> bash_modern #>> bash

operators_dag()