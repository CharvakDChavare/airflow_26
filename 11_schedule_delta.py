from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        dag_id="delta_schedule_dag",
        start_date = datetime(
             year = 2026,
             month= 9,
             day = 1, 
             tz="Asia/Kolkata"
             ),
        schedule = DeltaTriggerTimetable(duration(days=3)),
        end_date= datetime(year=2026, month=9, day=30, tz="Asia/Kolkata"),
        is_paused_upon_creation=False,
        catchup=True
)

def delta_schedule_dag():

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

delta_schedule_dag()