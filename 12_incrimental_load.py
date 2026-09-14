from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
        schedule=CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
        start_date=datetime(year=2026, month=9, day=1, tz="Asia/Kolkata"),
        end_date=datetime(year=2026, month=9, day=7, tz="Asia/Kolkata"),
        catchup=True
)

def incrimental_load():

    @task.python
    def incrimental_data_fecth(**kwargs):

        incrimental_start_date = kwargs['data_interval_start']
        incrimental_end_date = kwargs['data_interval_end']
        print(f"Fecthing data form {incrimental_start_date} to {incrimental_end_date}")

    @task.bash
    def incrimental_data_process():

        return"""
        echo "Procssing Incrimental data form {{data_interval_start}} to {{data_interval_end}}" 
        """

    fecth_task = incrimental_data_fecth()
    process_task = incrimental_data_process()

    fecth_task >> process_task

incrimental_load()