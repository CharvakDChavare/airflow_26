from airflow.sdk import dag, task

@dag(
    dag_id= "parallel_manual_dag"
)

def parallel_manual_dag():

    @task.python
    def extract_task(**kwargs):

        ti =kwargs['ti']

        print("Extracting data..")

        extract_data_dict ={
            "api_extracted_data":[1,2,3],
            "db_extracted_data":[4,5,6],
            "s3_extracted_data":[7,8,9]
        }

        ti.xcom_push(key="extract_data", value=extract_data_dict)

    @task.python
    def transform_task_api(**kwargs):

        ti = kwargs['ti']

        extract_data_dict = ti.xcom_pull(task_ids = "extract_task", key = "extract_data")

        api_extracted_data = extract_data_dict["api_extracted_data"]
        print(f"Transform api data: {api_extracted_data} .")

        transformed_api_data = [i * 10 for i in api_extracted_data]

        ti.xcom_push(key="api_data", value = transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):

        ti = kwargs['ti']

        extract_data_dict = ti.xcom_pull(task_ids = "extract_task", key = "extract_data")

        db_extracted_data = extract_data_dict["db_extracted_data"]
        print(f"Transform db data: {db_extracted_data} .")

        transform_db_data = [i * 100 for i in db_extracted_data]

        ti.xcom_push(key="db_data", value = transform_db_data)

    @task.python
    def transform_task_s3(**kwargs):

        ti = kwargs['ti']

        extract_data_dict = ti.xcom_pull(task_ids = "extract_task", key = "extract_data")

        s3_extracted_data = extract_data_dict["s3_extracted_data"]
        print(f"Transform s3 data: {s3_extracted_data} .")

        transform_s3_data = [i * 1000 for i in s3_extracted_data]

        ti.xcom_push(key="s3_data", value= transform_s3_data)

    @task.python
    def load_task(**kwargs):
        ti = kwargs['ti']

        api_data = ti.xcom_pull(task_ids = "transform_task_api", key = "api_data")

        db_data = ti.xcom_pull(task_ids = "transform_task_db", key = "db_data")

        s3_data = ti.xcom_pull(task_ids = "transform_task_s3", key = "s3_data")

        print("Loading data to destination..")

        load_data = {
                "api_data": api_data,
                "db_data": db_data,
                "s3_data": s3_data
            }

        print(f"Data loaded: {load_data}")

    # Dependencies

    extract = extract_task()

    api = transform_task_api()
    db = transform_task_db()
    s3 = transform_task_s3()

    load = load_task()

    extract >> [api, db, s3] >> load

parallel_manual_dag()