from airflow.sdk import dag, task

@dag(
        dag_id = "parallel_dag"
)

def parallel_dag():

    @task.python
    def extract_task():
        print("Extracting data..")

        
        extract_data_dict = {"api_extracted_data": [1,2,3],
                             "db_extracted_data": [4,5,6],
                             "s3_extracted_data": [7,8,9]}

        return extract_data_dict
    
    @task.python
    def transform_task_api(extract_data_dict:dict):

        api_extracted_data = extract_data_dict["api_extracted_data"]

        print(f"Transforming api data : {api_extracted_data}..")

        transformed_api_data = [i*10 for i in api_extracted_data]

        return transformed_api_data

    @task.python
    def transform_task_db(extract_data_dict:dict):

        db_extracted_data = extract_data_dict["db_extracted_data"]
        print(f"Transforming db data : {db_extracted_data}..")

        transformed_db_data = [i*100 for i in db_extracted_data]

        return transformed_db_data

    @task.python
    def transform_task_s3(extract_data_dict:dict):

        s3_extracted_data = extract_data_dict["s3_extracted_data"]
        print(f"Transforming s3 data : {s3_extracted_data}..")

        transformed_s3_data = [i*1000 for i in s3_extracted_data]

        return transformed_s3_data
        
    @task.python
    def load_task(api_data, db_data, s3_data):
        print("Loading data to destination....")

        load_data  = {
            "api_data": api_data,
            "db_data": db_data,
            "s3_data": s3_data
        }
        print(f"Data loaded:{load_data}")
        return load_data

   #dependances

    extract = extract_task()
    api_data = transform_task_api(extract)
    db_data = transform_task_db(extract)
    s3_data = transform_task_s3(extract)
    loaded_data = load_task(api_data, db_data, s3_data)

parallel_dag()