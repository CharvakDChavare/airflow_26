from airflow.sdk import dag, task

@dag

def xcom_auto():

    @task.python
    def first_task():
        print("Extraction data... , This is the first task")
        fetched_data =  {"data" : [1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data:dict):
        print("Transform data..., this is the second task")
        fetched_data = data['data']
        transform_data = fetched_data*2
        transform_data_dict = {"transf_data": transform_data}
        return transform_data_dict

    @task.python
    def third_task(data:dict):

        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcom_auto()