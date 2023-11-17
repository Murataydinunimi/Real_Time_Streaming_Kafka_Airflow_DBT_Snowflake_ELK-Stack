import os
import pandas as pd
import joblib
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv(dotenv_path='azure.env')
import json
from datetime import datetime


connection_string = os.getenv('connection_string')


def predict():

    file_path = os.path.join(os.getcwd(), 'ML/mart_data','result.csv')
    ml_mart_df = pd.read_csv(file_path)

    model_path = os.path.join(os.getcwd(), 'ML/model','random_forest_model.pkl')
    model = joblib.load(model_path)

    feature_names = model.feature_names_in_
    categorical_features = ['booking_source', 'time_of_day', 'day_of_week', 'weather_conditions', 'traffic_conditions']
    fake_df_encoded = pd.get_dummies(ml_mart_df, columns=categorical_features, drop_first=True)
    fake_df_encoded = fake_df_encoded.reindex(columns=feature_names, fill_value=0)

    probs = model.predict_proba(fake_df_encoded)  # Probabilities for both classes

    ml_mart_df['prob_of_cancellation'] = probs[:,0]

    file_path_predictions = os.path.join(os.getcwd(), 'ML/mart_data/','predictions','predictions.csv')

    ml_mart_df.to_csv(file_path_predictions, index=False)



def move_file_between_containers(source_container_name,destination_container_name):
    
    blob_name_dict = {'taxi-app-data':'transactions.json','predictions':'predictions.json'}    
    source_blob_name = blob_name_dict[source_container_name]

    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    destination_file_name = f"{current_timestamp}_{source_container_name}_uploaded.json"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    source_container_client = blob_service_client.get_container_client(source_container_name)
    source_blob_client = source_container_client.get_blob_client(source_blob_name)

    destination_container_client = blob_service_client.get_container_client(destination_container_name)
    destination_blob_client = destination_container_client.get_blob_client(destination_file_name)

    destination_blob_client.start_copy_from_url(source_blob_client.url)

    source_blob_client.delete_blob()




def write_ml_mart_data(**kwargs):
    ti = kwargs['ti']
    snowflake_task_result = ti.xcom_pull(task_ids='get_ml_mart_data')

    if not snowflake_task_result:
        raise ValueError("Snowflake task did not return any result.")

    # Assuming the result is stored as a DataFrame in XCom
    df = pd.DataFrame(snowflake_task_result)
    df.columns = [column.lower() for column in df.columns]



    file_path = os.path.join(os.getcwd(), 'ML/mart_data','result.csv')
    
    df.to_csv(file_path, index=False)

    print(f'file is written to {file_path} with len {len(df)}')



def load_predictions():

    file_path = os.path.join(os.getcwd(), 'ML/mart_data/','predictions','predictions.csv')
    df = pd.read_csv(file_path)
    container_name = 'predictions'
    json_string = df.to_json(orient='records', lines=True)
    json_objects = json_string.strip().split('\n')

    # Create a list to store the parsed JSON objects
    parsed_objects = []

    # Parse each JSON object and store it in the list
    for json_obj_str in json_objects:
        parsed_obj = json.loads(json_obj_str)
        parsed_objects.append(parsed_obj)
    

    json_file = json.dumps(parsed_objects)
   


    
    blob_name = "predictions.json"       

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)

    blob_client.upload_blob(json_file)

    print(f"Uploaded file to {blob_client.url}")