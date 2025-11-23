
import numpy as np
import pandas as pd
from plugins.db_api_endpoints import ENDPOINTS
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


from pendulum import timezone

mex_tz = timezone("America/Mexico_City")

def generate_seasonal_weights(A, B,  current_day):
    t = current_day
    wt = A* np.abs(np.sin(2 * np.pi * t / 365 - B))
    return wt


def update_weights(df_regions, df_products, current_counter):
    
    for i,row in df_regions.iterrows():
        if row["state_name"] in ['Alabama', 'Arizona', 'California']:
            weights_df = generate_seasonal_weights(40, 90 , current_counter)
    
        elif row["state_name"] in ['Connecticut', 'Florida','Iowa', 'Illinois', 'Louisiana']:
            weights_df = generate_seasonal_weights(30, 90 , current_counter)
    
        elif row["state_name"] in ['Maine', 'Minnesota', 'Missouri', 'Montana', 'North Carolina']:
            weights_df = generate_seasonal_weights(25, 90 , current_counter)
    
        elif row["state_name"] in ['Nebraska', 'New Mexico', 'Ohio','Oklahoma']:
            weights_df = generate_seasonal_weights(15, 90 , current_counter)
    
        else:
            weights_df = generate_seasonal_weights(10, 90 , current_counter)
    
    
        region_id = row["id"]
        
        response = requests.put(ENDPOINTS['update_region_weight']+str(region_id),
            params={"new_weight": weights_df}
        )
    
    for i,row in df_products.iterrows():
        if row["category"] in ['Bakery', 'Beverages']:
            weights_df = generate_seasonal_weights(30, 90 , current_counter)
    
        elif row["category"] in ['Dairy', 'Deli', 'Frozen Foods']:
            weights_df = generate_seasonal_weights(40, 90 , current_counter)
    
        elif row["category"] in ['Meat','Pantry', 'Produce']:
            weights_df = generate_seasonal_weights(10, 90 , current_counter)
    
        elif row["category"] in ['Produce', 'Seafood', 'Snacks']:
            weights_df = generate_seasonal_weights(20, 90 , current_counter)
    
    
        product_id = row["id"]
        
        response = requests.put(ENDPOINTS['update_product_weight']+str(product_id),
            params={"new_weight": weights_df}
        )


def generate_new_data():


    response = requests.post(ENDPOINTS['login'],
                        json={
                            "email": "user@example.com",
                            "password": "string"
                            })
    
    token = response.json().get('access_token')
    headers = {"Authorization": f"Bearer {token}"}
    
    
    
    response = requests.post(ENDPOINTS["newdata"], json={
          "days": 1
        }, headers= headers)
    


def call_function():
    today = datetime.now()
    day_of_year = today.timetuple().tm_yday
    
    
    response_regions = requests.get(ENDPOINTS["get_region_name"])
    data_regions = response_regions.json()
    df_regions = pd.DataFrame(data_regions)


    response_products = requests.get(ENDPOINTS["get_product_name"])
    data_products = response_products.json()
    df_products = pd.DataFrame(data_products)



    update_weights(df_regions, df_products, current_counter=day_of_year)
    generate_new_data()






with DAG(
    "GET_DATA",
    schedule_interval="31 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=mex_tz),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="GETTING_NEW_DATA",
        python_callable=call_function
    )