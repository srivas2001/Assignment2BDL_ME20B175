from airflow import DAG
import random 
import os
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import zipfile
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import pandas as pd
import geopandas as gpd
import geopandas as gpd 
import matplotlib.pyplot as plt
dag=DAG(
    'Analytics_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    catchup=False
)
path_zip="/opt/airflow/Climate_dataset2001.zip"
path_unzip="/opt/airflow/Climate_dataset2001"
#Task 1
check_available=FileSensor(
    task_id="archive_available",
    filepath=path_zip,
    timeout=5,
    poke_interval=1,
    dag=dag,
)
stop_pipeline_task = BashOperator(
    task_id='stop_pipeline',
    bash_command='exit 1',
    trigger_rule='one_failed', 
    dag=dag,
)
#Task 2
unzip_task=BashOperator(
    task_id='unzip_archive',
    bash_command=f'if unzip -tq {path_zip}; then unzip -o {path_zip} -d {path_unzip}; else exit 1; fi',
    dag=dag,
    trigger_rule='all_success',
)
beam_options=PipelineOptions(
    runner="DirectRunner",
    project="my-project",
    temp_location="/tmp",
    direct_num_workers=10,
    direct_running_mode='multi_processing'
)
def csv_df(folder_path,csv_file):
    csv_path = os.path.join(folder_path, csv_file)
    with open(csv_path, 'r') as file:
        # Read CSV file and process it
        df=pd.read_csv(file)
    tuple_data=[]
    for index,row in df.iterrows():
        lat=row['LATITUDE']
        long=row['LONGITUDE']
        date=row['DATE'].dt.month
        hourly_temp=row['HourlyDryBulbTemperature']
        hourly_speed=row['HourlyWindSpeed']
        tuple_data.append((date,lat,long,hourly_temp,hourly_speed))
    return tuple_data
def csv_to_dffilter(path_folder): # This function converts the csv file into the required tuple format
    with beam.Pipeline(options=beam_options) as pipeline:
        csv_files = [file for file in os.listdir(path_folder) if file.endswith('.csv')]
        result = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(lambda file: csv_df(path_folder, file))
            | beam.combiners.ToList()
        )
        return result
#Now let us create the function that will compute the monthly averages for the same latitude and longitude 
def monthly_average(data_list):
    data_df=pd.DataFrame(data_list,columns=['Month','Latitude','Longitude','Temperature','WindSpeed'])
    result_df=data_df.groupby(['Month','Latitude','Longitude']).agg({'Temperature':'mean','WindSpeed':'mean'}).reset_index()
    agg_data={}
    for _,row in result_df.iterrows():
        lat=row['Latitude']
        long=row['Longitude']
        month=row['Month']
        temp=row['Temperature']
        speed=row['WindSpeed']
        if (lat,long) not in agg_data:
            agg_data[(lat,long)]=[]
        agg_data[(lat,long)].append((temp,speed))
    final_result=[(lat,long,data) for (lat,long),data in agg_data.items()]
    return final_result
def beam_average(data_list):# Parallelizes the monthly average function
    with beam.Pipeline(options=beam_options) as pipeline:
        result = (
            pipeline
            | beam.Create(data_list)
            | beam.Map(monthly_average)
            | beam.combiners.ToList()
        )
        return result 
#Now time to use geopandas and geo datasets
def hmap(avg_list):
    latitude = [lat for (lat, lon), (temp, speed) in avg_list]
    longitude = [lon for (lat, lon), (temp, speed) in avg_list]
    temperature = [temp for (lat, lon), (temp, speed) in avg_list]
    wspeed = [wspeed for (lat, lon), (temp, wspeed) in avg_list]
    data_df = pd.DataFrame({
    'Latitude': latitude,
    'Longitude': longitude,
    'AvgTemperature': temperature,
    'AvgWSpeed': wspeed
    })
    gdf = gpd.GeoDataFrame(data_df, geometry=gpd.points_from_xy(data_df.Longitude, data_df.Latitude))
    fig, ax = plt.subplots(figsize=(10, 10))
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, column='AvgTemperature', cmap='coolwarm', markersize=50, alpha=0.8, legend=True)
    ax.set_title('Temperature')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    plt.grid(False)
    plt.axis('equal')
    plt.show()
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, column='AvgWSpeed', cmap='viridis', markersize=50, alpha=0.8, legend=True)
    ax.set_title('Speed Visualization')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    plt.grid(False)
    plt.axis('equal')
    plt.show()
#Task 3
tuple_gen=PythonOperator(
    task_id="Listdf",
    python_callable=csv_to_dffilter,
    op_kwargs={"path_folder":path_unzip},
    dag=dag,
)
#Task 4
avg_gen=PythonOperator(
    task_id="avg_tv",
    python_callable=beam_average,
    op_kwargs={"data_list":"{{ ti.xcom_pull(task_ids='Listdf') }}"},
    dag=dag,
)
#Task 5
hmap_gen=PythonOperator(
    task_id="plot_gen",
    python_callable=hmap,
    op_kwargs={"avg_list":"{{ ti.xcom_pull(task_ids='avg_tv') }}"}
)
#Task 6
delete_csv=BashOperator(
    task_id="csv_folderdel",
    bash_command=f'rm {path_unzip}',
    dag=dag,
    trigger_rule='all_success'
)
check_available>>stop_pipeline_task>>unzip_task>>tuple_gen>>avg_gen>>hmap_gen>>delete_csv
        

