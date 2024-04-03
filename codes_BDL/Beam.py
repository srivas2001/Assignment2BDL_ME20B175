from airflow import DAG
import random 
import json
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
path_zip="/opt/airflow/Climate_dataset.zip" #Path to the zip file
path_unzip="/opt/airflow/Climate_dataset2023" #Path to the unzipped folder
#Task 1
check_available=FileSensor(
    task_id="archive_available",
    filepath=path_zip,
    timeout=5,
    poke_interval=1,
    dag=dag,
)
#Task 2
unzip_task=BashOperator(
    task_id='unzip_archive',
    bash_command=f'if unzip -tq {path_zip}; then unzip -o {path_zip} -d {path_unzip}; else exit 1; fi', #Unzip the file
    dag=dag,
    trigger_rule='all_success',
)
def csv_df(csv_file):
    with open(csv_file, 'r') as file:
        # Read CSV file and process it
        df=pd.read_csv(file)
    tuple_data=[]
    col_anal=['HourlyDryBulbTemperature','HourlyWindSpeed']
    all_analyse=['LATITUDE','LONGITUDE']+col_anal #Columns to be analysed
    df[all_analyse]=df[all_analyse].apply(pd.to_numeric,errors='coerce') #Convert to numeric and coerce errors
    df.dropna(how='any') #Drop all nan values
    for index,row in df.iterrows():
        lat=row['LATITUDE'] #This gets the latitude
        print(index)
        long=row['LONGITUDE'] #This gets the longitude
        date=pd.to_datetime(row['DATE']).month
        hourly_temp=row['HourlyDryBulbTemperature'] #This gets the hourly temperature
        hourly_speed=row['HourlyWindSpeed'] #This gets the hourly wind speed
        tuple_data.append((date,float(lat),float(long),float(hourly_temp),float(hourly_speed))) #Convert everthing into float and put in list
    return tuple_data
def csv_to_dffilter(path_folder): # This function converts the csv file into the required tuple format
    with beam.Pipeline(runner='DirectRunner') as pipeline: #This creates the pipeline in direct runner mode in beam
        csv_files = [os.path.join(path_folder,file) for file in os.listdir(path_folder) if file.endswith('.csv')]
        result = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(csv_df)
            | beam.Map(monthly_average)
            | beam.io.WriteToText('/opt/airflow/tuple_data.txt')
        )
text_address='/opt/airflow/tuple_data.txt-00000-of-00001' #This is the address of the text file stored by beam
#Now let us create the function that will compute the monthly averages for the same latitude and longitude 
def monthly_average(data_list):
    data_df = pd.DataFrame(data_list, columns=['Month', 'Latitude', 'Longitude', 'Temperature', 'WindSpeed'])
    data_df.dropna(inplace=True)
    data_df['Temperature']=(data_df['Temperature']-32)*5/9
    final_result = []
    result_df = data_df.groupby(['Month', 'Latitude', 'Longitude']).agg({'Temperature': 'mean', 'WindSpeed': 'mean'}).reset_index() #Use groupby to find average temperature and wind speed for each month
    for _, row in result_df.iterrows():
        lat = row['Latitude']
        long = row['Longitude']
        month = row['Month']
        temp = row['Temperature']
        speed = row['WindSpeed']
        final_result.append([lat, long, month, temp, speed])
    return json.dumps(final_result)
#Now time to use geopandas and geo datasets
def hmap(link):
    avg_list = []
    with open(link, 'r') as file:
        for line in file:
            local_data = json.loads(line)
            avg_list.extend(local_data)  # Extend instead of append to flatten the nested lists
    
    latitude = [lat for lat, lon, month, temp, wspeed in avg_list]#Get latitude longitude etc
    longitude = [lon for lat, lon, month, temp, wspeed in avg_list]
    month=[month for lat, lon, month, temp, wspeed in avg_list]
    temperature = [temp for lat, lon, month, temp, wspeed in avg_list]
    wspeed = [wspeed for lat, lon, month, temp, wspeed in avg_list]
    data_df = pd.DataFrame({
        'Latitude': latitude,
        'Longitude': longitude,
        'Month': month,
        'AvgTemperature': temperature,
        'AvgWSpeed': wspeed
    })
    gdf = gpd.GeoDataFrame(data_df, geometry=gpd.points_from_xy(data_df.Longitude, data_df.Latitude)) #Create a geodataframe
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')) #This gets the world map
    unique_months = gdf['Month'].unique()
    temp_min = gdf['AvgTemperature'].min()
    temp_max = gdf['AvgTemperature'].max()# This is done to fix scale in vmin and vmax
    wind_speed_min = gdf['AvgWSpeed'].min()
    wind_speed_max = gdf['AvgWSpeed'].max() 
    for month in unique_months:
    # Filter the GeoDataFrame for the current month
        gdf_month = gdf[gdf['Month'] == month]
    
    # Plot the world basemap
    

    # Plot Temperature for the current month
        ax = world.plot( color='lightgrey', edgecolor='black',markersize=1,vmin=temp_min, vmax=temp_max)
        gdf_month.plot(column='AvgTemperature', vmin=temp_min, vmax=temp_max,ax=ax, cmap='coolwarm', legend=True, 
                   legend_kwds={'label': "AvgTemperature", 'orientation': "horizontal"})
        ax.set_title(f'Temperature for Month {month}')
        plt.axis('scaled')
        plt.savefig(f'/opt/airflow/temperature/plot_month_{month}_temperature.png') #Save the figure for each month

    # Plot Wind Speed for the current month
        ax = world.plot( color='lightgrey', edgecolor='black',markersize=1, vmin=wind_speed_min, vmax=wind_speed_max)
        gdf_month.plot(column='AvgWSpeed',vmin=wind_speed_min, vmax=wind_speed_max, ax=ax, cmap='viridis', legend=True, 
                   legend_kwds={'label': "AvgWSpeed", 'orientation': "horizontal"}) #Plot the wind speed
        ax.set_title(f'Wind Speed for Month {month}')
        plt.axis('scaled')
        plt.savefig(f'/opt/airflow/wind/Wind_Speed_for_Month_{month}.png')
    # Show the plots
        plt.tight_layout()
        plt.show()
#Task 3
#make the gif
 #These are the files to be used to convert to GIF
output_gif='/opt/airflow/temperature/temperature_302.gif'
frame_rate = 2
input_pattern = "/opt/airflow/temperature/plot_month_%d.0_temperature.png"
gif_task_1= BashOperator(
    task_id='create_gif_task',
    bash_command='ffmpeg -framerate {} -i {} {}'.format(frame_rate, input_pattern, output_gif), #make gif for temperature
    dag=dag,
)
input_pattern2="/opt/airflow/wind/Wind_Speed_for_Month_%d.0.png"
output_gif2='/opt/airflow/wind/wind_302.gif'
gif_task_2= BashOperator(
    task_id='create_gif_task2',
    bash_command='ffmpeg -framerate {} -i {} {}'.format(frame_rate, input_pattern2, output_gif2), #make gif for wind speed
    dag=dag,
)
tupleavg_gen=PythonOperator(
    task_id="Listdf",
    python_callable=csv_to_dffilter,
    op_kwargs={"path_folder":path_unzip}, #This executes the csv_to_dffilter function
    dag=dag,
)
#Task 4
#Task 5
hmap_gen=PythonOperator(
    task_id="plot_gen",
    python_callable=hmap,
    op_kwargs={"link":text_address}, #This gives the 12 plots for temperature and wind speed
)
#Task 6
delete_csv=BashOperator(
    task_id="csv_folderdel",
    bash_command=f'rm -r {path_unzip}', #This finally deletes the extrated csv files to prevent memory buildup 
    dag=dag,
    trigger_rule='all_success'
)
check_available>>unzip_task>>tupleavg_gen>>hmap_gen>>gif_task_1>>gif_task_2>>delete_csv #Final DAG created here 