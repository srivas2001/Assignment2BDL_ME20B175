from airflow import DAG
import random 
import os
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import zipfile
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import ast
base_url="https://www.ncei.noaa.gov/data/local-climatological-data/access/"  
number_files=300 #Can change this
year="2023" #Year too can be varied
dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,
    catchup=False
)
#get the html webpage
#get_cmd=f"curl -o D:\\Workspace\\Climate_Dataset{year}.html {base_url}{year}"
get_cmd=f"curl -L -o /opt/airflow/Climate_dataset{year}.html {base_url}{year}" #curl command for getting webpage
saved_path=f"/opt/airflow/Climate_dataset{year}.html" #place to be stored
def choose_files(html_path,number_files):
    with open(html_path,'r') as file:
        data=file.read()
        #parse this html file
        #get the links
    parse_file = BeautifulSoup(data, 'html.parser')    
    csv_links = [urljoin(base_url+year+'/',link.get('href')) for link in parse_file.find_all('a') if link.get('href').endswith('.csv')]
    random_csv_links = random.sample(csv_links, number_files) #Gets all csv files from webpage and randomly chooses number_files amount of files
    print(random_csv_links)
    return random_csv_links
def zip_files(csv_links,zip_filename):
    temp_dir = "/opt/airflow/temp" #directory where thhe file will be stored temporarily
    csv_links = ast.literal_eval(csv_links)
    os.makedirs(temp_dir, exist_ok=True)
    for links in csv_links:
        #print(i)
        #print("Downloading:", csv_links[i])
        res=requests.get(links)
        if res.status_code==200: #If everything is okay
            filename=os.path.join(temp_dir, os.path.basename(links))
            with open(filename, 'wb') as csv_file:
                    csv_file.write(res.content)
    with zipfile.ZipFile(zip_filename, 'w') as zipf: #We zip the file and store it in the zip_filename
        for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    zipf.write(os.path.join(root, file), arcname=file)
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file) #remove the file
        if os.path.isfile(file_path):
            os.remove(file_path)
    os.rmdir(temp_dir)
#gif code using ffmpeg and Bash Operator
#Task 1
fetch_task=BashOperator(task_id="fetch_page",bash_command=get_cmd,dag=dag) #executes the curl command
#Task 2
files_task=PythonOperator(task_id="choose_files",
                         python_callable=choose_files,
                         op_kwargs={"html_path":saved_path,"number_files":number_files}, #Executes the choose_files function
                         dag=dag)
#Task 3
zip_task=PythonOperator(task_id="zip_files",
                        python_callable=zip_files,
                        op_kwargs={"csv_links":"{{ ti.xcom_pull(task_ids='choose_files') }}", #This takes input from the choose files task by connecting with xcom
                                   "zip_filename":"/opt/airflow/Climate_dataset.zip"},
                                   dag=dag)
fetch_task>>files_task>>zip_task #Final DAG created here with the tasks connected