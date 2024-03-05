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
number_files=5
year="2001"
dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)
#get the html webpage
#get_cmd=f"curl -o D:\\Workspace\\Climate_Dataset{year}.html {base_url}{year}"
get_cmd=f"curl -L -o /opt/airflow/Climate_dataset{year}.html {base_url}{year}"
saved_path=f"/opt/airflow/Climate_dataset{year}.html"
def choose_files(html_path,number_files):
    with open(html_path,'r') as file:
        data=file.read()
        #parse this html file
        #get the links
    parse_file = BeautifulSoup(data, 'html.parser')    
    csv_links = [urljoin(base_url+year+'/',link.get('href')) for link in parse_file.find_all('a') if link.get('href').endswith('.csv')]
    random_csv_links = random.sample(csv_links, number_files)
    print(random_csv_links)
    return random_csv_links
def zip_files(csv_links,zip_filename):
    temp_dir = "/opt/airflow/temp"
    csv_links = ast.literal_eval(csv_links)
    os.makedirs(temp_dir, exist_ok=True)
    for links in csv_links:
        #print(i)
        #print("Downloading:", csv_links[i])
        res=requests.get(links)
        if res.status_code==200:
            filename=os.path.join(temp_dir, os.path.basename(links))
            with open(filename, 'wb') as csv_file:
                    csv_file.write(res.content)
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    zipf.write(os.path.join(root, file), arcname=file)
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    os.rmdir(temp_dir)
#Task 1
fetch_task=BashOperator(task_id="fetch_page",bash_command=get_cmd,dag=dag)
#Task 2
files_task=PythonOperator(task_id="choose_files",
                         python_callable=choose_files,
                         op_kwargs={"html_path":saved_path,"number_files":number_files},
                         dag=dag)
#Task 3
zip_task=PythonOperator(task_id="zip_files",
                        python_callable=zip_files,
                        op_kwargs={"csv_links":"{{ ti.xcom_pull(task_ids='choose_files') }}",
                                   "zip_filename":"/opt/airflow/Climate_dataset.zip"},
                                   dag=dag)
fetch_task>>files_task>>zip_task