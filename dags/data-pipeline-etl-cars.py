    ### Para obter informações detalhadas sobre o processo de execução, visite nosso artigo no Medium:
    ### 

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from app.test import start
import pandas as pd
import sqlalchemy

path_temp_csv = "/tmp/dataset.csv"
email_failed = "teste@teste.com"


dag = DAG(
    dag_id="data-pipeline-etl-cars",
    description="Pipeline para o processo de ETL dos ambientes de produção MySQL ao PostgreSQL.",
    start_date=days_ago(2),
    schedule_interval=None,
)

def _extract():
    # Conectando à base de dados do ambiente I; 
    engine_db_mysql = sqlalchemy.create_engine('mysql+pymysql://root:etl-2023@172.17.0.3:3306/cars')
    
    # Selecionando os dados;
    dataset_df = pd.read_sql_query("""
        SELECT
            owners.id_ as emp_no,
            owners.first_name,
            owners.last_name,
            cars.car_model,
            cars.car_brand
        FROM owners
        INNER JOIN cars ON owners.id_ = cars.owners_id
        LIMIT 100
    """, engine_db_mysql)
    
    # Exportando os dados para a área de stage;
    dataset_df.to_csv(path_temp_csv, index=False)

def _transform():
    dataset_df = pd.read_csv(path_temp_csv)

    # Transformando os dados dos atributos
    dataset_df["name"] = dataset_df["first_name"] + " " + dataset_df["last_name"]
    dataset_df["car"] = dataset_df["car_model"] + " " + dataset_df["car_brand"]

    dataset_df.drop(["first_name", "last_name", "car_model", "car_brand"], axis=1, inplace=True)

    # Persistindo o dataset no arquivo temporário
    dataset_df.to_csv(path_temp_csv, index=False)

def _load():
    # Conectando com o banco de dados postgresql
    engine_db_postgresql = sqlalchemy.create_engine('postgres+psycopg2://postgres:etl-2023@172.17.0.4:5432/cars')
    
    # Selecionando os dados;
    # Lendo os dados a partir de arquivos csv;
    dataset_df = pd.read_csv(path_temp_csv)
    
    # Carregando os dados no banco de dados
    dataset_df.to_sql("employees_dataset", engine_db_postgresql, if_exists="replace", index=False)


extract_task = PythonOperator(
    task_id="Extract_Dataset", 
    python_callable=_extract, 
    dag=dag
)

transform_task = PythonOperator(
    task_id="Transform_Dataset", 
    python_callable=_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset", 
    email_on_failure=True,
    email=email_failed, 
    python_callable=_load,
    dag=dag
)

email_task = EmailOperator(
    task_id="Notify",
    email_on_failure=True,
    email=email_failed, 
    to='teste@teste.com',
    subject='Pipeline Finalizado',
    html_content='<p> A execução do pipeline destinado à atualização de dados entre os ambientes MySQL e PostgreSQL foi concluída com êxito, demonstrando a eficácia do processo de Extração, Transformação e Carregamento (ETL) na sincronização e integração de informações entre os sistemas operacionais online (MySQL) e os ambientes analíticos online (PostgreSQL).. <p>',
    dag=dag)

extract_task >> transform_task >> load_task >> email_task

