### Para obter informações detalhadas sobre o processo de execução, visite nosso artigo no Medium:
## https://medium.com/@gabrielalmeidabispo/engenharia-de-fluxos-de-dados-desvendando-a-orquestra%C3%A7%C3%A3o-com-apache-airflow-1e542fff7c73

import csv
import mysql.connector

# Conecte-se ao seu banco de dados MySQL
# Verificar as portas de configuração do banco de dados no container docker;
conn = mysql.connector.connect(
    database="cars",
    user="root",
    password="etl-2023",
    host="localhost",
    port="3306"
)

cur = conn.cursor()

# Inserir registros na tabela owners
# Lembra-se de modificar o caminho para o seu diretório local/web
with open('/home/data/datasets/owners.csv', 'r') as owners_file:
    owners_reader = csv.reader(owners_file)
    next(owners_reader)  # Pule o cabeçalho
    owners_data = [(int(row[0]), row[1], row[2], row[3], row[4]) for row in owners_reader]

insert_owners_query = "INSERT INTO owners (id_, first_name, last_name, country, credit_card_type) VALUES (%s, %s, %s, %s, %s);"
cur.executemany(insert_owners_query, owners_data)

# Feche a conexão com o banco de dados antes de abrir novamente
conn.commit()
cur.close()
conn.close()

# Conecte-se novamente
# Verificar as portas de configuração do banco de dados no container docker;
conn = mysql.connector.connect(
    database="cars",
    user="root",
    password="etl-2023",
    host="localhost",
    port="3306"
)

cur = conn.cursor()

# Inserir registros na tabela cars
# Lembra-se de modificar o caminho para o seu diretório local/web
with open('/home/data/datasets/cars.csv', 'r') as cars_file:
    cars_reader = csv.reader(cars_file)
    next(cars_reader)  # Pule o cabeçalho
    cars_data = [(int(row[0]), row[1], row[2], row[3], int(row[4]), int(row[5])) for row in cars_reader]

insert_cars_query = "INSERT INTO cars (id_, car_brand, car_model, car_color, year_manufacture, owners_id) VALUES (%s, %s, %s, %s, %s, %s);"
cur.executemany(insert_cars_query, cars_data)

# Feche a conexão com o banco de dados
conn.commit()
cur.close()
conn.close()
