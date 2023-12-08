from sqlalchemy import create_engine

# Sua string de conexão existente
conexao_string = 'mysql+pymysql://root:etl-2023@172.17.0.3:3306/cars'

# Criação de uma engine SQLAlchemy
engine = create_engine(conexao_string)

# Recupera e imprime as credenciais
print("Usuário:", engine.url.username)
print("Senha:", engine.url.password)
print("Host:", engine.url.host)
print("Porta:", engine.url.port)
print("Banco de Dados:", engine.url.database)
