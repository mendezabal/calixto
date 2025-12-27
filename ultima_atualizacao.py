import pymysql
from datetime import datetime

# Conectar ao banco de dados
conn = pymysql.connect(
    host='localhost',        # Exemplo: 'localhost'
    user='root',             # Exemplo: 'root'
    password='Ns_Sra-1976*', # Sua senha
    database='calixto'       # Nome do banco de dados
)

cursor = conn.cursor()

# Apagar todas as informações da tabela 'ultima_atualizacao'
cursor.execute('TRUNCATE TABLE ultima_atualizacao')

# Obter a data e hora atuais
data_hora_atual = datetime.now()

# Inserir a data e hora na tabela 'ultima_atualizacao'
cursor.execute('''
    INSERT INTO ultima_atualizacao (ultima_atualizacao)
    VALUES (%s)
''', (data_hora_atual,))

# Confirmar a transação
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print("Data e hora de execução inseridas com sucesso.")
