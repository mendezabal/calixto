import requests
import pymysql
from time import sleep  # Adicionado para usar o tempo de espera

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",  # Altere se necessário
    "user": "root",  # Substitua pelo seu usuário do MySQL
    "password": "Ns_Sra-1976*",  # Substitua pela sua senha do MySQL
    "database": "calixto"  # Substitua pelo nome do seu banco de dados
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/geral/clientes/"
headers = {"Content-Type": "application/json"}

# Credenciais da API
data = {
    "call": "ListarClientes",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 500,
        "apenas_importado_api": "N"
    }]
}

clientes_total = []
pagina = 1

# Inicializa as variáveis de conexão e cursor
conn = None
cursor = None

try:
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    # Limpar a tabela antes de inserir novos dados
    cursor.execute("DELETE FROM clientes")
    cursor.execute("ALTER TABLE clientes AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabela 'clientes' limpa e sequência de auto incremento reiniciada.")

    while True:
        data["param"][0]["pagina"] = pagina
        response = requests.post(url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            print(response.text)
            break

        resposta = response.json()

        if "clientes_cadastro" not in resposta or not resposta["clientes_cadastro"]:
            break

        for cliente in resposta["clientes_cadastro"]:
            valores = (
                cliente.get("cep", ""),
                cliente.get("cidade", ""),
                cliente.get("cidade_ibge", ""),
                cliente.get("cnpj_cpf", ""),
                cliente.get("codigo_cliente_omie", ""),
                cliente.get("razao_social", "")
            )

            sql = """
                INSERT INTO clientes (cep, cidade, cidade_ibge, cnpj_cpf, codigo_cliente_omie, razao_social)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, valores)

        conn.commit()
        print(f"Página {pagina} processada com sucesso.")

        if len(resposta["clientes_cadastro"]) < 500:
            break

        pagina += 1
        sleep(2)  # Aguarda 2 segundos antes de fazer a próxima requisição

    print("Todos os dados foram inseridos no banco de dados.")

except pymysql.MySQLError as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if cursor:
        cursor.close()
    if conn and conn.open:
        conn.close()
        print("Conexão com o banco de dados encerrada.")
