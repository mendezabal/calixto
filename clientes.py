import requests
import mysql.connector
from mysql.connector import Error

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

# Conectar ao banco de dados
try:
    conn = mysql.connector.connect(**db_config)
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

        # Verifica se há clientes na resposta
        if "clientes_cadastro" not in resposta or not resposta["clientes_cadastro"]:
            break

        # Processa os clientes e insere no banco de dados
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

        # Se a quantidade de registros retornados for menor que o máximo por página, encerra o loop
        if len(resposta["clientes_cadastro"]) < 500:
            break

        pagina += 1

    print("Todos os dados foram inseridos no banco de dados.")

except Error as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")
