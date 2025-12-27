import requests
import pymysql
import json

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/geral/departamentos/"
headers = {"Content-Type": "application/json"}

# Credenciais da API
data = {
    "call": "ListarDepartamentos",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 50
    }]
}

# Conectar ao banco de dados
try:
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    # Limpar a tabela antes de inserir novos dados
    cursor.execute("DELETE FROM departamento_omie")
    cursor.execute("ALTER TABLE departamento_omie AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabela 'departamento_omie' limpa e sequência de auto incremento reiniciada.")

    pagina = 1
    while True:
        data["param"][0]["pagina"] = pagina
        response = requests.post(url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            break

        resposta = response.json()

        # Verifica se há dados na resposta
        if "departamentos" not in resposta or not resposta["departamentos"]:
            print("Nenhum dado encontrado ou estrutura inesperada.")
            break

        # Processa os departamentos e insere no banco de dados
        for departamento in resposta["departamentos"]:
            valores = (
                departamento.get("codigo", ""),
                departamento.get("descricao", "")
            )

            sql = """
                INSERT INTO departamento_omie (codigo, descricao)
                VALUES (%s, %s)
            """
            cursor.execute(sql, valores)

        conn.commit()
        print(f"Página {pagina} processada com sucesso.")

        # Se a quantidade de registros retornados for menor que o máximo por página, encerra o loop
        if len(resposta["departamentos"]) < data["param"][0]["registros_por_pagina"]:
            break

        pagina += 1

    print("Todos os dados foram inseridos no banco de dados.")

except pymysql.MySQLError as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    # Fecha a conexão
    if conn and conn.open:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")
