import requests
import mysql.connector
import json
from mysql.connector import Error

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/geral/produtos/"
headers = {"Content-Type": "application/json"}

# Credenciais da API
data = {
    "call": "ListarProdutos",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 500,
        "apenas_importado_api": "N",
        "filtrar_apenas_omiepdv": "N"
    }]
}

produtos_total = []
pagina = 1

# Conectar ao banco de dados
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Limpar a tabela antes de inserir novos dados
    cursor.execute("DELETE FROM produtos")
    cursor.execute("ALTER TABLE produtos AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabela 'produtos' limpa e sequência de auto incremento reiniciada.")

    while True:
        data["param"][0]["pagina"] = pagina
        response = requests.post(url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            print(response.text)
            break

        resposta = response.json()

        # Exibir a resposta completa para depuração
        print(json.dumps(resposta, indent=2, ensure_ascii=False))

        # Verifica se há produtos na resposta
        if "produto_servico_cadastro" not in resposta or not resposta["produto_servico_cadastro"]:
            print("Nenhum produto encontrado ou estrutura inesperada.")
            break

        # Processa os produtos e insere no banco de dados
        for produto in resposta["produto_servico_cadastro"]:
            valores = (
                produto.get("codigo", ""),
                produto.get("codigo_produto", ""),
                produto.get("descricao", "")
            )

            print("Inserindo:", valores)  # Depuração

            sql = """
                INSERT INTO produtos (codigo, codigo_produto, descricao)
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql, valores)

        conn.commit()
        print(f"Página {pagina} processada com sucesso.")

        # Se a quantidade de registros retornados for menor que o máximo por página, encerra o loop
        if len(resposta["produto_servico_cadastro"]) < 500:
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
