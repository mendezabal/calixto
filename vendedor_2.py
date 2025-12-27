import requests
import pymysql

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/geral/vendedores/"
headers = {"Content-Type": "application/json"}

# Credenciais da API
data = {
    "call": "ListarVendedores",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 5,
        "apenas_importado_api": "N"
    }]
}

vendedores_total = []
pagina = 1

# Inicializa as variáveis de conexão e cursor
conn = None
cursor = None

# Conectar ao banco de dados
try:
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    # Limpar a tabela antes de inserir novos dados
    cursor.execute("DELETE FROM vendedores")
    cursor.execute("ALTER TABLE vendedores AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabela 'vendedores' limpa e sequência de auto incremento reiniciada.")

    while True:
        data["param"][0]["pagina"] = pagina
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            break

        resposta = response.json()

        # Exibir a resposta completa para depuração
        # print("Resposta da API:", json.dumps(resposta, indent=2, ensure_ascii=False))

        # Verifica se há dados na resposta
        if "cadastro" not in resposta or not resposta["cadastro"]:
            print("Nenhum dado encontrado ou estrutura inesperada.")
            break

        # Processa os dados e insere no banco de dados
        for vendedor in resposta["cadastro"]:
            valores = (
                vendedor.get("codigo", ""),
                vendedor.get("nome", "")
            )

            # print("Inserindo:", valores)  # Depuração

            sql = """
                INSERT INTO vendedores (codigo, nome)
                VALUES (%s, %s)
            """
            cursor.execute(sql, valores)

        conn.commit()
        # print(f"Página {pagina} processada com sucesso.")

        # Se a quantidade de registros retornados for menor que o máximo por página, encerra o loop
        if len(resposta["cadastro"]) < 5:
            break

        pagina += 1

    print("Todos os dados foram inseridos no banco de dados.")

except pymysql.MySQLError as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if cursor:
        cursor.close()
    if conn and conn.open:
        conn.close()
        print("Conexão com o banco de dados encerrada.")
