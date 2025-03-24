import requests
import mysql.connector
import json
from mysql.connector import Error
from datetime import datetime

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/estoque/consulta/"
headers = {"Content-Type": "application/json"}

# Obter a data atual e formatá-la como "dd/mm/yyyy"
data_atual = datetime.now().strftime("%d/%m/%Y")

# Dados da API
data = {
    "call": "ListarPosEstoque",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "nPagina": 1,
        "nRegPorPagina": 50,
        "dDataPosicao": data_atual,
        "cExibeTodos": "N",
        "codigo_local_estoque": 0
    }]
}

# Conectar ao banco de dados
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Limpar a tabela antes de inserir novos dados
    cursor.execute("DELETE FROM posicao_estoque")
    cursor.execute("ALTER TABLE posicao_estoque AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabela 'posicao_estoque' limpa e sequência de auto incremento reiniciada.")

    # Inicializar a variável de controle de página
    pagina = 1

    while True:
        data["param"][0]["nPagina"] = pagina  # Corrigir a chave para "nPagina"
        response = requests.post(url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            print(response.text)
            break

        resposta = response.json()

        # Exibir a resposta completa para depuração
        print("Resposta completa da API:")
        print(json.dumps(resposta, indent=2, ensure_ascii=False))

        # Verifica se há dados na resposta
        if "produtos" not in resposta or not resposta["produtos"]:
            print("Nenhum dado encontrado ou estrutura inesperada.")
            break

        # Processa os dados e insere no banco de dados
        for produto in resposta["produtos"]:
            valores = (
                produto.get("cCodigo", ""),
                produto.get("cDescricao", ""),
                produto.get("codigo_local_estoque", ""),
                produto.get("nCMC", ""),
                produto.get("nCodProd", "")
            )

            print("Inserindo:", valores)  # Depuração

            sql = """
                INSERT INTO posicao_estoque (cCodigo, cDescricao, codigo_local_estoque, nCMC, nCodProd)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, valores)

        conn.commit()
        print(f"Página {pagina} processada com sucesso.")

        # Se a quantidade de registros retornados for menor que o máximo por página, encerra o loop
        if len(resposta["produtos"]) < data["param"][0]["nRegPorPagina"]:
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
