import requests
import mysql.connector
from mysql.connector import Error

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# URL da API e cabeçalhos
url = "https://app.omie.com.br/api/v1/servicos/os/"
headers = {"Content-Type": "application/json"}

# Dados da API
data = {
    "call": "ListarOS",
    "app_key": "4205722127607",
    "app_secret": "85381b60ecdb50ad73e461b57857571c",
    "param": [{
        "pagina": 1,
        "registros_por_pagina": 500,
        "apenas_importado_api": "N",
        "cExibirProdutos": "S"
    }]
}

# Conectar ao banco de dados
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Limpar as tabelas antes de inserir novos dados
    tabelas = ["vendas", "departamentos_vendas", "infocadastro", "servicosprestados", "produtoutilizado"]
    for tabela in tabelas:
        cursor.execute(f"DELETE FROM {tabela}")
        cursor.execute(f"ALTER TABLE {tabela} AUTO_INCREMENT = 1")
    conn.commit()
    print("Tabelas limpas e sequências de auto incremento reiniciadas.")

    while True:
        # Fazer a chamada da API
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            resposta = response.json()

            # Iterar sobre cada registro em osCadastro
            for os in resposta.get("osCadastro", []):
                cabecalho = os.get("Cabecalho", {})
                cNumOS = cabecalho.get("cNumOS", "")
                sql_vendas = """
                    INSERT INTO vendas (cEtapa, cNumOS, nCodCli, nCodOS, nCodVend, nValorTotal)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                valores_vendas = (
                    cabecalho.get("cEtapa", ""),
                    cNumOS,
                    cabecalho.get("nCodCli", 0),
                    cabecalho.get("nCodOS", 0),
                    cabecalho.get("nCodVend", 0),
                    cabecalho.get("nValorTotal", 0)
                )
                print("Tentando inserir em vendas:", valores_vendas)  # Depuração
                try:
                    cursor.execute(sql_vendas, valores_vendas)
                    print("Inserção em vendas bem-sucedida.")
                except Error as e:
                    print(f"Erro ao inserir em vendas: {e}")

                departamentos = os.get("Departamentos", [])
                for departamento in departamentos:
                    sql_departamentos = """
                        INSERT INTO departamentos_vendas (cNumOS, cCodDepto, nPerc, nValor)
                        VALUES (%s, %s, %s, %s)
                    """
                    valores_departamentos = (
                        cNumOS,
                        departamento.get("cCodDepto", ""),
                        departamento.get("nPerc", 0),
                        departamento.get("nValor", 0)
                    )
                    print("Tentando inserir em departamentos_vendas:", valores_departamentos)  # Depuração
                    try:
                        cursor.execute(sql_departamentos, valores_departamentos)
                        print("Inserção em departamentos_vendas bem-sucedida.")
                    except Error as e:
                        print(f"Erro ao inserir em departamentos_vendas: {e}")

                infocadastro = os.get("InfoCadastro", {})
                sql_infocadastro = """
                    INSERT INTO infocadastro (cNumOS, cCancelada, cFaturada, dDtFat)
                    VALUES (%s, %s, %s, %s)
                """
                valores_infocadastro = (
                    cNumOS,
                    infocadastro.get("cCancelada", ""),
                    infocadastro.get("cFaturada", ""),
                    infocadastro.get("dDtFat", "")
                )
                print("Tentando inserir em infocadastro:", valores_infocadastro)  # Depuração
                try:
                    cursor.execute(sql_infocadastro, valores_infocadastro)
                    print("Inserção em infocadastro bem-sucedida.")
                except Error as e:
                    print(f"Erro ao inserir em infocadastro: {e}")

                servicos_prestados = os.get("ServicosPrestados", [])
                for servico in servicos_prestados:
                    sql_servicos = """
                        INSERT INTO servicosprestados (cNumOS, cDescServ, nCodServico, nQtde, nValUnit, nValorAcrescimos, nValorDesconto)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    valores_servicos = (
                        cNumOS,
                        servico.get("cDescServ", ""),
                        servico.get("nCodServico", 0),
                        servico.get("nQtde", 0),
                        servico.get("nValUnit", 0),
                        servico.get("nValorAcrescimos", 0),
                        servico.get("nValorDesconto", 0)
                    )
                    print("Tentando inserir em servicosprestados:", valores_servicos)  # Depuração
                    try:
                        cursor.execute(sql_servicos, valores_servicos)
                        print("Inserção em servicosprestados bem-sucedida.")
                    except Error as e:
                        print(f"Erro ao inserir em servicosprestados: {e}")

                # Inserir produtos utilizados
                produtos_utilizados = os.get("produtosUtilizados", {}).get("produtoUtilizado", [])
                for produto in produtos_utilizados:
                    sql_produtos = """
                        INSERT INTO produtoutilizado (nCodProdutoPU, nQtdePU, cNumOS)
                        VALUES (%s, %s, %s)
                    """
                    valores_produtos = (
                        produto.get("nCodProdutoPU", 0),
                        produto.get("nQtdePU", 0),
                        cNumOS
                    )
                    print("Tentando inserir em produtoutilizado:", valores_produtos)  # Depuração
                    try:
                        cursor.execute(sql_produtos, valores_produtos)
                        print("Inserção em produtoutilizado bem-sucedida.")
                    except Error as e:
                        print(f"Erro ao inserir em produtoutilizado: {e}")

            # Confirmar as transações
            conn.commit()
            print(f"Dados da página {data['param'][0]['pagina']} inseridos com sucesso.")

            # Verificar se há mais páginas
            if data['param'][0]['pagina'] >= resposta.get("total_de_paginas", 0):
                break  # Se a página atual for a última, não há mais páginas

            # Incrementar o número da página para a próxima iteração
            data['param'][0]['pagina'] += 1

        else:
            print(f"Erro na requisição: {response.status_code}")
            print(response.text)
            break

except Error as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")
