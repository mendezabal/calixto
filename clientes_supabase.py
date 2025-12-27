import pymysql
from supabase import create_client

# Configuração do banco de dados MySQL
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Ns_Sra-1976*",
    "database": "calixto"
}

# Configuração do Supabase
supabase_url = "https://cdtditdcyzpmfcdsdnfu.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNkdGRpdGRjeXpwbWZjZHNkbmZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDMwMTUyNDMsImV4cCI6MjA1ODU5MTI0M30.32SJGPQ5KaYldHbcf0OE1gVnXSuz7RdPgqJeLfvn2I8"
supabase_client = create_client(supabase_url, supabase_key)

# Inicializa as variáveis de conexão e cursor
conn = None
cursor = None

try:
    # Conectar ao banco de dados MySQL
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()

    # Buscar dados do Supabase
    response = supabase_client.table('clientes').select('*').execute()

    if hasattr(response, 'error') and response.error:
        print(f"Erro na consulta ao Supabase: {response.error}")
        raise Exception(response.error)

    # Processar os clientes
    clientes = response.data

    for cliente in clientes:
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
    print(f"Total de {len(clientes)} clientes inseridos no banco de dados MySQL.")

    # Limpar todos os registros da tabela 'clientes' no Supabase
    delete_response = supabase_client.table('clientes').delete().neq('id', 0).execute()

    if hasattr(delete_response, 'error') and delete_response.error:
        print(f"Erro ao limpar tabela no Supabase: {delete_response.error}")
        raise Exception(delete_response.error)

    print("Tabela 'clientes' no Supabase foi limpa com sucesso.")

except Exception as e:
    print(f"Erro durante a execução: {e}")

finally:
    if cursor:
        cursor.close()
    if conn and conn.open:
        conn.close()
        print("Conexão com o banco de dados MySQL encerrada.")
