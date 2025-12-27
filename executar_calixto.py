import subprocess
import sys
import time
import os
from typing import List, Tuple
from time import sleep


def execute_script(script_path: str) -> Tuple[bool, float]:
    """
    Executa um script Python e retorna o status de sucesso e o tempo de execução.

    Args:
        script_path: Caminho do script a ser executado

    Returns:
        Tuple contendo status de sucesso (bool) e tempo de execução (float)
    """
    if not os.path.exists(script_path):
        print(f"Erro: O script {script_path} não foi encontrado.")
        return False, 0.0

    print("=" * 50)
    print(f"Executando: {os.path.basename(script_path)}")
    print("=" * 50)

    start_time = time.time()

    try:
        # Usando errors='replace' para lidar com caracteres que não podem ser decodificados
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',  # Substitui caracteres inválidos
            check=False  # Não levanta exceção se o código de saída for diferente de zero
        )

        # Exibir stdout e stderr
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"STDERR: {result.stderr}")

        success = result.returncode == 0
        if not success:
            print(f"Script retornou código de erro: {result.returncode}")

        return success, time.time() - start_time

    except Exception as e:
        print(f"Erro na execução do script: {e}")
        return False, time.time() - start_time


def run_scripts(script_list: List[str], stop_on_error: bool = True) -> None:
    """
    Executa uma lista de scripts em sequência.

    Args:
        script_list: Lista de caminhos dos scripts a serem executados
        stop_on_error: Se True, interrompe a execução quando um script falhar
    """
    total_start_time = time.time()
    results = []

    for script in script_list:
        success, execution_time = execute_script(script)
        results.append((script, success, execution_time))

        if not success and stop_on_error:
            print(f"\nExecução interrompida após falha em: {os.path.basename(script)}")
            break

    # Relatório final
    print("\n" + "=" * 50)
    print("RELATÓRIO DE EXECUÇÃO")
    print("=" * 50)

    for script, success, time_taken in results:
        status = "SUCESSO" if success else "FALHA"
        print(f"{os.path.basename(script)}: {status} ({time_taken:.2f}s)")

    print(f"\nTempo total de execução: {(time.time() - total_start_time):.2f}s")
    print(f"Scripts executados: {len(results)}/{len(script_list)}")
    failed_count = sum(1 for _, success, _ in results if not success)
    print(f"Scripts com falha: {failed_count}")


if __name__ == "__main__":
    scripts = [
        # Adicione aqui a lista de scripts que deseja executar
        #"clientes_2.py",
        "ultima_atualizacao.py",
        "departamento_omie_2.py",
        "posicao_estoque_2.py",
        "produtos_2.py",
        "vendas_2.py",
        "vendedor_2.py"
        ]

    # Defina se deseja parar em caso de erro
    run_scripts(scripts, stop_on_error=True)
