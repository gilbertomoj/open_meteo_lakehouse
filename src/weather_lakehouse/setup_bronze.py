from pyspark.sql import SparkSession
import os
from datacontract.data_contract import DataContract
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, ChannelInfo
import time


def read_bronze_contracts() -> list[str]:
    base_dir = "./contracts/models/"
    files = os.listdir(base_dir)
    contracts = [base_dir + file for file in files if file.endswith(".yaml")]

    for contract in contracts:
        data_contract = DataContract(data_contract_file=contract)
        schema_name = data_contract.get_data_contract().name

        query = data_contract.export(export_format="sql")

        with open(f"src/weather_lakehouse/ddl/{schema_name}.sql", "w") as file:
            file.write(query)
            file.close()


def main():
    # Realiza a orquestração do setup, cria um job para a criação e setup do schema, e deleta ao final (one-time-run job)
    w = WorkspaceClient()

    contract = read_data_contract()
    # queries = read_sql()

    schema_info = contract.schema_[0]
    server_info = contract.servers[0]

    schema_name = server_info.catalog + "." + server_info.schema_

    if not w.schemas.get(full_name=schema_name):
        w.schemas.create(catalog_name=server_info.catalog, name=server_info.schema_)
        print("Schema criado")
    else:
        print("Schema encontrado")

    run_ddl(w)


def run_ddl(w: WorkspaceClient):
    files = os.listdir("src/weather_lakehouse/ddl")

    for file in files:
        path = "src/weather_lakehouse/ddl/" + file
        with open(path, "r", encoding="utf-8") as f:
            ddl_query = f.read()

    response = w.statement_execution.execute_statement(
        statement=ddl_query, warehouse_id="a544623fcf87551c"
    )

    status = response.status.state
    statement_id = response.statement_id

    while status in [StatementState.PENDING, StatementState.RUNNING]:
        time.sleep(1)
        poll_response = w.statement_execution.get_statement(statement_id)
        status = poll_response.status.state

        if status == StatementState.SUCCEEDED:
            print(f"Instrução {statement_id} executada com sucesso!")
        else:
            error_msg = (
                poll_response.status.error.message
                if poll_response.status and poll_response.status.error
                else "Erro desconhecido"
            )
            raise RuntimeError(f"Falha na instrução {statement_id}: {error_msg}")


def generate_data_contract_ddl() -> None:
    # Função responsável por gerar os ddls dos schemas
    pass


def read_data_contract():

    base_dir = "./contracts/models/"
    files = os.listdir(base_dir)
    contract = [base_dir + file for file in files if file.endswith(".yaml")][0]

    data_contract = DataContract(data_contract_file=contract).get_data_contract()

    return data_contract


def setup_environment() -> None:
    # Deve fazer a leitura dos valores de custom property e configurar o env do ambiente de execução
    print("")


def create_tables():
    # Deverá ler na pasta DDL, validar, e executar todos os ddls encontrados
    print("")


main()
