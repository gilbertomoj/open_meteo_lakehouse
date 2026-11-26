# Open Meteo Lakehouse

Lakehouse de dados meteorológicos construído em Python, PySpark e Databricks. O projeto consulta a API de previsão da [Open-Meteo](https://open-meteo.com/), armazena arquivos Parquet em um volume do Unity Catalog e transforma os dados em tabelas Delta nas camadas Bronze, Silver e Gold.

> **Estado atual:** o repositório contém os scripts ETL e os recursos de jobs do Databricks descritos abaixo. A configuração principal está concentrada no catálogo/schema `workspace.default`. Os arquivos `lakehouse_silver_workflow.yml` e `lakehouse_gold_workflow.yml` estão vazios; as etapas Silver e Gold são hoje tarefas Spark Python do job de ingestão, não pipelines DLT separadas. Não há workflow de CI/CD (por exemplo, GitHub Actions) versionado neste repositório.

## Arquitetura

```text
Open-Meteo Forecast API
        │
        ▼
Ingestão Python (4 cidades, dados horários)
        │
        ▼
Volume Unity Catalog: Parquet
        │
        ▼
Bronze (Delta) ──► Silver (Delta) ──► Gold diária
                                      ├─► Gold semanal
                                      ├─► Gold mensal
                                      └─► Gold alertas diários
```

O job de ingestão encadeia as tarefas de arquivo, Bronze e Silver. Depois da Silver, as quatro tarefas Gold são executadas em paralelo. Um job separado prepara o schema e executa o DDL da Bronze.

## Jobs e pipelines Databricks

| Recurso | Configuração | O que faz |
|---|---|---|
| `weather_one_time_setup_job` | `resources/lakehouse_setup.yml` | Executa `setup_bronze.py` para ler o contrato, garantir o schema e executar o DDL da tabela Bronze. É uma etapa de setup separada; não é dependência automática do job de ingestão. |
| `weather_hourly_ingestion_job` | `resources/lakeouse_file_ingestion.yml` | Job agendado que grava Parquet e executa as transformações Bronze, Silver e Gold. Apesar do nome “hourly”, o cron atual `0 0 23 * * ?` executa **uma vez por dia às 23:00**, no fuso `America/Recife`. |

### Etapas do job de ingestão

1. **`camada_ingestao`** — `src/weather_lakehouse_etl/extractions/open_meteo_file_ingestion.py` consulta a previsão horária para Recife, São Paulo, Rio de Janeiro e Brasília. Persiste `city`, `datetime`, `temperature_2m`, `precipitation` e `wind_speed_10m` em um arquivo Parquet diário no volume `/Volumes/workspace/default/data/weather_data`.
2. **`camada_bronze`** — `open_meteo_bronze.py` lê o arquivo do dia, seleciona e renomeia as colunas, converte tipos e descarta linhas sem cidade ou timestamp. Acrescenta os registros à tabela Delta `workspace.default.weather_data_bronze`.
3. **`camada_silver`** — `open_meteo_silver.py` filtra e normaliza nomes de cidade, remove duplicatas pela chave cidade/timestamp, deriva `measurement_date` e valida intervalos básicos das medidas. Regrava a tabela `workspace.default.weather_data_silver` em modo overwrite.
4. **Gold diária** — `open_meteo_gold.py` agrega por cidade e data: médias, máximas e mínimas de temperatura, precipitação acumulada, estatísticas de vento e contagem de registros. Regrava `workspace.default.weather_data_gold`.
5. **Gold semanal** — `open_meteo_gold_weekly.py` agrega por cidade e início da semana, incluindo dias ativos. Faz merge/upsert em `workspace.default.weather_data_gold_weekly`.
6. **Gold mensal** — `open_meteo_gold_monthly.py` agrega por cidade e mês (`yyyy-MM`), incluindo dias chuvosos. Faz merge/upsert em `workspace.default.weather_data_gold_monthly`.
7. **Alertas Gold** — `open_meteo_gold_alerts_daily.py` calcula extremos diários e indicadores de chuva forte (>= 20 mm), vento forte (>= 40 km/h) e amplitude térmica alta (>= 12 °C). Faz merge/upsert em `workspace.default.weather_data_gold_alerts_daily`.

As tarefas Gold estão configuradas para depender somente de `camada_silver`, portanto podem executar em paralelo após a Silver. As tabelas semanais, mensais e de alertas selecionam períodos impactados recentemente para atualizar os resultados.

## Estrutura do repositório

```text
contracts/
  config/location.yml                   # Configuração de localização (atualmente não consumida pelo ETL)
  models/weather_data_bronze.yaml        # Contrato da tabela Bronze
resources/
  lakehouse_setup.yml                    # Job de setup do schema/DDL
  lakeouse_file_ingestion.yml            # Job agendado de ingestão e transformações
  lakehouse_silver_workflow.yml          # Arquivo vazio
  lakehouse_gold_workflow.yml            # Arquivo vazio
src/
  weather_lakehouse/                     # Código base do bundle e setup Bronze
  weather_lakehouse_etl/extractions/     # Scripts de ingestão e transformações
  weather_lakehouse_etl/transformations/ # Exemplos DLT ainda ligados a dados de táxi
tests/                                   # Teste de exemplo herdado do template Databricks
databricks.yml                           # Configuração do Databricks Asset Bundle
pyproject.toml                           # Dependências e metadados Python
uv.lock                                  # Lockfile do uv
```

O pacote `weather_lakehouse` inclui código remanescente do template Databricks (`main.py` e exemplo de táxis). As transformações em `weather_lakehouse_etl/transformations/` também são exemplos do template, não parte do fluxo meteorológico descrito acima.

## Requisitos

- Python `>=3.10,<3.13`
- [uv](https://docs.astral.sh/uv/)
- Databricks CLI e autenticação para um workspace
- Permissões de acesso ao catálogo/schema e a um SQL warehouse para o setup
- Volume Unity Catalog `workspace.default.data` disponível para leitura e gravação
- Databricks Runtime/ambiente compatível com as dependências Spark e Delta usadas pelos jobs

Dependências de desenvolvimento incluem `pytest`, `ruff`, Databricks Connect e `databricks-dlt`. O build de wheel é feito pelo Asset Bundle com `uv build --wheel`.

## Desenvolvimento local

```bash
uv sync --dev
```

Configure o perfil Databricks esperado pelo bundle (`DEFAULT`) seguindo a documentação da CLI. Para verificar a configuração antes do deploy:

```bash
databricks bundle validate --target dev
```

Os testes existentes usam Databricks Connect e exigem workspace/compute configurados:

```bash
uv run pytest
```

> **Nota sobre os testes:** `tests/sample_taxis_test.py` e o código de setup do pytest são exemplos do template Databricks. O teste importa `weather_lakehouse.taxis`, que não está implementado neste repositório; portanto, a suíte atual não representa testes automatizados do ETL meteorológico e pode falhar até que esse exemplo seja substituído ou completado.

## Deploy e execução

O `databricks.yml` define um Databricks Asset Bundle chamado `weather_lakehouse`, com targets:

- **`dev`** (padrão): modo development, catálogo `workspace` e schema derivado do usuário atual.
- **`prod`**: modo production, catálogo `workspace` e schema `prod`.

Para validar e publicar os recursos no workspace:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

O deploy de produção usa `--target prod`. O bundle monta um wheel e os YAMLs em `resources/` definem os jobs. Depois do deploy, os jobs podem ser iniciados pelo workspace Databricks (Jobs & Pipelines) ou pela CLI:

```bash
databricks bundle run weather_one_time_setup_job --target dev
databricks bundle run weather_hourly_ingestion_job --target dev
```

Execute o setup antes da primeira ingestão, confirme a existência do volume e valide que o job possui as permissões e dependências necessárias. O target `prod` não altera automaticamente os nomes de tabela nem o caminho do volume definidos como `workspace.default` no código ETL; revise esses valores antes de usá-lo em produção.

## Contrato de dados e tabelas

O contrato `contracts/models/weather_data_bronze.yaml` descreve a tabela `weather_data_bronze`, com cidade, timestamp, temperatura, precipitação e vento. Ele lista Recife, São Paulo, Rio de Janeiro e Brasília como localidades esperadas.

| Camada/tabela | Granularidade | Escrita |
|---|---|---|
| `workspace.default.weather_data_bronze` | Medição horária por cidade | Append |
| `workspace.default.weather_data_silver` | Medição horária deduplicada por cidade | Overwrite |
| `workspace.default.weather_data_gold` | Cidade/dia | Overwrite |
| `workspace.default.weather_data_gold_weekly` | Cidade/semana | Merge/upsert |
| `workspace.default.weather_data_gold_monthly` | Cidade/mês | Merge/upsert |
| `workspace.default.weather_data_gold_alerts_daily` | Cidade/dia e indicadores | Merge/upsert |

O DDL versionado em `src/weather_lakehouse/ddl/weather_data_bronze.sql` é uma materialização do contrato. Mantenha contrato, DDL e casts do ETL alinhados ao modificar o schema.

## Configurações importantes e pontos de atenção

- O caminho do volume, os nomes das tabelas e várias referências a catálogo/schema estão fixos nos scripts como `workspace.default`; os parâmetros `catalog` e `schema` do bundle não parametrizam essas referências hoje.
- A configuração `contracts/config/location.yml` não é consumida pelo script de ingestão, que mantém sua própria lista de cidades.
- O agendamento de produção presente no YAML é diário às 23:00, embora o recurso se chame `weather_hourly_ingestion_job`.
- A Bronze converte medidas para `DECIMAL(38,0)`, o que elimina casas decimais. O contrato descreve campos decimais com escala, então essa definição merece alinhamento.
- A Silver regrava a tabela inteira a cada execução; as Gold diária também são regravadas, enquanto as outras três saídas usam merge.
- O job de ingestão pressupõe que o arquivo Parquet correspondente à data atual já exista no volume. O fuso usado para formar o nome do arquivo vem do relógio local do processo; confirme que ele coincide com a programação e o fuso de ingestão.
- Não há workflow de CI/CD versionado em `.github/workflows/`. O agendamento atual é um recurso de job do Databricks, não uma pipeline de build/teste/deploy do GitHub.

## Licença

Distribuído sob a licença MIT. Consulte [LICENSE](LICENSE).
