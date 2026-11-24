-- Data Contract: my-data-contract-id
-- SQL Dialect: databricks
CREATE OR REPLACE TABLE workspace.default.weather_data_bronze (
  city STRING not null COMMENT "Nome da cidade referente à medição.",
  timestamp TIMESTAMP not null COMMENT "Data e hora da coleta da medição.",
  temperature DECIMAL(38,0) COMMENT "Temperatura registrada em graus Celsius.",
  precipitation DECIMAL(38,0) COMMENT "Volume de precipitação pluviométrica em mm.",
  wind DECIMAL(38,0) COMMENT "Velocidade do vento registrada em km/h."
) COMMENT "Tabela RAW para os dados temporais de clima.";