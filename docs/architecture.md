# Arquitetura — Amazon Reviews Big Data Analytics

## 1. Objetivo

Construir uma solução de Engenharia de Dados em batch utilizando Databricks, Apache Spark, PySpark e Delta Lake.

O projeto emprega a arquitetura Medallion (Bronze, Silver e Gold) para processar avaliações e metadados de produtos da Amazon.

## 2. Tecnologias

- Databricks Free Edition
- Apache Spark
- Delta Lake
- Unity Catalog
- Lakeflow Spark Declarative Pipelines
- Databricks AI/BI Dashboard
- Databricks Genie
- Git e GitHub

## 3. Fontes de dados

**Origem:** Amazon Reviews 2023 — McAuley Lab.

**Categoria:** All Beauty.

- `raw_review_All_Beauty`: avaliações de clientes.
- `raw_meta_All_Beauty`: metadados de produtos.

Formato: Parquet.

Chave de relacionamento prevista: `parent_asin`.

Os arquivos serão armazenados no Managed Volume `workspace.amazon_reviews.raw_files`.

## 4. Arquitetura Medallion

### Bronze — Ingestão

- `bronze_reviews_raw`
- `bronze_products_raw`

Responsabilidades: preservar os registros de origem, manter rastreabilidade da ingestão e permitir reexecuções seguras.

### Silver — Transformação e qualidade

- `silver_reviews_clean`
- `silver_products_clean`

Responsabilidades: padronizar tipos, tratar dados inválidos, definir políticas de deduplicação e aplicar regras de qualidade.

### Gold — Modelagem analítica

- `gold_product_metrics`
- `gold_monthly_review_metrics`
- `gold_genie_product_analytics`

Responsabilidades: relacionar avaliações e produtos, calcular indicadores e disponibilizar dados para o Dashboard e Genie.

As granularidades planejadas são produto, mês e produto, respectivamente, sujeitas à validação dos dados reais.

## 5. Orquestração

Um único Lakeflow Spark Declarative Pipeline realizará o processamento das três camadas.

As transformações definitivas serão organizadas em arquivos Python.

Os notebooks serão utilizados para exploração dos dados e experimentos de performance.

## 6. Qualidade e confiabilidade

Serão implementados e validados:

- Contratos de dados.
- Verificações de nulos e duplicações.
- Validação de cardinalidade dos JOINs.
- Idempotência.
- Processamento incremental quando apropriado.
- Tratamento de erros.
- Testes automatizados.
- Monitoramento das execuções.

## 7. Performance

Serão utilizados planos de execução e métricas do Spark para investigar JOINs, shuffles e gargalos.

As otimizações serão justificadas por medições antes e depois das alterações.

## 8. Consumo e infraestrutura

O projeto utilizará a Databricks Free Edition, priorizando computação Serverless, processamento sob demanda e uso controlado de recursos.

Não serão criados Jobs adicionais nem serviços externos de orquestração.

## 9. Consumo analítico

Um AI/BI Dashboard apresentará indicadores de avaliações e produtos.

Um Genie Space permitirá consultas em linguagem natural sobre uma tabela Gold documentada.

## 10. Status

**Arquitetura planejada — implementação pendente.**

As decisões referentes a schemas, granularidade, deduplicação e estratégias incrementais poderão ser refinadas mediante evidências obtidas nas etapas de desenvolvimento.

## Camada Bronze — Implementação

### Objetivo

Realizar a ingestão incremental dos arquivos Parquet
do Amazon Reviews 2023, preservando os dados originais
em tabelas Delta gerenciadas pelo Lakeflow.

### Infraestrutura

- Plataforma: Databricks
- Catálogo: workspace
- Schema: amazon_reviews
- Pipeline: amazon_reviews_pipeline
- Compute: Serverless
- Modo: Triggered
- Processamento: Apache Spark
- Formato de destino: Delta Lake
- Ingestão: Auto Loader

### Fontes de dados

Reviews:
`/Volumes/workspace/amazon_reviews/raw_files/reviews/`

Products:
`/Volumes/workspace/amazon_reviews/raw_files/products/`

### Tabelas Bronze

| Tabela | Registros |
|---|---:|
| bronze_reviews_raw | 701528 |
| bronze_products_raw | 112590 |

### Implementação

Arquivo: `pipelines/bronze.py`

As duas tabelas utilizam:

- `@dp.table` para definição declarativa.
- `spark.readStream` para leitura incremental.
- `cloudFiles` para descoberta dos arquivos Parquet.
- Unity Catalog para gerenciamento das tabelas.

### Rastreabilidade

Campos adicionados:

- `_source_file`
- `_source_file_modified_at`
- `_ingested_at`

O Auto Loader também disponibiliza `_rescued_data`.

### Validações realizadas

- Pipeline executado com sucesso.
- Quantidades conferidas com os arquivos originais.
- Metadados de rastreabilidade preenchidos.
- Reexecuções normais sem aumento das contagens.
- Idempotência de Products validada por comparação
  das contagens e limites dos timestamps.
- Nenhum registro com `_rescued_data` preenchido.

### Resultado

| Indicador | Reviews | Products |
|---|---:|---:|
| Total de registros | 701528 | 112590 |
| Registros resgatados | 0 | 0 |

### Regras da Bronze

- Preservar os dados originais.
- Não remover duplicatas.
- Não filtrar registros por regras de negócio.
- Não modificar os preços.
- Não transformar timestamps de avaliação.
- Manter estruturas aninhadas da origem.



## Camada Silver — Arquitetura implementada

### Objetivo

Transformar os dados brutos da Bronze em conjuntos
padronizados e validados para análises posteriores.

### Processamento

- Pipeline: amazon_reviews_pipeline
- Catálogo: workspace
- Schema: amazon_reviews
- Compute: Serverless
- Modo de execução: Triggered
- Arquivo: pipelines/silver.py
- Tecnologia: PySpark e Lakeflow Pipelines
- Tipo de saída: Materialized Views

### Fluxo de dados

bronze_reviews_raw
    -> silver_reviews_clean

bronze_products_raw
    -> silver_products_clean

### Silver Reviews

Origem: workspace.amazon_reviews.bronze_reviews_raw
Destino: workspace.amazon_reviews.silver_reviews_clean

Transformações:
- Seleção das 10 colunas originais.
- Remoção das duplicatas exatamente iguais.
- Conversão de timestamp Unix em milissegundos.
- Criação de review_timestamp_utc.
- Monitoramento de regras de qualidade.

Resultados:
- Registros de origem: 701528
- Registros transformados: 694253
- Duplicatas removidas: 7275
- Timestamps divergentes: 0
- Ratings inválidos: 0
- Helpful votes inválidos: 0

### Silver Products

Origem: workspace.amazon_reviews.bronze_products_raw
Destino: workspace.amazon_reviews.silver_products_clean

Transformações:
- Preservação dos 112590 produtos.
- Preservação do preço textual em price_raw.
- Conversão do preço para DECIMAL(12,2).
- Criação da classificação price_status.
- Monitoramento de regras de qualidade.

Resultados:
- Produtos preservados: 112590
- Preços convertidos: 17704
- Preços ausentes: 94886
- Preços inválidos: 0
- Identificadores inválidos: 0

### Governança e qualidade

As duas Materialized Views utilizam expectativas
de qualidade para monitorar violações.

As expectativas não descartam automaticamente
os registros.

Os dados originais permanecem disponíveis nas
tabelas Bronze.

### Relacionamento entre entidades

Chave de relacionamento: parent_asin

As duas tabelas Silver permanecem separadas.

O relacionamento será utilizado na camada Gold
para produzir indicadores de avaliações e produtos.

### Estado da implementação

- 2 Streaming Tables Bronze validadas.
- 2 Materialized Views Silver validadas.
- 1 Lakeflow Pipeline.
- Processamento Serverless.
- Execução Triggered.



## Camada Gold — Arquitetura implementada

### Objetivo

Disponibilizar dados agregados e indicadores de negócio
para análises de produtos, evolução de avaliações,
Dashboards e consultas futuras com Databricks Genie.

### Infraestrutura

- Pipeline: amazon_reviews_pipeline
- Catálogo: workspace
- Schema: amazon_reviews
- Compute: Serverless
- Modo: Triggered
- Arquivo: pipelines/gold.py
- Tecnologia: PySpark e Delta Lake
- Saídas: 3 Materialized Views

### Dependências

silver_reviews_clean
    -> gold_product_metrics
    -> gold_genie_product_analytics

silver_reviews_clean
    -> gold_monthly_review_metrics

silver_products_clean
    -> gold_genie_product_analytics

### gold_product_metrics

Origem: silver_reviews_clean

Granularidade:
Uma linha por produto avaliado (parent_asin).

Métricas:
- Quantidade de avaliações deduplicadas.
- Média das avaliações.
- Quantidade de compras verificadas.
- Percentual de avaliações verificadas.
- Total de votos de utilidade.

Resultados:
- Produtos avaliados: 112565
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Votos de utilidade: 641610
- Métricas inválidas: 0

### gold_monthly_review_metrics

Origem: silver_reviews_clean

Granularidade:
Uma linha por mês UTC com avaliações.

Resultados:
- Meses representados: 265
- Primeiro mês: 2000-11-01
- Último mês: 2023-09-01
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Métricas inválidas: 0

Observação:
O ano de 2023 possui cobertura parcial.

### gold_genie_product_analytics

Origens:
- silver_products_clean
- gold_product_metrics

Relacionamento:
LEFT JOIN por parent_asin.

Granularidade:
Uma linha por produto cadastrado.

Resultados:
- Produtos totais: 112590
- Produtos com avaliações: 112565
- Produtos sem avaliações: 25
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Votos de utilidade: 641610
- Preços numéricos: 17704
- Métricas inconsistentes: 0

### Regras de modelagem

- Preservar produtos sem avaliações.
- Representar contagens ausentes como zero.
- Manter médias sem observações como NULL.
- Não substituir preços ausentes por zero.
- Diferenciar métricas calculadas das métricas
  informadas nos metadados originais.
- Não inferir vendas ou faturamento das avaliações.

### Objetos implementados

Bronze:
- bronze_reviews_raw (Streaming Table)
- bronze_products_raw (Streaming Table)

Silver:
- silver_reviews_clean (Materialized View)
- silver_products_clean (Materialized View)

Gold:
- gold_product_metrics (Materialized View)
- gold_monthly_review_metrics (Materialized View)
- gold_genie_product_analytics (Materialized View)

Total: 7 objetos gerenciados pelo Lakeflow.

### Consumo analítico planejado

Dashboard:
- Indicadores de avaliações.
- Comparações entre produtos.
- Evolução mensal.

Databricks Genie:
- Consultas em linguagem natural.
- Fonte principal: gold_genie_product_analytics.
- Métricas e descrições documentadas.



## Dashboard AI/BI — Amazon Reviews

### Objetivo

Disponibilizar indicadores e visualizações
interativas para análise de avaliações de produtos
da categoria All Beauty, utilizando dados Gold
processados no Databricks.

### Tecnologia

- Ferramenta: Databricks AI/BI Dashboards
- Nome: Amazon Reviews — Big Data Analytics
- Fonte: Unity Catalog
- Catálogo: workspace
- Schema: amazon_reviews
- Execução de consultas: SQL Warehouse

### Fontes Gold

- gold_genie_product_analytics
- gold_monthly_review_metrics

### Datasets SQL

1. kpi_resumo_geral
   Indicadores agregados de produtos e avaliações.

2. evolucao_mensal_avaliacoes
   Quantidade de avaliações e percentual mensal
   de avaliações verificadas.

3. top_10_produtos_avaliados
   Ranking dos dez produtos mais avaliados.

4. distribuicao_por_categoria
   Quantidade de produtos e avaliações por
   categoria. Dataset mantido para exploração;
   não utilizado em gráfico na versão atual.

### Indicadores principais

- Total de avaliações: 694253
- Total de produtos: 112590
- Avaliações verificadas: 628456
- Total de votos úteis: 641610

### Visualizações implementadas

1. Evolução Mensal das Avaliações
   - Tipo: gráfico de linhas
   - Métrica: reviews_count
   - Dimensão: review_month

2. Top 10 Produtos Mais Avaliados
   - Tipo: barras horizontais
   - Métrica: reviews_count
   - Dimensão: product_name

3. Evolução Mensal de Avaliações Verificadas (%)
   - Tipo: gráfico de linhas
   - Métrica: verified_review_pct
   - Dimensão: review_month

### Interatividade

Filtro: Período das Avaliações

Campo: review_month

Dataset: evolucao_mensal_avaliacoes

O filtro atua somente nas duas visualizações
mensais, preservando os KPIs gerais e o Top 10.

### Publicação e permissões

- Estado: Published
- Data permissions: Individual data permissions
- Compartilhamento amplo: não habilitado
- Atualização automática agendada: não configurada

A visualização dos dados depende das permissões
individuais de cada usuário.

### Atualização dos dados

As tabelas Gold são mantidas pelo Lakeflow
Spark Declarative Pipeline em modo Triggered.

O Dashboard consulta os resultados disponíveis
nas tabelas Gold.

O refresh do Dashboard não equivale à execução
do pipeline de transformação.

### Limitações

- Dataset restrito à categoria All Beauty.
- Dados históricos com cobertura parcial de 2023.
- Avaliações não representam vendas.
- Preços ausentes não representam preço zero.
- As métricas representam os dados disponíveis
  após deduplicação.
- O filtro temporal utiliza granularidade mensal.

### Estado da implementação

Dashboard publicado e validado com:

- 4 KPIs
- 3 gráficos analíticos
- 4 datasets SQL
- 1 filtro interativo
- 1 seção de metodologia e limitações


## Databricks Genie — Análises em linguagem natural

### Objetivo

Permitir consultas de negócio em português brasileiro
sobre produtos e avaliações da categoria All Beauty,
utilizando o Databricks Genie e dados preparados
na camada Gold.

### Arquitetura

- Ferramenta: Databricks Genie
- Nome: Amazon Reviews — Genie Product Analytics
- Fonte: workspace.amazon_reviews.gold_genie_product_analytics
- Compute: Serverless SQL Warehouse
- Quantidade de fontes: 1
- Granularidade: um registro por parent_asin
- Produtos disponíveis: 112590

O Genie consulta a tabela Gold por meio de SQL.
Não realiza ingestão ou transformação dos dados
e não substitui o Lakeflow Pipeline.

### Configuração semântica

O Genie possui:

- Descrição do contexto de negócio.
- General instructions em português brasileiro.
- Regras para interpretação das métricas.
- Orientações sobre limitações dos dados.
- Cinco exemplos SQL executados e validados.

### Exemplos SQL cadastrados

1. Básico:
   Contagem de produtos e avaliações.

2. Intermediário:
   Ranking dos dez produtos mais avaliados.

3. Intermediário:
   Percentual de avaliações verificadas por categoria.

4. Avançado:
   Ranking dos três produtos mais avaliados por categoria,
   utilizando funções de janela.

5. Avançado:
   Reconciliação entre médias calculadas e médias
   informadas nos metadados originais.

### Regras importantes

- COUNT(*) representa quantidade de produtos.
- SUM(reviews_count) representa avaliações disponíveis.
- SUM(verified_review_count) representa avaliações
  com compra verificada.
- O percentual geral verificado deve utilizar a razão
  entre as somas, não a média simples dos percentuais.
- source_rating_number é diferente de reviews_count.
- source_average_rating é diferente de
  average_review_rating.
- Preços NULL não representam preço zero.
- A moeda dos preços não foi confirmada.
- Avaliações não representam vendas ou faturamento.
- A tabela não permite análises temporais por mês.

### Validação das respostas

Foram realizados seis testes em linguagem natural.

Teste 7.5 — Total de avaliações
- Resultado: 694253
- Situação: aprovado
- SQL validado pelo usuário.

Teste 7.6 — Produtos sem avaliações
- Resultado: 25
- Situação: aprovado
- Condição: reviews_count = 0

Teste 7.7 — Percentual geral verificado
- Resultado: 90,52%
- Situação: aprovado
- Cálculo: SUM(verified_review_count)
  dividido por SUM(reviews_count), vezes 100.

Teste 7.8 — Ranking de avaliações verificadas
- Resultado: 5 produtos
- Situação: aprovado
- Ordenação: verified_review_count DESC

Teste 7.9 — Reconciliação das médias
- Resultado: 5 produtos
- Situação: aprovado
- Critérios: reviews_count >= 30
  e diferença absoluta entre as médias.

Teste 7.10 — Pergunta sobre faturamento
- Resultado: dados insuficientes
- Situação: aprovado
- O Genie reconheceu a ausência de dados de vendas
  e não apresentou faturamento fictício.

### Observações da validação

- O SQL do teste 7.5 não foi reproduzido na documentação
  da revisão, mas o usuário confirmou sua validação.
- O ranking do teste 7.8 poderia incluir parent_asin
  como critério adicional de desempate.
- O teste 7.9 poderia ordenar pelas diferenças
  antes do arredondamento para maior precisão.
- Os testes aprovados demonstram resultados corretos
  para as perguntas avaliadas, sem garantir precisão
  para todas as perguntas possíveis.

### Limitações e segurança

- Uma única tabela Gold serve como fonte.
- Não existem dados de vendas, faturamento ou pedidos.
- Os metadados podem representar mais avaliações
  do que as disponíveis no dataset processado.
- Os dados possuem cobertura histórica parcial em 2023.
- O acesso aos dados depende das permissões
  do usuário no Unity Catalog.
- Não foram criados agentes personalizados adicionais,
  RAG ou bancos vetoriais.

### Estado da implementação

- Genie criado e configurado.
- Uma tabela Gold associada.
- Cinco exemplos SQL cadastrados.
- Seis perguntas de validação aprovadas.
- Nenhuma alteração adicional na pipeline.
