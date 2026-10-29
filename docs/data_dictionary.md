# Dicionário de Dados — Amazon Reviews Big Data Analytics

## 1. Fonte

- Dataset: Amazon Reviews 2023 — McAuley Lab
- Categoria: All Beauty
- Formato: Parquet
- Chave de relacionamento: `parent_asin`

## 2. Reviews — Campos principais esperados

| Campo | Tipo esperado | Descrição |
|---|---|---|
| parent_asin | string | Identificador do produto pai |
| rating | double | Nota atribuída pelo cliente |
| timestamp | long | Data da avaliação em Unix timestamp (milissegundos) |
| user_id | string | Identificador do avaliador |
| verified_purchase | boolean | Indicador de compra verificada |
| helpful_vote | long | Quantidade de votos úteis |

## 3. Products — Campos principais esperados

| Campo | Tipo esperado | Descrição |
|---|---|---|
| parent_asin | string | Identificador do produto pai |
| title | string | Nome do produto |
| average_rating | double | Média de avaliações informada na origem |
| rating_number | long | Quantidade de avaliações informada na origem |
| price | string | Preço bruto, sujeito à conversão |
| store | string | Loja ou marca associada |

## 4. Contrato preliminar

- Validar a presença e os tipos dos campos.
- Identificar valores nulos em `parent_asin`.
- Identificar duplicações nos produtos e avaliações.
- Verificar a cardinalidade do relacionamento.
- Validar notas entre 1 e 5.
- Verificar conversão de timestamps.
- Avaliar valores inválidos de preço.
- Medir correspondências entre avaliações e produtos.

## 5. Status

Schema dos dois arquivos validado com Apache Spark.

Exploração e verificações de qualidade realizadas
sobre os datasets completos na Etapa 2.

Regras de transformação e deduplicação ainda não
implementadas. Validações deverão ser repetidas
após o processamento na Silver.

## 6. Resultados da exploração

### 6.1. Volumetria

- Reviews: 701.528 registros.
- Products: 112.590 registros.
- Chaves parent_asin ausentes: 0 em ambos.
- Chaves parent_asin duplicadas em Products: 0.

### 6.2. Integridade referencial

- Reviews com produto correspondente: 701.528.
- Reviews sem produto correspondente: 0.
- Produtos com avaliações no arquivo Reviews: 112.565.
- Produtos sem avaliações no arquivo Reviews: 25.

### 6.3. Duplicações em Reviews

- Combinações repetidas de user_id, asin e timestamp: 6.139.
- Registros excedentes por essa combinação: 7.276.
- Cópias exatas excedentes: 7.275.
- Registros distintos completos: 694.253.
- Uma combinação possui duas versões distintas,
  cuja única diferença identificada é helpful_vote.

### 6.4. Qualidade dos campos

Reviews:
- rating: sem nulos e entre 1 e 5.
- helpful_vote: sem nulos ou valores negativos.
- timestamp: sem nulos ou valores não positivos.
- verified_purchase: sem nulos.

Products:
- average_rating: sem nulos, entre 1 e 5.
- rating_number: sem nulos, maior que zero.
- price: 94.886 registros com indicadores textuais
  de ausência ("None", "null" ou vazio).
- price: nenhum valor SQL NULL encontrado.

### 6.5. Divergências entre fontes

- Soma de rating_number: 5.690.450.
- Total de registros Reviews: 701.528.
- Produtos com contagens diferentes: 97.776.
- Produtos com contagens iguais: 14.814.

Investigação de cinco produtos com mais reviews
no Parquet do que nos metadados:
- Cópias exatas encontradas: 17.
- Divergência absoluta antes da deduplicação: 1.641.
- Divergência absoluta após a deduplicação: 1.624.

### 6.6. Decisões para as próximas etapas

- Preservar os dados brutos na Bronze.
- Definir deduplicação exata na Silver.
- Investigar a política para versões divergentes.
- Não presumir equivalência entre rating_number
  e quantidade de avaliações processadas.
- Calcular indicadores Gold a partir de dados limpos.
- Preservar produtos sem reviews quando apropriado.
- Validar o tratamento dos preços antes de usá-los
  em indicadores de negócio.


## Contrato de qualidade — Camada Silver

### silver_reviews_clean

Origem: bronze_reviews_raw

Regras:
- Remover duplicatas totalmente idênticas nos campos originais.
- Não usar metadados Bronze para definir duplicatas.
- Preservar avaliações que tenham diferenças reais.
- Converter timestamp Unix em milissegundos para UTC.
- Validar rating entre 1 e 5.
- Validar helpful_vote não negativo.
- Preservar parent_asin para análises por produto.

Contagem inicial esperada após deduplicação: 694253.

### silver_products_clean

Origem: bronze_products_raw

Regras:
- Preservar parent_asin como identificador.
- Tratar indicadores textuais de preço ausente como NULL.
- Converter preços válidos para tipo numérico.
- Não substituir preços ausentes por zero.
- Investigar preços que não puderem ser convertidos.
- Validar average_rating entre 0 e 5.
- Validar rating_number não negativo.
- Preservar produtos sem avaliações.

Contagem inicial esperada: 112590.

### Princípios

- Bronze permanece imutável durante as transformações.
- Não descartar dados sem uma regra justificada.
- Priorizar qualidade, rastreabilidade e baixo consumo.
- Validar todas as contagens após o processamento.


## Resultados de validação — Camada Silver

### Infraestrutura

- Pipeline: amazon_reviews_pipeline
- Compute: Serverless
- Modo: Triggered
- Arquivo de transformação: pipelines/silver.py
- Tipo das tabelas: Materialized Views
- Destino: workspace.amazon_reviews

### silver_reviews_clean

Origem: bronze_reviews_raw

Resultados:
- Registros Bronze: 701528
- Registros Silver: 694253
- Duplicatas exatas removidas: 7275
- Timestamps nulos: 0
- Timestamps divergentes: 0
- Ratings inválidos: 0
- Helpful votes inválidos: 0

Deduplicação:
- Comparação das 10 colunas originais.
- Metadados de ingestão não participam da comparação.
- Duas avaliações com a mesma chave operacional,
  mas conteúdos diferentes, foram preservadas.

Transformações:
- Remoção de duplicatas exatas.
- Criação de review_timestamp_utc.
- Preservação do timestamp original.
- Monitoramento de regras de qualidade.

### silver_products_clean

Origem: bronze_products_raw

Resultados:
- Registros Bronze: 112590
- Registros Silver: 112590
- Identificadores distintos: 112590
- Identificadores inválidos: 0
- Ratings inválidos: 0
- Contagens inválidas: 0

Preços:
- Preços válidos: 17704
- Preços ausentes: 94886
- Preços inválidos: 0
- Status incoerentes: 0
- Preços divergentes: 0
- Preços negativos: 0
- Menor preço numérico: 0.01
- Maior preço numérico: 2548.98

Transformações:
- Preservação do preço original em price_raw.
- Conversão para price DECIMAL(12,2).
- Classificação na coluna price_status.
- Preservação de preços ausentes como NULL.
- Preservação de todos os produtos.

### Considerações de qualidade

- As expectativas monitoram violações sem
  descartar automaticamente os registros.
- As regras verificadas não apresentaram violações.
- Preços ausentes não foram substituídos por zero.
- A unidade monetária não foi confirmada.
- Valores extremos não foram removidos.
- As tabelas Bronze permanecem preservadas.

### Resultado

As duas Materialized Views Silver foram criadas
e aprovadas nas validações realizadas.


## Contrato de métricas — Camada Gold

### Objetivo

Disponibilizar indicadores analíticos de produtos e
avaliações para Dashboard e Databricks Genie.

### gold_product_metrics

Origem: silver_reviews_clean
Granularidade: uma linha por parent_asin avaliado.

Métricas:
- reviews_count: quantidade de avaliações deduplicadas.
- average_review_rating: média das notas individuais.
- verified_review_count: quantidade de compras verificadas.
- verified_review_pct: percentual de avaliações verificadas.
- total_helpful_votes: soma dos votos de utilidade.

Percentual verificado:
100 * verified_review_count / reviews_count

### gold_monthly_review_metrics

Origem: silver_reviews_clean
Granularidade: uma linha por mês UTC.

Métricas:
- review_month: mês de referência.
- reviews_count: quantidade de avaliações.
- reviewed_products_count: produtos distintos avaliados.
- average_review_rating: média das notas do mês.
- verified_review_count: compras verificadas.
- verified_review_pct: percentual de avaliações verificadas.

### gold_genie_product_analytics

Origens:
- silver_products_clean
- gold_product_metrics

Granularidade: uma linha por produto.

Relacionamento:
LEFT JOIN por parent_asin.

Regras:
- Preservar todos os produtos.
- Produtos sem avaliações recebem reviews_count = 0.
- Médias sem avaliações permanecem NULL.
- Manter os preços ausentes como NULL.
- Documentar os indicadores para o Genie.

Campos importantes:
- parent_asin
- product_title
- main_category
- price
- price_status
- reviews_count
- average_review_rating
- source_average_rating
- source_rating_number

### Regras gerais

- Utilizar os dados preparados da Silver.
- Não utilizar a Bronze diretamente nas métricas Gold.
- Não confundir avaliações disponíveis no dataset
  com as contagens informadas nos metadados originais.
- Não calcular receita ou vendas sem dados transacionais.
- Manter uma granularidade definida para cada tabela.
- Validar as contagens e os cálculos após a execução.



## Resultados validados — Camada Gold

### Infraestrutura

- Pipeline: amazon_reviews_pipeline
- Arquivo: pipelines/gold.py
- Catálogo: workspace
- Schema: amazon_reviews
- Tipo de saída: Materialized Views
- Processamento: Serverless e Triggered

### gold_product_metrics

Granularidade: uma linha por produto avaliado.

Resultados:
- Produtos avaliados: 112565
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Votos de utilidade: 641610
- Métricas inválidas detectadas: 0

### gold_monthly_review_metrics

Granularidade: uma linha por mês UTC com avaliações.

Resultados:
- Meses com avaliações: 265
- Primeiro mês: 2000-11-01
- Último mês: 2023-09-01
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Métricas inválidas detectadas: 0

Observação:
- O período de 2023 é parcial.
- As médias mensais não devem ser combinadas
  por uma média simples sem ponderação.

### gold_genie_product_analytics

Granularidade: uma linha por produto cadastrado.

Fontes:
- silver_products_clean
- gold_product_metrics

Relacionamento: LEFT JOIN por parent_asin.

Resultados:
- Produtos totais: 112590
- Produtos distintos: 112590
- Produtos com avaliações: 112565
- Produtos sem avaliações: 25
- Avaliações representadas: 694253
- Avaliações verificadas: 628456
- Votos de utilidade: 641610
- Produtos com preço numérico: 17704
- Métricas inconsistentes detectadas: 0

### Semântica dos indicadores para o Genie

reviews_count:
Quantidade de avaliações deduplicadas disponíveis
no dataset Silver para o produto.

source_rating_number:
Quantidade de avaliações informada nos metadados
originais. Pode ser diferente de reviews_count.

average_review_rating:
Média das avaliações disponíveis na Silver,
arredondada para duas casas decimais.

source_average_rating:
Média informada nos metadados originais.
Não é necessariamente igual a average_review_rating.

verified_review_count:
Quantidade de avaliações com compra verificada
no dataset Silver.

verified_review_pct:
Percentual das avaliações disponíveis que possuem
compra verificada. Não representa taxa de vendas.

total_helpful_votes:
Soma dos votos de utilidade das avaliações Silver.

price:
Preço numérico disponível nos metadados.
A unidade monetária não foi confirmada.

price_status:
Classificação do preço como valido, ausente
ou invalido.

### Regras para consultas analíticas

- Para contar avaliações disponíveis, usar reviews_count.
- Para calcular notas com base no dataset, usar
  average_review_rating.
- Não confundir source_rating_number com reviews_count.
- Não confundir source_average_rating com
  average_review_rating.
- Produtos sem avaliações possuem reviews_count = 0.
- Produtos sem avaliações possuem médias calculadas NULL.
- Preços ausentes não representam preço zero.
- Não interpretar avaliações verificadas como vendas.
- Não calcular faturamento sem dados de transações.
- Informar que os dados de 2023 são parciais.

### Status

As três Materialized Views Gold foram criadas
e aprovadas nas validações realizadas.
