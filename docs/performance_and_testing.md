# Performance e Testes — Amazon Reviews Big Data Analytics

## 1. Objetivo

Documentar os testes de performance, qualidade de dados, integridade e idempotência realizados no projeto Amazon Reviews Big Data Analytics.

Os testes foram executados no Databricks Free Edition, utilizando Apache Spark, Delta Lake e Lakeflow Spark Declarative Pipelines.

## 2. Ambiente e arquitetura

- Plataforma: Databricks Free Edition
- Compute: Serverless
- Linguagem: Python / PySpark / SQL
- Catálogo: `workspace`
- Schema: `amazon_reviews`
- Pipeline: `amazon_reviews_pipeline`
- Notebook de testes: `02_spark_performance`
- Arquitetura: Medallion (Bronze, Silver e Gold)
- Total de tabelas: 7

As análises foram realizadas utilizando consultas de leitura, sem alterações manuais nas tabelas.

## 3. Performance do Apache Spark

### 3.1 Plano físico de leitura e filtragem

Foi analisada uma consulta à tabela `gold_genie_product_analytics`, selecionando apenas `parent_asin` e `reviews_count` e aplicando o filtro `reviews_count >= 100`.

Observações:

- Leitura de somente duas colunas.
- Identificação de `PhotonScan parquet`.
- Filtros presentes em `DictionaryFilters` e `RequiredDataFilters`.
- Consulta totalmente suportada pelo Photon.
- Nenhum operador explícito de shuffle no plano apresentado.

O teste demonstrou a aplicação de projeção de colunas e o encaminhamento de filtros à leitura.

### 3.2 Medição com Query Profile

Resultado da consulta de contagem:

| Métrica | Resultado |
|---|---:|
| Produtos com 100 ou mais avaliações | 732 |
| Duração observada | 6 segundos |
| Volume lido | 325 KB |

O tempo representa uma execução específica e não constitui um benchmark reproduzível.

### 3.3 Agregação por categoria

Foi analisado o plano de execução de uma agregação por `main_category`, utilizando `COUNT(*)` e `SUM(reviews_count)`.

O plano apresentou:

- `PhotonScan parquet`
- `PhotonGroupingAgg`
- `PhotonColumnarToRow`
- `PhotonResultStage`
- `AdaptiveSparkPlan`

O plano inicial não apresentou shuffle explícito, mas continha `isFinalPlan=false`.

A ausência de shuffle na execução final não foi comprovada.

Resultados executados:

| Categoria | Produtos | Avaliações |
|---|---:|---:|
| All Beauty | 112.135 | 687.166 |
| Premium Beauty | 455 | 7.087 |
| **Total** | **112.590** | **694.253** |

Métricas de execução:

- Duração observada: 1 segundo.
- Volume lido: 326 KB.

Não foi realizada comparação direta de velocidade entre as consultas, pois possuem operações e condições de execução diferentes.

### 3.4 Estatísticas do otimizador

Os planos apresentaram estatísticas ausentes para a estrutura interna de materialização da Gold.

Não foi executado `ANALYZE TABLE`, pois não identificamos evidência de um gargalo que justificasse essa operação.

## 4. Qualidade de dados — Bronze e Silver

### 4.1 Reconciliação de registros

| Indicador | Resultado |
|---|---:|
| Avaliações Bronze | 701.528 |
| Avaliações Silver | 694.253 |
| Registros removidos | 7.275 |
| Percentual removido | 1,04% |

A diferença quantitativa corresponde à quantidade de cópias excedentes identificadas.

### 4.2 Validação de duplicatas

Resultados do teste com assinaturas SHA-256:

| Indicador | Resultado |
|---|---:|
| Grupos de cópias exatas | 6.138 |
| Registros nesses grupos | 13.413 |
| Cópias excedentes | 7.275 |
| Máximo de repetições | 10 |

A exploração inicial havia identificado 6.140 registros distintos entre 13.415 registros suspeitos.

A diferença ocorre porque a exploração primeiro selecionava registros pela combinação `user_id`, `asin` e `timestamp`, enquanto o teste posterior agrupou assinaturas dos dez campos originais.

Os métodos produziram a mesma quantidade de 7.275 cópias excedentes. A identidade dos dois registros adicionais considerados suspeitos não foi verificada individualmente.

### 4.3 Integridade de registros

Foi utilizada a operação `exceptAll()` nos dois sentidos para comparar a Bronze deduplicada com a Silver.

Resultados:

- Registros esperados ausentes na Silver: 0.
- Registros inesperados na Silver: 0.

A comparação demonstrou equivalência dos registros considerando os dez campos originais e suas multiplicidades.

## 5. Qualidade de dados — Gold de produtos

Tabela validada: `gold_genie_product_analytics`.

### 5.1 Reconciliação Silver × Gold

- Avaliações Silver: 694.253.
- Avaliações Gold: 694.253.
- Produtos Silver: 112.590.
- Produtos Gold: 112.590.
- Produtos com avaliações: 112.565.
- Produtos sem avaliações: 25.

Todas as contagens corresponderam aos valores esperados.

### 5.2 Validação por produto

A quantidade de avaliações foi comparada individualmente por `parent_asin`.

Resultado: zero divergências em 112.590 produtos.

### 5.3 Integridade das chaves

- Registros totais: 112.590.
- Chaves distintas: 112.590.
- Chaves duplicadas: 0.
- Registros excedentes: 0.
- Identificadores nulos: 0.
- Identificadores vazios: 0.

A tabela respeita a granularidade de um registro por produto no estado validado.

### 5.4 Compras verificadas e votos úteis

Resultados:

| Indicador | Silver | Gold |
|---|---:|---:|
| Avaliações verificadas | 628.456 | 628.456 |
| Votos úteis | 641.610 | 641.610 |

Foram identificadas zero divergências por produto nas duas métricas.

### 5.5 Notas médias e percentuais

Foram comparadas as métricas `average_review_rating` e `verified_review_pct` da Gold com os cálculos reconstruídos da Silver.

Resultados:

- Produtos comparados: 112.590.
- Divergências de notas médias: 0.
- Divergências de percentuais verificados: 0.
- Produtos sem avaliações corretamente tratados com valores nulos: 25.

Foi utilizada tolerância absoluta de 0,011 para diferenças de arredondamento.

## 6. Qualidade de dados — Gold mensal

Tabela validada: `gold_monthly_review_metrics`.

### 6.1 Reconciliação mensal

Resultados:

- Meses comparados: 265.
- Meses ausentes na Gold: 0.
- Meses extras na Gold: 0.
- Divergências nas contagens de avaliações: 0.
- Divergências nas avaliações verificadas: 0.

Totais preservados:

- Avaliações: 694.253.
- Avaliações verificadas: 628.456.

### 6.2 Métricas mensais derivadas

Foram reconstruídas, a partir da Silver, as métricas:

- Nota média mensal.
- Percentual mensal de avaliações verificadas.
- Produtos distintos avaliados por mês.

Resultados:

- Meses comparados: 265.
- Divergências de notas médias: 0.
- Divergências de percentuais: 0.
- Divergências de produtos distintos: 0.

Para médias e percentuais, foi utilizada tolerância absoluta de 0,011. As quantidades de produtos foram comparadas sem tolerância.

## 7. Testes de idempotência

### 7.1 Baseline

Foram registradas as contagens das sete tabelas antes da atualização.

| Camada | Tabela | Registros |
|---|---|---:|
| Bronze | bronze_reviews_raw | 701.528 |
| Bronze | bronze_products_raw | 112.590 |
| Silver | silver_reviews_clean | 694.253 |
| Silver | silver_products_clean | 112.590 |
| Gold | gold_product_metrics | 112.565 |
| Gold | gold_monthly_review_metrics | 265 |
| Gold | gold_genie_product_analytics | 112.590 |

Os arquivos de origem e as definições do pipeline foram confirmados como inalterados.

### 7.2 Reexecução do Lakeflow

- Pipeline: `amazon_reviews_pipeline`
- Tipo: Refresh padrão
- Status: concluído com sucesso
- Duração: 2 minutos e 4 segundos
- Full refresh: não utilizado
- Erros relatados: nenhum

### 7.3 Idempotência quantitativa

Após o Refresh, as sete tabelas mantiveram exatamente as mesmas contagens.

Resultado: **7 de 7 tabelas aprovadas, com zero diferenças quantitativas**.

### 7.4 Preservação dos indicadores de negócio

Os indicadores permaneceram inalterados após o Refresh:

- Produtos: 112.590.
- Produtos avaliados: 112.565.
- Produtos sem avaliações: 25.
- Avaliações: 694.253.
- Avaliações verificadas: 628.456.
- Votos úteis: 641.610.
- Meses com avaliações: 265.

As duas Gold também continuaram consistentes entre si.

### 7.5 Limitações do teste de idempotência

Foi demonstrada idempotência quantitativa e preservação dos indicadores avaliados em uma atualização padrão sem mudanças nas fontes.

Não foi comprovada igualdade integral de conteúdo entre versões anteriores e posteriores de todas as tabelas, pois não foram registrados snapshots ou hashes completos antes do Refresh.

Também não foram testados cenários de falha intermediária, novos arquivos ou Full refresh.

## 8. Boas práticas adotadas

- Utilização de Serverless.
- Consultas com projeção de colunas.
- Inspeção dos planos físicos com `explain()`.
- Utilização de Photon quando suportado.
- Análise de métricas com Query Profile.
- Reconciliação de contagens entre camadas.
- Validações por chave de negócio.
- Comparações bidirecionais de registros.
- Tolerâncias explícitas para métricas arredondadas.
- Evitar `cache()`, `repartition()` e operações de manutenção sem evidência de necessidade.
- Evitar reprocessamentos completos desnecessários.
- Registrar limitações e resultados efetivamente observados.

## 9. Conclusão

Os testes executados não identificaram divergências nas transformações e métricas avaliadas das camadas Bronze, Silver e Gold.

O pipeline demonstrou consistência quantitativa, integridade das métricas de negócio e preservação das contagens após uma atualização padrão sem alterações nas fontes.

As consultas de performance apresentaram planos compatíveis com Photon e volumes de leitura pequenos nos casos observados.

Os resultados constituem evidências de qualidade e confiabilidade nos cenários testados, sem garantir ausência de problemas em todas as condições futuras.

O projeto permanece com sete tabelas Delta, dois notebooks principais e um único Lakeflow Spark Declarative Pipeline.