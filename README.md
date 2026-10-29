# Amazon Reviews — Big Data Analytics

**Engenharia de Dados | Apache Spark | Databricks | Delta Lake | Lakeflow | AI/BI Dashboard | Genie**

Projeto de Engenharia de Dados desenvolvido no **Databricks Free Edition**, com o objetivo de construir uma solução analítica de ponta a ponta para avaliações de produtos da Amazon, utilizando a arquitetura Medallion (Bronze, Silver e Gold).

A solução abrange ingestão de arquivos Parquet, transformações com Apache Spark, qualidade de dados, agregações analíticas, visualização de indicadores e consultas em linguagem natural com Databricks Genie.

## 1. Visão geral

O projeto utiliza um subconjunto do dataset público **Amazon Reviews 2023**, disponibilizado pelo McAuley Lab, especificamente a categoria `All_Beauty`.

O objetivo é transformar dados brutos de produtos e avaliações em informações organizadas e confiáveis para responder perguntas como:

- Quantos produtos possuem avaliações?
- Quais produtos concentram mais avaliações?
- Como as notas se distribuem ao longo do tempo?
- Qual é a proporção de avaliações com compra verificada?
- Quantos votos úteis as avaliações receberam?
- Como os indicadores variam entre categorias e meses?

O projeto prioriza qualidade, consistência, processamento eficiente e documentação técnica.

## 2. Tecnologias utilizadas

| Tecnologia | Aplicação |
|---|---|
| Databricks Free Edition | Plataforma de desenvolvimento e processamento |
| Apache Spark / PySpark | Transformação e análise distribuída |
| Spark SQL | Consultas, agregações e testes |
| Delta Lake | Armazenamento das tabelas analíticas |
| Unity Catalog | Organização dos dados em catálogo e schema |
| Lakeflow Spark Declarative Pipelines | Definição e atualização do pipeline |
| Serverless | Execução sem gerenciamento manual de clusters |
| Databricks AI/BI Dashboard | Visualização de indicadores |
| Databricks Genie | Consultas aos dados em linguagem natural |
| GitHub | Versionamento e apresentação do projeto |

## 3. Arquitetura de dados

O projeto segue a arquitetura Medallion, com três camadas.

```text
Amazon Reviews 2023 — All_Beauty
             |
             v
       Arquivos Parquet
       Reviews / Products
             |
             v
          BRONZE
    bronze_reviews_raw
    bronze_products_raw
             |
             v
          SILVER
    silver_reviews_clean
    silver_products_clean
             |
             v
           GOLD
    gold_product_metrics
    gold_monthly_review_metrics
    gold_genie_product_analytics
             |
       +-----+-----+
       |           |
       v           v
   Dashboard     Genie
```

### Bronze — Ingestão

Responsável pela ingestão dos arquivos Parquet para duas Streaming Tables:

- `bronze_reviews_raw`: avaliações brutas.
- `bronze_products_raw`: metadados brutos dos produtos.

A camada preserva os registros recebidos para permitir auditoria e processamento posterior.

### Silver — Tratamento

Responsável por estruturar os dados utilizados nas análises:

- `silver_reviews_clean`: avaliações tratadas, com remoção de cópias exatas.
- `silver_products_clean`: metadados de produtos tratados.

As transformações incluem validações de qualidade, tratamento de campos e preparação para agregações.

### Gold — Dados analíticos

Responsável por fornecer informações consolidadas:

- `gold_product_metrics`: métricas agregadas por produto com avaliações.
- `gold_monthly_review_metrics`: indicadores mensais de avaliações.
- `gold_genie_product_analytics`: visão analítica por produto para consultas no Genie.

As tabelas Silver e Gold são materialized views gerenciadas pelo Lakeflow Pipeline.

### Orquestração

O processamento utiliza um único Lakeflow Spark Declarative Pipeline:

- Nome: `amazon_reviews_pipeline`.
- Tipo de execução: Triggered.
- Compute: Serverless.
- Destino: `workspace.amazon_reviews`.

O código está organizado em três arquivos: `bronze.py`, `silver.py` e `gold.py`.

## 4. Fonte de dados

**Dataset:** Amazon Reviews 2023 — McAuley Lab.

**Categoria utilizada:** `All_Beauty`.

**Fonte:** https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023

Os dados utilizados incluem:

- Avaliações de produtos: notas, textos, identificadores, datas, votos úteis e indicação de compra verificada.
- Metadados de produtos: títulos, categorias, avaliações agregadas, preços e outros atributos.

O projeto utiliza arquivos Parquet disponibilizados pela fonte original.

Os arquivos brutos não são incluídos neste repositório. Para reproduzir o projeto, é necessário obtê-los na fonte e disponibilizá-los no ambiente Databricks.

## 5. Principais resultados

### Volumetria dos dados

| Indicador | Resultado |
|---|---:|
| Avaliações na Bronze | 701.528 |
| Avaliações na Silver | 694.253 |
| Cópias exatas excedentes removidas | 7.275 |
| Produtos cadastrados | 112.590 |
| Produtos com avaliações | 112.565 |
| Produtos sem avaliações | 25 |
| Avaliações com compra verificada | 628.456 |
| Total de votos úteis | 641.610 |
| Meses com avaliações registrados na Gold | 265 |

Os resultados correspondem ao conjunto `All_Beauty` processado no projeto.

### Deduplicação

Foram identificadas 7.275 cópias excedentes de avaliações, correspondentes a aproximadamente 1,04% dos registros da Bronze.

A comparação bidirecional entre a Bronze deduplicada e a Silver não identificou divergências nos dez campos originais avaliados.

### Integridade entre camadas

Os testes de reconciliação verificaram:

- Correspondência entre as avaliações Silver e os totais Gold.
- Unicidade de `parent_asin` na Gold do Genie.
- Consistência das contagens de avaliações por produto.
- Consistência dos votos úteis e compras verificadas.
- Consistência das médias, percentuais e métricas mensais.

Nenhuma divergência foi identificada nos critérios avaliados, respeitando as tolerâncias documentadas para indicadores arredondados.

## 6. Dashboard analítico

Foi desenvolvido um **Databricks AI/BI Dashboard** para apresentar indicadores e tendências das avaliações.

A solução permite visualizar os principais resultados do conjunto processado, incluindo indicadores agregados e distribuições relevantes para análise.

As evidências visuais do Dashboard poderão ser adicionadas ao repositório durante a etapa de publicação.

O Dashboard é um recurso do Databricks e não é recriado automaticamente apenas com os arquivos Python.

## 7. Consultas com Databricks Genie

O projeto inclui um Genie Space conectado à tabela:

`workspace.amazon_reviews.gold_genie_product_analytics`

O objetivo é permitir perguntas em linguagem natural sobre avaliações e produtos, utilizando uma tabela Gold organizada e documentada.

O Genie foi configurado com instruções e exemplos de consultas SQL, e perguntas de negócio foram validadas.

**Exemplos de perguntas:**

- Quais são os produtos com mais avaliações?
- Quais produtos possuem as maiores notas médias entre aqueles com avaliações?
- Qual é o percentual de avaliações verificadas por produto?
- Quais produtos receberam mais votos úteis?

O Genie está limitado aos dados e indicadores disponíveis na tabela conectada.

## 8. Performance e otimização

Os planos de execução foram analisados com `explain()` e as consultas foram inspecionadas por meio do Query Profile.

### Resultados observados

| Operação | Duração | Volume lido |
|---|---:|---:|
| Contagem filtrada de produtos | 6 segundos | 325 KB |
| Agregação por categoria | 1 segundo | 326 KB |
| Refresh padrão do Lakeflow | 2 minutos e 4 segundos | Não registrado |

Os testes identificaram:

- Leitura apenas das colunas necessárias.
- Encaminhamento de filtros à leitura.
- Operadores Photon nas consultas analisadas.
- Utilização de agregações otimizadas pelo Spark.

Essas medições representam execuções específicas, não benchmarks controlados.

Não foi comprovada a ausência de shuffle em todas as execuções, nem identificado um gargalo que justificasse mudanças adicionais de configuração.

## 9. Testes de qualidade e idempotência

Os testes foram implementados no notebook `02_spark_performance`.

### Qualidade

Foram validadas contagens, duplicatas, integridade entre camadas, chaves, métricas por produto e métricas mensais.

### Idempotência

Foi registrada uma baseline das sete tabelas e executado um Refresh padrão do Lakeflow, com arquivos de origem e definições do pipeline inalterados.

Após o Refresh:

- As sete tabelas preservaram suas contagens.
- Os sete principais indicadores de negócio permaneceram iguais.
- As duas tabelas Gold avaliadas continuaram consistentes.

**Conclusão:** foi demonstrada idempotência quantitativa e preservação dos indicadores avaliados nas condições testadas.

Não foi comprovada igualdade histórica completa do conteúdo de todas as tabelas. Também não foram testados todos os cenários de falha, novos arquivos ou Full refresh.

## 10. Organização do código

O projeto contém dois notebooks principais:

- `01_data_exploration`: exploração inicial, schemas, análise de qualidade e características da fonte.
- `02_spark_performance`: investigação de performance, reconciliação, integridade e idempotência.

A definição do pipeline está distribuída nos arquivos Python:

- `bronze.py`: ingestão dos dados.
- `silver.py`: tratamento e limpeza.
- `gold.py`: construção das tabelas analíticas.

A documentação técnica está organizada em:

- [Arquitetura](docs/architecture.md)
- [Dicionário de dados](docs/data_dictionary.md)
- [Performance e testes](docs/performance_and_testing.md)

No Workspace original do Databricks, os arquivos do pipeline permanecem separados dos notebooks e documentos. Neste repositório GitHub, o código está organizado nas pastas `pipelines/`, `notebooks/` e `docs/`.

## 11. Como reproduzir o projeto

### Pré-requisitos

- Acesso a um ambiente Databricks com suporte aos recursos utilizados.
- Permissão para criar schemas, volumes, tabelas Delta e Lakeflow Pipelines.
- Dados da categoria `All_Beauty` do Amazon Reviews 2023.

### Preparação

**1. Criar o catálogo e schema de destino conforme as permissões do ambiente.**

O ambiente original utiliza:

```text
workspace.amazon_reviews
```

**2. Disponibilizar os arquivos Parquet em um Unity Catalog Volume.**

Caminhos utilizados no projeto original:

```text
/Volumes/workspace/amazon_reviews/raw_files/reviews/
/Volumes/workspace/amazon_reviews/raw_files/products/
```

**3. Ajustar os caminhos e referências de catálogo, caso o ambiente seja diferente.**

Os caminhos do Workspace e do Unity Catalog são específicos da configuração original.

**4. Disponibilizar os arquivos Python do pipeline e configurar um Lakeflow Spark Declarative Pipeline.**

O pipeline deve utilizar os arquivos `bronze.py`, `silver.py` e `gold.py`, com destino ao schema adequado.

**5. Executar a atualização do pipeline e validar as tabelas.**

O ambiente precisa disponibilizar os recursos utilizados pelo projeto.

**6. Executar os notebooks para exploração e testes.**

As consultas devem ser adaptadas se o catálogo ou schema tiver nomes diferentes.

**7. Configurar o Dashboard e o Genie no Databricks.**

Esses recursos dependem de configuração específica na plataforma e não são automaticamente recriados apenas pelo código das tabelas.

## 12. Limitações e considerações

- O processamento foi realizado inicialmente apenas na categoria `All_Beauty`, e não no dataset Amazon Reviews completo.
- O conjunto contém avaliações, não transações comerciais. Portanto, quantidade de avaliações não representa quantidade de vendas.
- Votos úteis não representam necessariamente pessoas únicas.
- A contagem de avaliações nos metadados de origem pode diferir da quantidade de registros disponível no conjunto processado.
- Não foi confirmada uma moeda uniforme para todos os preços tratados; valores monetários não devem ser utilizados para estimar faturamento.
- Existem produtos sem informações completas de preço.
- A série mensal possui cobertura temporal desigual, incluindo meses parciais.
- Os tempos das consultas são observações específicas, não garantias de performance.
- A idempotência comprovada está limitada às contagens e métricas avaliadas após o Refresh.
- O projeto foi desenvolvido para fins educacionais e de portfólio, não como uma solução produtiva com SLA.

## 13. Documentação adicional

Consulte os documentos disponíveis na pasta `docs/`:

- **architecture.md:** arquitetura, componentes e fluxo dos dados.
- **data_dictionary.md:** descrição das tabelas e campos.
- **performance_and_testing.md:** metodologia, resultados e limitações dos testes.

## 14. Referência e créditos

Os dados utilizados pertencem ao conjunto público **Amazon Reviews 2023**, disponibilizado pelo **McAuley Lab**.

Fonte: https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023

Este projeto é uma implementação educacional independente e não representa uma solução oficial da Amazon, do McAuley Lab ou da Databricks.


## 15. Evidências visuais do ambiente Databricks

Na pasta "images" encontra-se evidências visuais do projeto dentro do ambiente do Databricks.