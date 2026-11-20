# 🚀 10 Projetos Práticos de Engenharia de Dados

> *"Não aprenda apenas ferramentas isoladas. Conecte-as. Movimente dados através delas. Prove que você sabe construir pipelines reais."*

Este repositório documenta uma trilha prática composta por **10 arquiteturas essenciais de Engenharia de Dados**. O objetivo é sair do estudo teórico de ferramentas individuais e avançar para a construção de sistemas completos, resilientes e escaláveis de ponta a ponta (*end-to-end*).

---

## 📋 Sumário

1. [Data Lake na AWS](#1-data-lake)
2. [Arquitetura Lakehouse](#2-lakehouse)
3. [Pipeline ELT Moderno](#3-elt-pipeline)
4. [Streaming em Tempo Real](#4-real-time-streaming)
5. [Arquitetura Medalhão (Medallion)](#5-medallion-architecture)
6. [Orquestração de Workflows](#6-workflow-orchestration)
7. [Pipeline de CDC (Change Data Capture)](#7-cdc-pipeline)
8. [Processamento de Big Data](#8-big-data-processing)
9. [Qualidade de Dados e Observabilidade](#9-data-quality--observability)
10. [Analytics End-to-End](#10-end-to-end-analytics)

---

## 1. Data Lake

Construção de um repositório centralizado e escalável na nuvem para armazenar dados brutos e estruturados, permitindo consultas SQL serverless diretamente sobre os arquivos.

### 🔄 Fluxo da Arquitetura
```text
[Dados Brutos] ➔ [Amazon S3] ➔ [AWS Glue Crawler] ➔ [Amazon Athena]
```

### 🛠️ Stack Tecnológica
* **Amazon S3:** Armazenamento de objetos altamente durável e de baixo custo para os dados brutos (CSV, JSON, Parquet).
* **AWS Glue Crawler:** Varre automaticamente os arquivos no S3, infere os esquemas (*schemas*) e popula o AWS Glue Data Catalog.
* **Amazon Athena:** Motor de consultas SQL interativo e *serverless* (baseado em Trino/Presto) para analisar os dados catalogados diretamente no S3.

### 🎯 O que você vai aprender e implementar
* Particionamento de dados e otimização de formatos de arquivo (conversão para Parquet/ORC).
* Catalogação automática de metadados e evolução de esquemas.
* Execução de consultas analíticas sem a necessidade de provisionar servidores.

---

## 2. Lakehouse

Implementação de uma arquitetura moderna que combina a flexibilidade e o baixo custo de um Data Lake com a confiabilidade, transações ACID e governança de um Data Warehouse.

### 🔄 Fluxo da Arquitetura
```text
[Dados] ➔ [Databricks] ➔ [Delta Lake] ➔ [Unity Catalog]
```

### 🛠️ Stack Tecnológica
* **Databricks:** Plataforma unificada de engenharia e ciência de dados para processamento distribuído.
* **Delta Lake:** Camada de armazenamento open-source que traz transações ACID, *time travel* (versionamento de dados) e controle de esquema.
* **Unity Catalog:** Solução centralizada de governança, controle de acesso granular (RBAC), auditoria e linhagem de dados (*data lineage*).

### 🎯 O que você vai aprender e implementar
* Criação de tabelas Delta com suporte a operações `MERGE`, `UPDATE` e `DELETE`.
* Uso de *Time Travel* para auditar ou reverter alterações nos dados.
* Configuração de políticas de segurança, permissões e rastreamento de linhagem de dados.

---

## 3. ELT Pipeline

Construção de um fluxo moderno de **Extração, Carga e Transformação (ELT)**, onde os dados são carregados primeiro no destino e transformados posteriormente usando o poder computacional do Data Warehouse.

### 🔄 Fluxo da Arquitetura
```text
[Fontes de Dados] ➔ [Airbyte] ➔ [Snowflake] ➔ [dbt]
```

### 🛠️ Stack Tecnológica
* **Airbyte:** Plataforma open-source de integração de dados responsável por extrair dados de APIs/bancos e carregá-los no destino.
* **Snowflake:** Cloud Data Warehouse escalável que separa armazenamento de computação.
* **dbt (Data Build Tool):** Ferramenta para transformar dados dentro do warehouse utilizando SQL modular, controle de versão e testes.

### 🎯 O que você vai aprender e implementar
* Configuração de conectores de ingestão incremental e *full-refresh*.
* Modelagem dimensional (Star Schema / Snowflake Schema) com modelos SQL no dbt.
* Documentação automática e testes de integridade referencial.

---

## 4. Real-Time Streaming

Processamento contínuo de eventos de baixa latência para cenários que exigem tomada de decisão imediata, como detecção de fraudes, monitoramento de IoT ou telemetria.

### 🔄 Fluxo da Arquitetura
```text
[Eventos] ➔ [Apache Kafka] ➔ [Spark Structured Streaming] ➔ [Delta Lake]
```

### 🛠️ Stack Tecnológica
* **Apache Kafka:** Broker de mensageria distribuído de alta vazão para ingestão e desacoplamento de fluxos de eventos.
* **Spark Structured Streaming:** Motor de processamento de streams tolerante a falhas que consome tópicos do Kafka em micro-batches ou processamento contínuo.
* **Delta Lake:** Destino transacional que permite leituras e escritas concorrentes em tempo real.

### 🎯 O que você vai aprender e implementar
* Produção e consumo de mensagens em tópicos Kafka com particionamento adequado.
* Tratamento de eventos atrasados (*watermarks*) e agregações em janelas de tempo (*windowing*).
* Garantia de processamento *exactly-once* e persistência em formato Delta.

---

## 5. Medallion Architecture

Estruturação lógica dos dados em camadas progressivas de qualidade e refinamento dentro de um ambiente Lakehouse.

### 🔄 Fluxo da Arquitetura
```text
[Dados Brutos] ➔ [Bronze (Raw)] ➔ [Silver (Cleansed)] ➔ [Gold (Aggregated)]
                  └──────────────────── Databricks ────────────────────┘
```

### 🛠️ Stack Tecnológica
* **Databricks (Apache Spark + Delta Lake):** Ambiente utilizado para orquestrar e processar as transformações entre as três camadas:
  * **Camada Bronze:** Ingestão fiel dos dados brutos, mantendo o histórico completo.
  * **Camada Silver:** Dados limpos, deduplicados, tipados e enriquecidos.
  * **Camada Gold:** Tabelas agregadas e modeladas para consumo direto por BI e Machine Learning.

### 🎯 O que você vai aprender e implementar
* Padrões de limpeza, padronização e deduplicação de registros.
* Isolamento de falhas (se uma regra de negócio mudar na camada Gold, não é preciso reextrair os dados da origem).
* Otimização de leitura para ferramentas de visualização na camada Gold.

---

## 6. Workflow Orchestration

Automação, agendamento e monitoramento de dependências complexas entre tarefas de ingestão e transformação de dados.

### 🔄 Fluxo da Arquitetura
```text
[Dados] ➔ [Apache Airflow] ➔ [dbt] ➔ [Snowflake]
```

### 🛠️ Stack Tecnológica
* **Apache Airflow:** Orquestrador baseado em código (Python) que gerencia DAGs (*Directed Acyclic Graphs*).
* **dbt:** Executado como uma ou mais tarefas dentro do Airflow para rodar as transformações analíticas.
* **Snowflake:** Motor de execução onde as consultas SQL disparadas pelo dbt são processadas.

### 🎯 O que você vai aprender e implementar
* Desenvolvimento de DAGs idempotentes e resilientes em Python.
* Configuração de retentativas automáticas (*retries*), alertas de falha e SLAs.
* Integração entre Airflow e dbt (via `BashOperator`, `Cosmos` ou `dbt Cloud`).

---

## 7. CDC Pipeline

Captura de alterações em tempo real diretamente dos logs de transações de bancos de dados relacionais, evitando consultas pesadas na origem.

### 🔄 Fluxo da Arquitetura
```text
[Source DB] ➔ [Debezium] ➔ [Apache Kafka] ➔ [Apache Iceberg]
```

### 🛠️ Stack Tecnológica
* **Source DB (ex: PostgreSQL / MySQL):** Banco de dados transacional de origem.
* **Debezium:** Plataforma distribuída de *Change Data Capture* (CDC) que lê os logs de transação (WAL/Binlog) e captura `INSERT`, `UPDATE` e `DELETE`.
* **Apache Kafka:** Transporta os eventos de alteração capturados pelo Debezium.
* **Apache Iceberg:** Formato de tabela aberto de alto desempenho para grandes volumes analíticos, suportando evolução de esquema e transações ACID.

### 🎯 O que você vai aprender e implementar
* Configuração de replicação lógica em bancos relacionais.
* Sincronização contínua de estado entre sistemas OLTP e OLAP sem impacto na performance do banco de produção.
* Manipulação de operações de `UPSERT` e exclusões lógicas/físicas em tabelas Iceberg.

---

## 8. Big Data Processing

Processamento distribuído em larga escala para grandes volumes de dados não estruturados ou semiestruturados utilizando clusters gerenciados.

### 🔄 Fluxo da Arquitetura
```text
[Dados Massivos] ➔ [Amazon EMR] ➔ [Apache Spark] ➔ [Amazon S3]
```

### 🛠️ Stack Tecnológica
* **Amazon EMR (Elastic MapReduce):** Serviço gerenciado na AWS para executar frameworks de Big Data em clusters escaláveis.
* **Apache Spark (PySpark / Scala):** Motor de processamento distribuído em memória para transformações complexas em terabytes de dados.
* **Amazon S3:** Camada de leitura e escrita desacoplada do armazenamento local do cluster (HDFS).

### 🎯 O que você vai aprender e implementar
* Dimensionamento de clusters (*autoscaling* e uso de instâncias Spot para redução de custos).
* Otimização de jobs Spark (gerenciamento de memória, *broadcast joins*, redução de *shuffle* e *data skew*).
* Escrita de resultados particionados e compactados no S3.

---

## 9. Data Quality & Observability

Implementação de barreiras de proteção (*data contracts* e validações) e alertas proativos para garantir que dados corrompidos nunca cheguem aos usuários finais.

### 🔄 Fluxo da Arquitetura
```text
[Dados] ➔ [Great Expectations] ➔ [dbt tests] ➔ [Slack Alerts]
```

### 🛠️ Stack Tecnológica
* **Great Expectations (GX):** Framework Python para validação, documentação e perfilamento de dados antes ou durante a ingestão.
* **dbt tests:** Testes nativos e customizados (ex: `unique`, `not_null`, `accepted_values`, `dbt-expectations`) aplicados após as transformações.
* **Slack Alerts:** Canal de comunicação integrado via Webhooks para notificar a equipe de engenharia imediatamente em caso de quebra de qualidade.

### 🎯 O que você vai aprender e implementar
* Definição de expectativas de qualidade (faixas de valores, formatos regex, completude).
* Implementação do padrão *Circuit Breaker* (interromper o pipeline caso dados críticos falhem na validação).
* Criação de alertas acionáveis com contexto detalhado do erro para rápida resolução.

---

## 10. End-to-End Analytics

Construção completa da *Modern Data Stack*, conectando múltiplas fontes de dados operacionais até a entrega de dashboards executivos para o negócio.

### 🔄 Fluxo da Arquitetura
```text
[Fontes de Dados] ➔ [Fivetran] ➔ [Google BigQuery] ➔ [dbt] ➔ [Looker]
```

### 🛠️ Stack Tecnológica
* **Fivetran:** Plataforma gerenciada de ingestão automatizada com conectores prontos e manutenção zero.
* **Google BigQuery:** Data Warehouse *serverless* altamente escalável da Google Cloud.
* **dbt:** Camada de transformação responsável por consolidar regras de negócio e criar tabelas analíticas.
* **Looker:** Plataforma de Business Intelligence (BI) com camada semântica (LookML) para exploração visual e métricas consistentes.

### 🎯 O que você vai aprender e implementar
* Integração de múltiplas fontes (CRM, ERP, Ads, Bancos de Dados) em um único repositório central.
* Governança de métricas de negócio através de modelagem no dbt e camada semântica no Looker.
* Entrega de valor tangível para stakeholders por meio de relatórios e dashboards interativos.

---

## 💡 Próximos Passos e Boas Práticas

Para destacar esses projetos no seu portfólio:
1. **Infraestrutura como Código (IaC):** Utilize **Terraform** ou **AWS CDK** para provisionar os recursos na nuvem de forma reprodutível.
2. **Containerização:** Utilize **Docker** e **Docker Compose** para rodar ferramentas como Airflow, Kafka, Debezium e Spark localmente.
3. **CI/CD:** Configure pipelines no **GitHub Actions** para rodar linters (SQLFluff, Flake8) e testes automatizados do dbt a cada Pull Request.
4. **Documentação Visual:** Adicione diagramas de arquitetura e prints dos resultados/dashboards no repositório de cada projeto.
