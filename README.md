<p align="center">
  <img width="600" height="600" alt="iFood Data Architecture Case" src="https://github.com/user-attachments/assets/0414dcbd-ac2a-4553-8308-98c18e34bc26" />
</p>

# iFood — Case de Arquitetura de Dados (Lakehouse NYC Taxi)

Lakehouse **totalmente local e reprodutível** que ingere os dados de corridas de táxi amarelo da
NYC TLC (Jan–Mai/2023), modela-os através de uma arquitetura **medallion** (bronze → silver →
gold) sobre **Delta Lake**, expõe-nos via **SQL (DuckDB)** e responde às duas perguntas de negócio.

Entrega as três frentes do case de ponta a ponta — **(1) ingestão** no lake, **(2) consumo via
SQL** e **(3) as duas análises** — com as transformações bronze/silver/gold escritas em
**PySpark** (a etapa de processamento exigida).

> **Por que não Databricks Community Edition?** O case permite "qualquer tecnologia de sua
> escolha" e avalia *justificativa técnica* e *criatividade*. Esta solução roda a plataforma
> inteira com um único comando, sem conta na nuvem, com uma matriz de versões fixada e validada
> de forma adversarial — então o avaliador a reproduz exatamente.

---

## Como rodar — `make demo`

Um único comando faz tudo, do build à resposta das duas perguntas:

```bash
make demo
```

Ele encadeia **4 fases**. Cada uma é também um target isolado (útil para depurar), mas o `demo`
roda todas em sequência:

| # | Fase (`make`) | O que faz | Resultado observável |
|---|---|---|---|
| 1 | `up`       | Build + sobe MinIO & Spark e aguarda o *health*; preflight de Docker e de RAM | containers *healthy* |
| 2 | `ingest`   | Baixa os parquet da TLC (Jan–Mai/2023) → `landing/` no MinIO (idempotente, com manifesto) | ~16M corridas no lake |
| 3 | `pipeline` | `bronze` → `silver` (limpeza + Data Quality + quarentena) → `gold` (fato + marts Q1/Q2) | tabelas Delta do medallion |
| 4 | `analyze`  | SQL via DuckDB sobre o Gold e imprime as respostas Q1 & Q2 | tabelas no terminal |

**Ao final você verá:**

```
✅ Demo done.  MinIO console: http://localhost:9001 (minio/minio123)  ·  Spark UI on :4040 during jobs
```

- As respostas **Q1 e Q2** são impressas no terminal pela fase `analyze` (ver [As duas perguntas](#as-duas-perguntas--respostas)).
- **Console do MinIO:** <http://localhost:9001> (`minio` / `minio123`) — para inspecionar o lake (`landing/`, `bronze/`, `silver/`, `gold/`).
- **Spark UI:** <http://localhost:4040> — disponível enquanto um job está rodando.

Outros comandos úteis:

```bash
make all       # igual ao demo, mas com janela configurável (START=YYYY-MM END=YYYY-MM)
make test      # testes unitários em uma SparkSession local (sem Docker)
make help      # lista todos os targets disponíveis
```

### Antes de rodar

**Pré-requisitos (no host):** todo o resto (Java, Spark, Python, jars) vive nas imagens, então o
host só precisa de:

- **`make`** — macOS: `xcode-select --install` (Command Line Tools); Debian/Ubuntu: `apt install make`.
- **Um engine Docker** — Docker Desktop **ou** Colima (`brew install colima`). Você não precisa
  iniciá-lo: o `make` roda um passo `check-docker` que **auto-inicia** o Colima
  (`--cpu 4 --memory 8`) ou o Docker Desktop se o daemon estiver parado (desabilite com
  `SKIP_DOCKER_AUTOSTART=1`).
- **~8 GB de RAM livre** para a VM e **rede de saída** (dados da TLC + dependências do primeiro
  build). Funciona em Apple Silicon **e** Intel (imagens multi-arch, sem emulação).
- Colima já rodando? Garanta ≥ 8 GB — o `make` não redimensiona uma VM ativa:
  `colima stop && colima start --cpu 4 --memory 8`. (O `make` roda um preflight `check-mem` que
  **falha cedo** com essa instrução se o engine expuser < 7 GB — evitando OOM no meio do pipeline;
  contorne com `MIN_MEM_GB=0`.)

**Tempo de execução:** o **primeiro** `make demo` faz o build da imagem Spark (pull único da
imagem base de ~2 GB + jars + deps) — tipicamente **~10–15 min, dominado por esse pull**. Tudo é
então cacheado, então **execuções seguintes levam ~6 min** — o demo ingere a janela completa
Jan–Mai/2023 (~16M corridas) e roda o pipeline bronze→silver→gold inteiro em `local[*]`. O
primeiro start do Colima também baixa uma pequena imagem de VM uma vez.

---

## Arquitetura

![Arquitetura local: o Makefile orquestra um trigger → ingestão em Python (captura de dados brutos) → jobs Spark (limpeza → transformação) com um portão de Data Quality sobre um medallion MinIO/Delta (landing → bronze → silver → gold), respondido via SQL no DuckDB; EDA sobre o Silver.](docs/img/local-architecture.png)

```
TLC CDN ──(downloader: faixa de datas, idempotente, manifesto)──▶ MinIO  s3a://datalake/
  landing/ (parquet bruto, imutável)
     └▶ bronze/  (Delta: tipos conformados + auditoria, particionado por source year/month)
          └▶ silver/ (Delta: limpo, dedup, DQ + quarentena, dimensões de tempo)
               ├▶ silver_rejected/ (+ _reject_reason)   └▶ dq.check_results (métricas)
               └▶ gold/ (fact_yellow_trips [pronto p/ ML] + mart Q1 + mart Q2)
                    └▶ DuckDB SQL → make analyze    (consumo — leve)

Processamento: Spark local[*] (container único).  Profile opcional: `cluster` (Spark standalone real).
Alvo de produção: AWS serverless (EMR Serverless + Athena) — docs/aws-reference-architecture.md.
Qualidade: pytest + ruff/black/mypy + CI.
```

---

## As duas perguntas — respostas

Calculadas sobre os arquivos oficiais da TLC (Jan–Mai/2023); tabelas completas em
[`docs/RESULTS.md`](docs/RESULTS.md).

**Q1 — média de `total_amount` por mês** (amarelo) → `gold.agg_monthly_total_amount`

| 2023-01 | 2023-02 | 2023-03 | 2023-04 | 2023-05 |
|---:|---:|---:|---:|---:|
| 27.45 | 27.34 | 28.27 | 28.76 | 29.46 |

**Q2 — média de `passenger_count` por hora de embarque** (Maio, `passenger_count > 0`) →
`gold.agg_may_passengers_by_hour` — varia de **1.26** (06h, commute matinal) a **1.46** (02h,
madrugada); tabela das 24 horas em [`docs/RESULTS.md`](docs/RESULTS.md).

`make analyze` imprime ambas; o SQL está em [`analysis/sql/`](analysis/sql). As 5 colunas de
consumo exigidas (`VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`,
`tpep_dropoff_datetime`) são garantidas no Silver/Gold. A política de limpeza está detalhada em
[Análise exploratória → decisões de limpeza](#análise-exploratória--decisões-de-limpeza); o
escopo da Q2 **"todos os táxis" → amarelo** (as colunas `tpep_*` são específicas do amarelo) é
uma premissa deliberada e documentada.

---

## Análise exploratória → decisões de limpeza

`make eda` perfila a camada Silver e a quarentena — o ciclo por trás de cada regra de limpeza é
**explorar → encontrar → decidir**, não "limpei os dados". Todos os números abaixo são a saída
real do `make eda` sobre a carga Jan–Mai/2023 (16.186.383 linhas brutas no Bronze).

**Como os dados se apresentam (profiling):**

- **Volume/mês** (Silver limpo): 3,04M · 2,89M · 3,37M · 3,26M · 3,48M = **16.041.339** corridas.
- **`total_amount`**: mín **0,01**, mediana **20,70**, média **28,31**, máx **6.304,90** —
  assimétrico à direita; outliers são **mantidos** (só descartamos valores não-positivos, ver abaixo).
- **`passenger_count`**: **73,5%** carregam 1 passageiro; **701.006 (4,4%)** são 0/NULL.

**Achados → regras (o ciclo):**

| Sonda da EDA | O que encontrou (real) | Regra → severidade |
|---|---|---|
| faixa do timestamp de embarque | **104** embarques fora de Jan–Mai/2023 (2008/09 perdidos, vazamento de mês) | `pickup_in_window` → QUARENTENA |
| distribuição de `total_amount` | **144.146** corridas ≤ \$0 (reembolsos/estornos/tarifa zero) | `amount_positive` → QUARENTENA |
| ordem embarque vs desembarque | **795** corridas com desembarque < embarque (impossível) | `dropoff_after_pickup` → QUARENTENA |
| chaves obrigatórias | **0** nulos em VendorID/embarque/desembarque/valor | `*_not_null` → QUARENTENA (guardas) |
| distribuição de `passenger_count` | **702.146** corridas com 0/NULL passageiros (4,3%) | `passenger_positive` → **WARN** |

**Como as linhas que falham são tratadas** (`src/common/dq.py`):

- **QUARENTENA** → a linha é roteada para `silver_yellow_trips_rejected` com um `_reject_reason`
  (auditada, *não* descartada silenciosamente) e excluída do Gold.
- **WARN** → a linha é **mantida**; apenas registrada como métrica. `passenger_count` é WARN
  porque corridas com 0 passageiros são receita real (válidas para Q1) e só erradas para Q2 —
  então mantemos e filtramos `passenger_count > 0` **na query da Q2**, não globalmente.
- **BLOCK** → interromperia o pipeline (nenhuma configurada hoje).
- Cada check é persistido por execução em `dq.check_results` (Delta, chaveado por `run_id`).

**Os números reconciliam (auditável):** total em quarentena = 144.146 + 795 + 104 − **1** de
sobreposição = **145.044**; Silver = 16.186.383 − 145.044 = **16.041.339**, que é igual à soma
mensal acima. ~0,9% em quarentena, 4,3% sinalizados-mas-mantidos — longe de um filtro cego.

Reproduza: `make eda`.

---

## Matriz de versões validada

Spark **4.0.1** (Scala 2.13, Java 17) · Delta **4.0.0** · hadoop-aws **3.4.1** + AWS SDK v2
**2.24.6** · MinIO · DuckDB · Python 3.10 (imagem base do Spark). O Spark é fixado em **4.0.x, não
4.1**, porque só a 4.0.x tem um match verificado de `hadoop-aws` — o principal quebra-pilha do
s3a. Os jars **e as extensões do DuckDB** são **embutidos na imagem** (seguro offline).

O caminho padrão é apenas **MinIO + um container Spark `local[*]` + DuckDB**; o cluster Spark
standalone é um **profile opcional** (então o padrão fica em ~4 GB).

---

## Estrutura do repositório

```
src/ingestion/   downloader: parquet da TLC -> landing no MinIO (idempotente + manifesto)
src/jobs/        transforms PySpark: bronze.py, silver.py, gold.py (funções puras + run() fino)
src/common/      SparkSession (s3a+Delta), paths do lake, cliente S3, engine de data quality
src/config.py    settings via env (fonte única da verdade)
analysis/        run_questions.py (SQL DuckDB), build_report.py (charts), eda.py, sql/
infra/           docker-compose, imagem Spark (jars embutidos), imagem de teste containerizada
tests/           pytest (Python puro + oráculo DuckDB + PySpark) + fixture determinística
docs/            aws-reference-architecture.md (+ diagrama), RESULTS.md
```

---

## Testes

`make test` roda os testes unitários em uma SparkSession local. A suíte tem três camadas:

- **Python puro** (`test_io_config`, `test_downloader`) — paths, config, ingestão idempotente.
- **Oráculo DuckDB** (`test_oracle`) — recomputa a limpeza do Silver + Q1/Q2 numa fixture
  determinística e afirma os mesmos números calculados à mão que o pipeline Spark deve produzir
  (uma verificação cruzada da lógica analítica, sem JVM).
- **PySpark** (`test_dq`, `test_bronze`, `test_silver`, `test_gold`) — as transforms reais;
  Q1/Q2 são verificadas contra as expectativas compartilhadas em `tests/expected.py`.

**Zero setup local:** `make test-docker` roda a suíte **completa** (incl. PySpark, contra os jars
Delta embutidos — sem rede) dentro de um container; o host só precisa de Docker. `make test` é a
mesma suíte usando um toolchain Python+Java local.

A CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) roda dois jobs:

- **`test`** (todo push/PR) — ruff + black + mypy e a suíte unitária completa (incl. PySpark) em
  Python 3.10 + Java 17, espelhando o runtime do container Spark.
- **`smoke-docker`** (PRs + pushes para `main`) — faz o build da imagem Spark **real** e roda a
  suíte **dentro dela**, pegando problemas de runtime que o job de lint/unit não pega (ex.:
  `spark-submit` no PATH, jars Delta embutidos, versão do Python do container).

---

## Opcional: cluster Spark standalone real (prova de escala)

```bash
make cluster-up    # spark-master + spark-worker (Spark distribuído real)
make cluster-run   # roda o pipeline completo no cluster
```

---

## Produção: arquitetura de referência na AWS

O mesmo código mapeia para um **lakehouse serverless na AWS** (S3 + EMR Serverless + Glue Data
Catalog + Athena + MWAA) **apenas por configuração** — ver
[`docs/aws-reference-architecture.md`](docs/aws-reference-architecture.md).

![Arquitetura de referência AWS: Airflow/MWAA orquestra um trigger EventBridge/cron → ingestão em Lambda (período como parâmetro) → jobs Spark no EMR Serverless sobre um medallion no S3 (landing → bronze → silver → gold) → Glue Data Catalog → Athena, com um portão de data quality que alerta (Teams/Slack) em falha; Terraform provisiona tudo.](docs/img/aws-architecture.png)

---

## Targets do Make

`make help` lista tudo: `demo`, `all`, `ingest`, `pipeline` (`bronze`/`silver`/`gold`),
`analyze`, `eda`, `report`, `cluster-up`/`cluster-run`, `test`/`test-docker`, `lint`, `clean`.

---

## Mapeamento para os critérios de avaliação do case

| Critério | Onde é endereçado |
|---|---|
| **Qualidade & organização do código** | `src/` como funções puras + `run()` fino; suíte de testes em 3 camadas; ruff + black + mypy e uma [CI](.github/workflows/ci.yml) de 2 jobs (lint/unit + smoke test no container). |
| **Análise exploratória** | [Análise exploratória → decisões de limpeza](#análise-exploratória--decisões-de-limpeza) — números reais do `make eda` guiando cada regra de limpeza. |
| **Justificativa técnica** | A racional "Por que não Databricks CE", a matriz de versões validada e as decisões de design percorridas na apresentação. |
| **Criatividade** | Um lakehouse de um comando, totalmente local e reprodutível offline (jars/extensões embutidos), DuckDB-sobre-Delta como consumo SQL sem infra, e a troca para AWS serverless apenas por config. |
| **Clareza dos resultados** | Tabelas de respostas Q1/Q2 + [`docs/RESULTS.md`](docs/RESULTS.md), o diagrama de arquitetura AWS e charts prontos para slides via `make report`. |

---

## Notas & limitações

- **Spark single-node:** o Spark local é `local[*]` — dimensionado para ~16M linhas. Escala =
  AWS serverless (EMR Serverless + Athena), uma troca apenas por config
  ([docs/aws-reference-architecture.md](docs/aws-reference-architecture.md)).
- **Memória (OOM, exit 137):** os jobs de ~16M linhas querem uma VM Docker com ≥ 8 GB. O `make`
  faz preflight disso (`check-mem`); e se um job ainda for OOM-killed sob pressão de memória do
  host, a execução imprime uma mensagem clara *"out-of-memory — limite de ambiente, não bug do
  pipeline"* (exit 137) com o fix, em vez de um erro críptico.
- **Build do primeiro run (BuildKit):** o BuildKit pode, ocasionalmente, falhar o primeiro build
  da imagem com um erro transiente de snapshot (`parent snapshot ... does not exist`) — uma
  esquisitice do Docker, não um bug do projeto. O `make` refaz o build uma vez automaticamente;
  rodar `make demo` de novo também resolve.
- **Credenciais do demo** (`minio`/`minio123`) são apenas para uso local.
