# iFood Data Architecture Case — single entrypoint.
# Quickstart for the evaluator:   make demo
SHELL := /bin/bash
# --project-directory . => all relative paths in the compose file resolve from repo root.
DC   := docker compose --project-directory . -f infra/docker-compose.yml
EXEC := $(DC) exec -T spark
START ?= 2023-01
END   ?= 2023-05
# min RAM (GB) the Docker engine/VM must expose; Spark jobs OOM-kill (137) below this
MIN_MEM_GB ?= 7

# Appended after each heavy job: turn a bare SIGKILL (137) into a clear out-of-memory message,
# so an evaluator never mistakes an out-of-RAM machine for a bug in the pipeline.
OOM_HINT = rc=$$?; if [ $$rc -eq 137 ]; then \
	  printf '\n\033[31m❌ A job was OOM-killed (exit 137 = SIGKILL): the machine ran out of free RAM.\033[0m\n'; \
	  printf '   This is an environment limit, NOT a bug in the pipeline. To fix, do one of:\n'; \
	  printf '     • close memory-heavy apps and re-run \140make demo\140, or\n'; \
	  printf '     • give the Docker VM more memory: colima stop && colima start --cpu 4 --memory 12\n'; \
	  printf '       (Docker Desktop: Settings > Resources > Memory >= 12 GB)\n\n'; \
	fi; exit $$rc

.DEFAULT_GOAL := help
.PHONY: help check-docker check-mem up down clean logs demo all ingest pipeline bronze silver gold \
        analyze eda report cluster-up cluster-run test test-docker lint fmt

help: ## show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

check-docker: ## ensure a Docker engine is running (auto-starts Colima / Docker Desktop if down)
	@if docker info >/dev/null 2>&1; then exit 0; fi; \
	if [ "$$SKIP_DOCKER_AUTOSTART" = "1" ]; then \
	  echo "❌ Docker engine down and auto-start disabled — start Docker Desktop or: colima start --cpu 4 --memory 8"; exit 1; \
	fi; \
	echo "⏳ Docker engine not running — starting it (set SKIP_DOCKER_AUTOSTART=1 to disable)..."; \
	if command -v colima >/dev/null 2>&1; then colima start --cpu 4 --memory 8; \
	elif [ -d /Applications/Docker.app ]; then open -a Docker; \
	else echo "❌ No Docker engine found. Install Docker Desktop or Colima."; exit 1; fi; \
	printf "waiting for Docker engine"; \
	for i in $$(seq 1 60); do docker info >/dev/null 2>&1 && { echo " ✓ ready"; exit 0; }; printf "."; sleep 2; done; \
	echo " ✗ timed out (try: colima start --cpu 4 --memory 8)"; exit 1

check-mem: ## fail fast if the Docker engine exposes < MIN_MEM_GB of RAM (avoids mid-run OOM kill 137)
	@mem=$$(docker info --format '{{.MemTotal}}' 2>/dev/null); \
	case "$$mem" in ''|*[!0-9]*) mem=0;; esac; gb=$$(( mem / 1024 / 1024 / 1024 )); \
	if [ "$(MIN_MEM_GB)" -gt 0 ] && [ "$$gb" -lt "$(MIN_MEM_GB)" ]; then \
	  echo "❌ Docker engine exposes only $${gb} GB RAM — the Spark jobs need ≥ $(MIN_MEM_GB) GB or they get OOM-killed (exit 137)."; \
	  echo "   Colima:         colima stop && colima start --cpu 4 --memory 8"; \
	  echo "   Docker Desktop: Settings → Resources → Memory ≥ 8 GB"; \
	  echo "   (bypass with: make demo MIN_MEM_GB=0)"; exit 1; \
	fi; \
	echo "✓ Docker engine RAM: $${gb} GB (need ≥ $(MIN_MEM_GB))"

up: check-docker check-mem ## build + start the core stack (minio + spark), wait for health
	@$(DC) up -d --build --wait || { \
	  echo ""; \
	  echo "⚠️  Build/up failed — often a transient BuildKit snapshot glitch on the first run"; \
	  echo "    (\"parent snapshot ... does not exist\"). Retrying once..."; \
	  echo ""; \
	  $(DC) up -d --build --wait; }

down: ## stop the stack
	$(DC) --profile cluster --profile test down

clean: ## stop + remove volumes (full reset)
	$(DC) --profile cluster --profile test down -v

logs: ## tail logs
	$(DC) logs -f

## ----- the one command for the evaluator -----
demo: up ## ingest Jan–May/2023, run the pipeline, print Q1/Q2 (DuckDB SQL over Gold)
	$(MAKE) ingest START=2023-01 END=2023-05
	$(MAKE) pipeline
	$(MAKE) analyze
	@echo "✅ Demo done.  MinIO console: http://localhost:9001 (minio/minio123)  ·  Spark UI on :4040 during jobs"

all: up ## full run for Jan–May 2023 (START/END overridable)
	$(MAKE) ingest START=$(START) END=$(END)
	$(MAKE) pipeline
	$(MAKE) analyze

ingest: ## download parquet -> MinIO landing  (START=YYYY-MM END=YYYY-MM)
	$(EXEC) python3 -m src.ingestion.downloader --start $(START) --end $(END)

pipeline: bronze silver gold ## bronze -> silver -> gold

bronze: ## landing -> bronze (Delta)
	@$(EXEC) spark-submit /opt/app/src/jobs/bronze.py; $(OOM_HINT)
silver: ## bronze -> silver (clean + DQ + quarantine)
	@$(EXEC) spark-submit /opt/app/src/jobs/silver.py; $(OOM_HINT)
gold: ## silver -> gold (fact + Q1/Q2 aggregates)
	@$(EXEC) spark-submit /opt/app/src/jobs/gold.py; $(OOM_HINT)

analyze: ## print Q1/Q2 via DuckDB SQL over the Gold Delta tables (lightweight consumption)
	@$(EXEC) python3 -m analysis.run_questions; $(OOM_HINT)

report: ## build analysis/report.html (Q1/Q2 charts for slides) from the Gold marts
	@$(EXEC) python3 -m analysis.build_report; $(OOM_HINT)

eda: ## exploratory analysis over Silver + DQ results
	@$(EXEC) spark-submit /opt/app/analysis/eda.py; $(OOM_HINT)

## ----- optional: real Spark standalone cluster (proof of distributed execution) -----
CLUSTER_EXEC := $(DC) exec -T spark-master
cluster-up: check-docker ## start a real Spark standalone cluster (master + worker)
	$(DC) --profile cluster up -d --build --wait
cluster-run: cluster-up ## run the full pipeline on the standalone cluster (START/END overridable)
	$(CLUSTER_EXEC) python3 -m src.ingestion.downloader --start $(START) --end $(END)
	$(CLUSTER_EXEC) spark-submit /opt/app/src/jobs/bronze.py
	$(CLUSTER_EXEC) spark-submit /opt/app/src/jobs/silver.py
	$(CLUSTER_EXEC) spark-submit /opt/app/src/jobs/gold.py
	$(CLUSTER_EXEC) python3 -m analysis.run_questions

## ----- local dev (no Docker needed) -----
test: ## unit tests on a local SparkSession (needs local Python+Java)
	pytest tests/ -m "not integration"

test-docker: check-docker ## run the FULL suite (incl. Spark) in a container — ONLY Docker required
	$(DC) build spark
	$(DC) --profile test run --rm --build test
lint: ## ruff + black --check + mypy
	ruff check src tests analysis && \
	black --check src tests analysis && mypy src
fmt: ## auto-format
	ruff check --fix src tests analysis && \
	black src tests analysis
