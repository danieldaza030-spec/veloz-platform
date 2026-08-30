# Custom Airflow image so DAG tasks can create a PySpark/Delta session in-process
# (LocalExecutor runs tasks inside the scheduler/webserver container — no separate
# Spark cluster). Versions below must track the local dev venv (see repo root
# .venv) so code behaves the same whether it's run by hand or by Airflow.
#
# python3.11 is required, not python3.12+: pyspark 3.5.3 only supports up to
# Python 3.12, and this tag keeps comfortable headroom.
#
# This image also backs the `jupyter` compose service (see docker-compose.yml)
# so notebooks under ./notebooks run against the same Python/Spark/Delta stack
# as every DAG, instead of the host .venv (which is on Python 3.14, outside
# pyspark 3.5.3's supported range) — that's a deliberate reuse, not scope
# creep: one image, two commands (airflow vs. `jupyter lab`), rather than a
# second image that could quietly drift out of version sync with this one.
FROM apache/airflow:3.3.1-python3.11

# TARGETARCH is one of BuildKit's automatic platform args; it has to be
# re-declared with ARG in this stage to be usable below (not implicitly in
# scope just because BuildKit sets it).
ARG TARGETARCH

USER root

# openjdk-17-jdk-headless: Spark 3.5.x requires a JDK; Temurin 17 is what the
# manual's local setup installs via `brew install --cask temurin17`, so match
# the major version here even though this is the apt/Debian build of the JDK,
# not Temurin's own package.
#
# The JAVA_HOME path differs by CPU architecture (java-17-openjdk-amd64 vs
# -arm64), so symlink it to a fixed location instead of hardcoding either.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/default-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# mc (MinIO client): notebooks shell out to it directly via subprocess.run
# (see notebooks/01_delta_lake_tutorial.ipynb) to inspect what actually landed
# on MinIO, separately from what Spark reports. There's no apt package for it
# (curl is already in the base image, used only as the download client here);
# MinIO documents this direct-binary-download method itself:
# https://min.io/docs/minio/linux/reference/minio-mc.html. linux-${TARGETARCH}
# picks the right binary for either amd64 or arm64 hosts.
RUN curl -fsSL "https://dl.min.io/client/mc/release/linux-${TARGETARCH}/mc" -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc

USER airflow

# Pinned to match the local dev venv exactly (see Read of .venv package versions
# during setup) so DAG-authored Spark/Delta code behaves identically in both places.
# pandas ships with the base Airflow 3.3.1 image already (3.0.5, matching the
# venv) — not repeated here. faker is not bundled and the generators/ DAGs
# (dags/generate_*.py) import it via reference_data.py, so it's added here.
# boto3 is not bundled either and the generators now write directly to MinIO's
# raw_incoming_data bucket over S3 (generators/s3_io.py) instead of the local
# ./data/raw bind mount — apache-airflow-providers-amazon isn't installed here
# (no Airflow-side S3 hooks/operators are in use), so this is boto3 on its own,
# not that provider's transitive pin. jupyterlab is for the `jupyter` compose
# service only (see FROM comment above) — Airflow itself never imports it, so
# it can't affect airflow-facing behavior; it's here rather than a separate
# image so both services can't drift onto different pyspark/delta-spark builds.
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.1 \
    faker==40.37.0 \
    boto3==1.43.83 \
    jupyterlab==4.3.5
