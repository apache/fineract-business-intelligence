# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,

FROM apache/airflow:3.1.5-python3.11

USER root
RUN mkdir -p /opt/dbt-venv /opt/airflow/passwords && chown airflow: /opt/dbt-venv /opt/airflow/passwords
USER airflow

COPY extractor/requirements.txt /tmp/extractor-requirements.txt
RUN pip install --no-cache-dir -r /tmp/extractor-requirements.txt

COPY dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

ENV PYTHONPATH=/opt/fbi \
    FBI_CONTRACT_PATH=/opt/fbi/contracts/fineract_tables.json \
    FBI_DBT_BIN=/opt/dbt-venv/bin/dbt \
    FBI_DBT_PROJECT_DIR=/opt/fbi/dbt
