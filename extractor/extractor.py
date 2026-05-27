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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pg8000.dbapi

from extractor.config import AppConfig
from extractor.replica_lag_check import ensure_replica_safe
from extractor.watermark_manager import WatermarkManager

logger = logging.getLogger(__name__)


def as_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class TableSpec:
    source_table: str
    raw_table: str
    columns: tuple[str, ...]
    primary_key: str
    cursor_column: str = "last_modified_on_utc"


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        source_table="m_office",
        raw_table="raw_m_office",
        primary_key="id",
        columns=(
            "id",
            "parent_id",
            "hierarchy",
            "external_id",
            "name",
            "opening_date",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_currency",
        raw_table="raw_m_currency",
        primary_key="id",
        columns=(
            "id",
            "code",
            "decimal_places",
            "currency_multiplesof",
            "display_symbol",
            "name",
            "internationalized_name_code",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_client",
        raw_table="raw_m_client",
        primary_key="id",
        columns=(
            "id",
            "account_no",
            "external_id",
            "status_enum",
            "activation_date",
            "office_joining_date",
            "office_id",
            "staff_id",
            "gender_cv_id",
            "date_of_birth",
            "legal_form_enum",
            "submittedon_date",
            "updated_on",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_product_loan",
        raw_table="raw_m_product_loan",
        primary_key="id",
        columns=(
            "id",
            "short_name",
            "currency_code",
            "currency_digits",
            "currency_multiplesof",
            "principal_amount",
            "min_principal_amount",
            "max_principal_amount",
            "arrearstolerance_amount",
            "name",
            "description",
            "nominal_interest_rate_per_period",
            "annual_nominal_interest_rate",
            "repay_every",
            "repayment_period_frequency_enum",
            "number_of_repayments",
            "overdue_days_for_npa",
            "start_date",
            "close_date",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_loan",
        raw_table="raw_m_loan",
        primary_key="id",
        columns=(
            "id",
            "account_no",
            "external_id",
            "client_id",
            "product_id",
            "loan_status_id",
            "loan_type_enum",
            "currency_code",
            "currency_digits",
            "currency_multiplesof",
            "principal_amount_proposed",
            "principal_amount",
            "approved_principal",
            "net_disbursal_amount",
            "annual_nominal_interest_rate",
            "nominal_interest_rate_per_period",
            "interest_method_enum",
            "interest_calculated_in_period_enum",
            "term_frequency",
            "term_period_frequency_enum",
            "repay_every",
            "repayment_period_frequency_enum",
            "number_of_repayments",
            "amortization_method_enum",
            "submittedon_date",
            "approvedon_date",
            "expected_disbursedon_date",
            "expected_firstrepaymenton_date",
            "disbursedon_date",
            "expected_maturedon_date",
            "maturedon_date",
            "principal_disbursed_derived",
            "principal_repaid_derived",
            "principal_writtenoff_derived",
            "principal_outstanding_derived",
            "interest_charged_derived",
            "interest_repaid_derived",
            "interest_writtenoff_derived",
            "interest_outstanding_derived",
            "fee_charges_outstanding_derived",
            "penalty_charges_outstanding_derived",
            "total_expected_repayment_derived",
            "total_repayment_derived",
            "total_writtenoff_derived",
            "total_outstanding_derived",
            "loan_counter",
            "is_npa",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_loan_transaction",
        raw_table="raw_m_loan_transaction",
        primary_key="id",
        columns=(
            "id",
            "loan_id",
            "office_id",
            "is_reversed",
            "transaction_type_enum",
            "transaction_date",
            "amount",
            "principal_portion_derived",
            "interest_portion_derived",
            "fee_charges_portion_derived",
            "penalty_charges_portion_derived",
            "outstanding_loan_balance_derived",
            "submitted_on_date",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_delinquency_range",
        raw_table="raw_m_delinquency_range",
        primary_key="id",
        columns=(
            "id",
            "classification",
            "min_age_days",
            "max_age_days",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_delinquency_bucket",
        raw_table="raw_m_delinquency_bucket",
        primary_key="id",
        columns=(
            "id",
            "name",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_delinquency_bucket_mappings",
        raw_table="raw_m_delinquency_bucket_mappings",
        primary_key="id",
        columns=(
            "id",
            "delinquency_range_id",
            "delinquency_bucket_id",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="m_loan_delinquency_tag_history",
        raw_table="raw_m_loan_delinquency_tag_history",
        primary_key="id",
        columns=(
            "id",
            "delinquency_range_id",
            "loan_id",
            "addedon_date",
            "liftedon_date",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
    TableSpec(
        source_table="batch_job_execution",
        raw_table="raw_batch_job_execution",
        primary_key="job_execution_id",
        columns=(
            "job_execution_id",
            "status",
            "start_time",
            "end_time",
            "exit_code",
            "exit_message",
            "created_on_utc",
            "last_modified_on_utc",
        ),
    ),
)


class FineractExtractor:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def _source_conn(self):
        return pg8000.dbapi.connect(**self.config.source.connect_kwargs)

    def _warehouse_conn(self):
        return pg8000.dbapi.connect(**self.config.warehouse.connect_kwargs)

    def run(self, mode: str) -> dict[str, int | str]:
        if mode not in {"backfill", "incremental"}:
            raise ValueError(f"Unsupported mode: {mode}")

        run_id = uuid.uuid4()
        rows_extracted = 0
        rows_loaded = 0

        source_conn = self._source_conn()
        warehouse_conn = self._warehouse_conn()
        try:
            source_conn.autocommit = True
            warehouse_conn.autocommit = False

            logger.info("Starting %s extraction for tenant '%s'.", mode, self.config.tenant_id)
            self._insert_pipeline_state(warehouse_conn, run_id, mode, "running")
            watermark_manager = WatermarkManager(warehouse_conn, self.config.tenant_id)

            try:
                if mode == "backfill":
                    self._reset_backfill_state(warehouse_conn, watermark_manager)
                lag_seconds = ensure_replica_safe(source_conn, self.config.replica_lag_threshold_seconds)
                logger.info("Replica safety check passed with lag=%ss.", lag_seconds)
                self._ensure_cob_completed(source_conn)

                for spec in TABLE_SPECS:
                    logger.info("Extracting source table '%s' into raw.%s.", spec.source_table, spec.raw_table)
                    extracted_rows = self._extract_table(source_conn, warehouse_conn, watermark_manager, spec, mode)
                    rows_extracted += extracted_rows
                    rows_loaded += extracted_rows
                    logger.info(
                        "Completed table '%s': rows_extracted=%s rows_loaded=%s.",
                        spec.source_table,
                        extracted_rows,
                        extracted_rows,
                    )

                self._update_pipeline_state(
                    warehouse_conn,
                    run_id,
                    status="success",
                    rows_extracted=rows_extracted,
                    rows_loaded=rows_loaded,
                )
                warehouse_conn.commit()
                logger.info(
                    "Extraction run %s completed successfully: rows_extracted=%s rows_loaded=%s.",
                    run_id,
                    rows_extracted,
                    rows_loaded,
                )
                return {
                    "run_id": str(run_id),
                    "status": "success",
                    "rows_extracted": rows_extracted,
                    "rows_loaded": rows_loaded,
                    "replica_lag_seconds": lag_seconds,
                }
            except Exception as exc:
                warehouse_conn.rollback()
                logger.exception("Extraction run %s failed.", run_id)
                self._update_pipeline_state(
                    warehouse_conn,
                    run_id,
                    status="failed",
                    rows_extracted=rows_extracted,
                    rows_loaded=rows_loaded,
                    error_message=str(exc),
                )
                warehouse_conn.commit()
                raise
        finally:
            source_conn.close()
            warehouse_conn.close()

    def _reset_backfill_state(self, warehouse_conn, watermark_manager: WatermarkManager) -> None:
        cursor = warehouse_conn.cursor()
        for spec in TABLE_SPECS:
            cursor.execute(
                f"DELETE FROM raw.{_quote_identifier(spec.raw_table)} WHERE tenant_id = %s",
                (self.config.tenant_id,),
            )
        cursor.execute(
            """
            DELETE FROM meta.watermarks
            WHERE tenant_id = %s
            """,
            (self.config.tenant_id,),
        )
        logger.info("Backfill reset completed for raw layer and watermarks.")

    def _insert_pipeline_state(self, warehouse_conn, run_id: uuid.UUID, mode: str, status: str) -> None:
        cursor = warehouse_conn.cursor()
        cursor.execute(
            """
            INSERT INTO meta.pipeline_state (
                run_id,
                tenant_id,
                run_mode,
                started_at,
                status,
                rows_extracted,
                rows_loaded
            )
            VALUES (%s, %s, %s, %s, %s, 0, 0)
            """,
            (str(run_id), self.config.tenant_id, mode, datetime.now(timezone.utc), status),
        )
        warehouse_conn.commit()

    def _update_pipeline_state(
        self,
        warehouse_conn,
        run_id: uuid.UUID,
        status: str,
        rows_extracted: int,
        rows_loaded: int,
        error_message: str | None = None,
    ) -> None:
        cursor = warehouse_conn.cursor()
        cursor.execute(
            """
            UPDATE meta.pipeline_state
            SET completed_at = %s,
                status = %s,
                rows_extracted = %s,
                rows_loaded = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                status,
                rows_extracted,
                rows_loaded,
                error_message,
                str(run_id),
            ),
        )

    def _ensure_cob_completed(self, source_conn) -> None:
        schema = _quote_identifier(self.config.source.schema)
        cursor = source_conn.cursor()
        cursor.execute(
            f"""
            SELECT MAX(end_time)
            FROM {schema}.batch_job_execution
            WHERE status = %s
            """,
            ("COMPLETED",),
        )
        last_completed = as_utc_datetime(cursor.fetchone()[0])

        if last_completed is None:
            raise RuntimeError("No completed COB execution found in batch_job_execution.")

        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.config.cob_lookback_hours)
        if last_completed < cutoff:
            raise RuntimeError(
                f"Latest COB completion {last_completed.isoformat()} is older than {self.config.cob_lookback_hours} hours."
            )
        logger.info("COB completion gate passed with latest completion at %s.", last_completed.isoformat())

    def _extract_table(
        self,
        source_conn,
        warehouse_conn,
        watermark_manager: WatermarkManager,
        spec: TableSpec,
        mode: str,
    ) -> int:
        previous_watermark = None if mode == "backfill" else watermark_manager.get(spec.source_table)

        effective_watermark = previous_watermark
        if effective_watermark is not None and self.config.extract_lookback_seconds > 0:
            effective_watermark = effective_watermark - timedelta(seconds=self.config.extract_lookback_seconds)
            logger.info(
                "Applied lookback window of %ss for '%s': effective_watermark=%s.",
                self.config.extract_lookback_seconds,
                spec.source_table,
                effective_watermark.isoformat(),
            )

        total_rows = 0
        query, params = self._build_source_query(spec, effective_watermark)

        cursor = source_conn.cursor()
        cursor.execute(query, params)
        while True:
            batch = cursor.fetchmany(self.config.extract_batch_size)
            if not batch:
                break
            total_rows += len(batch)
            cursor_col_index = spec.columns.index(spec.cursor_column)
            latest_watermark = max(row[cursor_col_index] for row in batch)
            self._upsert_rows(warehouse_conn, spec, batch)
            watermark_manager.update(spec.source_table, spec.cursor_column, latest_watermark)
            logger.info(
                "Loaded batch for '%s': batch_size=%s latest_watermark=%s.",
                spec.source_table,
                len(batch),
                latest_watermark.isoformat() if latest_watermark else None,
            )

        return total_rows

    def _build_source_query(self, spec: TableSpec, watermark: datetime | None) -> tuple[str, tuple]:
        columns_sql = ", ".join(_quote_identifier(col) for col in spec.columns)
        schema = _quote_identifier(self.config.source.schema)
        table = _quote_identifier(spec.source_table)
        cursor = _quote_identifier(spec.cursor_column)
        pk = _quote_identifier(spec.primary_key)

        base = f"SELECT {columns_sql} FROM {schema}.{table}"
        order = f"ORDER BY {cursor}, {pk}"

        if watermark is None:
            return f"{base} {order}", ()

        return f"{base} WHERE {cursor} >= %s {order}", (watermark,)

    def _upsert_rows(self, warehouse_conn, spec: TableSpec, rows: list[tuple]) -> None:
        raw_columns = ("tenant_id", *spec.columns)
        insert_columns_sql = ", ".join(_quote_identifier(column) for column in raw_columns)
        update_columns = [
            column
            for column in raw_columns
            if column not in {"tenant_id", spec.primary_key}
        ]
        update_assignments_sql = ", ".join(
            f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
            for column in update_columns
        )

        raw_table = _quote_identifier(spec.raw_table)
        pk = _quote_identifier(spec.primary_key)

        placeholders = []
        values = []
        for row in rows:
            row_values = (self.config.tenant_id, *row)
            ph = "(" + ", ".join("%s" for _ in row_values) + ")"
            placeholders.append(ph)
            values.extend(row_values)

        values_sql = ", ".join(placeholders)

        upsert_query = f"""
            INSERT INTO raw.{raw_table} ({insert_columns_sql})
            VALUES {values_sql}
            ON CONFLICT ({_quote_identifier("tenant_id")}, {pk})
            DO UPDATE SET {update_assignments_sql}
        """

        cursor = warehouse_conn.cursor()
        cursor.execute(upsert_query, values)
