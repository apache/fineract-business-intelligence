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

import json
import os
from pathlib import Path

from sqlalchemy import text
from superset.app import create_app

ROOT = Path("/workspace")
DATASET_DIR = ROOT / "superset" / "datasets"
DASHBOARD_DIR = ROOT / "superset" / "dashboards"

app = create_app()
app.app_context().push()

from superset import security_manager  # noqa: E402
from superset.connectors.sqla.models import SqlMetric, SqlaTable, TableColumn  # noqa: E402
from superset.extensions import db  # noqa: E402
from superset.models.core import Database  # noqa: E402
from superset.models.dashboard import Dashboard  # noqa: E402
from superset.models.slice import Slice  # noqa: E402


DELINQUENCY_COLUMNS: list[dict] = [
    {"name": "snapshot_date",             "type": "DATE",    "is_dttm": True},
    {"name": "tenant_id",                 "type": "TEXT"},
    {"name": "office_id",                 "type": "BIGINT"},
    {"name": "office_name",               "type": "TEXT"},
    {"name": "product_id",                "type": "BIGINT"},
    {"name": "product_name",              "type": "TEXT"},
    {"name": "currency_code",             "type": "TEXT"},
    {"name": "bucket_key",                "type": "BIGINT"},
    {"name": "bucket_name",               "type": "TEXT"},
    {"name": "standard_par_band",         "type": "TEXT"},
    {"name": "bucket_loan_count",         "type": "BIGINT"},
    {"name": "bucket_outstanding_amount", "type": "NUMERIC"},
    {"name": "total_loan_count",          "type": "BIGINT"},
    {"name": "total_portfolio_amount",    "type": "NUMERIC"},
    {"name": "watch_list_amount",         "type": "NUMERIC"},
    {"name": "par_30_59_amount",          "type": "NUMERIC"},
    {"name": "par_60_89_amount",          "type": "NUMERIC"},
    {"name": "par_90_plus_amount",        "type": "NUMERIC"},
    {"name": "par_30_amount",             "type": "NUMERIC"},
    {"name": "par_60_amount",             "type": "NUMERIC"},
    {"name": "par_90_amount",             "type": "NUMERIC"},
    {"name": "npa_amount",                "type": "NUMERIC"},
    {"name": "current_amount",            "type": "NUMERIC"},
    {"name": "watch_list_loan_count",     "type": "BIGINT"},
    {"name": "par_30_loan_count",         "type": "BIGINT"},
    {"name": "par_60_loan_count",         "type": "BIGINT"},
    {"name": "par_90_loan_count",         "type": "BIGINT"},
    {"name": "npa_loan_count",            "type": "BIGINT"},
    {"name": "current_loan_count",        "type": "BIGINT"},
    {"name": "par_30_ratio",              "type": "NUMERIC"},
    {"name": "par_60_ratio",              "type": "NUMERIC"},
    {"name": "par_90_ratio",              "type": "NUMERIC"},
    {"name": "npa_ratio",                 "type": "NUMERIC"},
    {"name": "par_30_rate",               "type": "NUMERIC"},
    {"name": "par_60_rate",               "type": "NUMERIC"},
    {"name": "par_90_rate",               "type": "NUMERIC"},
    {"name": "average_loan_outstanding",  "type": "NUMERIC"},
]

PORTFOLIO_COLUMNS: list[dict] = [
    {"name": "snapshot_date",                   "type": "DATE",    "is_dttm": True},
    {"name": "tenant_id",                       "type": "TEXT"},
    {"name": "office_id",                       "type": "BIGINT"},
    {"name": "office_name",                     "type": "TEXT"},
    {"name": "product_id",                      "type": "BIGINT"},
    {"name": "product_name",                    "type": "TEXT"},
    {"name": "currency_code",                   "type": "TEXT"},
    {"name": "active_loan_count",               "type": "BIGINT"},
    {"name": "active_borrower_count",           "type": "BIGINT"},
    {"name": "gross_loan_portfolio",            "type": "NUMERIC"},
    {"name": "total_outstanding_amount",        "type": "NUMERIC"},
    {"name": "interest_outstanding_amount",     "type": "NUMERIC"},
    {"name": "average_loan_size",               "type": "NUMERIC"},
    {"name": "average_exposure_per_borrower",   "type": "NUMERIC"},
    {"name": "npa_outstanding_amount",          "type": "NUMERIC"},
    {"name": "npa_loan_count",                  "type": "BIGINT"},
    {"name": "npa_ratio",                       "type": "NUMERIC"},
    {"name": "par_outstanding_amount",          "type": "NUMERIC"},
    {"name": "par_loan_count",                  "type": "BIGINT"},
    {"name": "par_ratio",                       "type": "NUMERIC"},
    {"name": "performing_outstanding_amount",   "type": "NUMERIC"},
    {"name": "performing_loan_count",           "type": "BIGINT"},
    {"name": "performing_ratio",                "type": "NUMERIC"},
    {"name": "disbursed_amount_on_date",        "type": "NUMERIC"},
    {"name": "disbursed_loan_count_on_date",    "type": "BIGINT"},
    {"name": "principal_collected_on_date",     "type": "NUMERIC"},
    {"name": "collected_amount_on_date",        "type": "NUMERIC"},
    {"name": "writeoff_amount_on_date",         "type": "NUMERIC"},
    {"name": "writeoff_count_on_date",          "type": "BIGINT"},
]

REPAYMENT_COLUMNS: list[dict] = [
    {"name": "reporting_date",                        "type": "DATE",    "is_dttm": True},
    {"name": "client_hash",                           "type": "TEXT"},
    {"name": "office_id",                             "type": "BIGINT"},
    {"name": "office_name",                           "type": "TEXT"},
    {"name": "product_id",                            "type": "BIGINT"},
    {"name": "product_name",                          "type": "TEXT"},
    {"name": "currency_code",                         "type": "TEXT"},
    {"name": "repayment_transaction_count",           "type": "BIGINT"},
    {"name": "repaid_loan_count",                     "type": "BIGINT"},
    {"name": "repaying_borrower_count",               "type": "BIGINT"},
    {"name": "repayment_amount",                      "type": "NUMERIC"},
    {"name": "actual_collected_amount",               "type": "NUMERIC"},
    {"name": "contractually_due_amount",              "type": "NUMERIC"},
    {"name": "collection_efficiency_ratio",           "type": "NUMERIC"},
    {"name": "post_transaction_outstanding_balance",  "type": "NUMERIC"},
    {"name": "principal_collected",                   "type": "NUMERIC"},
    {"name": "interest_collected",                    "type": "NUMERIC"},
    {"name": "fee_collected",                         "type": "NUMERIC"},
    {"name": "penalty_collected",                     "type": "NUMERIC"},
    {"name": "overpayment_collected",                 "type": "NUMERIC"},
    {"name": "waived_amount",                         "type": "NUMERIC"},
    {"name": "paid_in_advance_amount",                "type": "NUMERIC"},
    {"name": "paid_late_amount",                      "type": "NUMERIC"},
    {"name": "early_payment_count",                   "type": "BIGINT"},
    {"name": "on_time_payment_count",                 "type": "BIGINT"},
    {"name": "late_payment_count",                    "type": "BIGINT"},
    {"name": "restructured_installment_count",        "type": "BIGINT"},
    {"name": "overdue_penalty_charged",               "type": "NUMERIC"},
    {"name": "overdue_penalty_waived",                "type": "NUMERIC"},
    {"name": "total_installments_due",                "type": "BIGINT"},
    {"name": "recovery_repayment_amount",             "type": "NUMERIC"},
    {"name": "recovery_loan_count",                   "type": "BIGINT"},
]

DELINQUENCY_METRICS: list[dict] = [
    {
        "metric_name": "par_30_ratio_metric",
        "expression": "SUM(par_30_amount) / NULLIF(SUM(total_portfolio_amount), 0)",
    },
    {
        "metric_name": "par_60_ratio_metric",
        "expression": "SUM(par_60_amount) / NULLIF(SUM(total_portfolio_amount), 0)",
    },
    {
        "metric_name": "par_90_ratio_metric",
        "expression": "SUM(par_90_amount) / NULLIF(SUM(total_portfolio_amount), 0)",
    },
    {
        "metric_name": "npa_ratio_metric",
        "expression": "SUM(npa_amount) / NULLIF(SUM(total_portfolio_amount), 0)",
    },
]

def adhoc_metric(label: str, expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "hasCustomLabel": True,
    }



def sql_filter(sql_expression: str) -> dict:
    return {
        "clause": "WHERE",
        "expressionType": "SQL",
        "sqlExpression": sql_expression,
    }


def mart_exists(database: Database, table_name: str) -> bool:
    with database.get_sqla_engine() as engine:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.tables"
                    "  WHERE table_schema = 'analytics' AND table_name = :t"
                    ")"
                ),
                {"t": table_name},
            ).scalar()
    return bool(result)


def ensure_database(sqlalchemy_uri: str) -> Database:
    database = (
        db.session.query(Database)
        .filter_by(database_name="Analytics Warehouse")
        .one_or_none()
    )
    if database is None:
        database = Database(
            database_name="Analytics Warehouse",
            sqlalchemy_uri=sqlalchemy_uri,
            expose_in_sqllab=True,
            allow_run_async=False,
            allow_ctas=False,
            allow_cvas=False,
        )
        db.session.add(database)
    else:
        database.sqlalchemy_uri = sqlalchemy_uri
        database.expose_in_sqllab = True

    db.session.commit()
    return database


def ensure_dataset(
    database: Database,
    dataset_name: str,
    sql_text: str,
    owner,
    main_dttm_col: str,
    columns: list[dict],
    metrics: list[dict] | None = None,
) -> SqlaTable:
    dataset = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, table_name=dataset_name)
        .one_or_none()
    )

    if dataset is None:
        dataset = SqlaTable(
            table_name=dataset_name,
            sql=sql_text,
            schema=None,
            database=database,
            main_dttm_col=main_dttm_col,
            owners=[owner],
        )
        db.session.add(dataset)
        db.session.commit()
    else:
        dataset.sql = sql_text
        dataset.main_dttm_col = main_dttm_col
        dataset.owners = [owner]
        db.session.commit()

    db.session.query(TableColumn).filter_by(table_id=dataset.id).delete()
    db.session.query(SqlMetric).filter_by(table_id=dataset.id).delete()
    db.session.commit()

    for col in columns:
        db.session.add(
            TableColumn(
                table=dataset,
                column_name=str(col["name"]),
                type=str(col["type"]),
                is_dttm=bool(col.get("is_dttm", False)),
                expression=None,
                verbose_name=str(col["name"]),
            )
        )

    for metric in metrics or []:
        db.session.add(
            SqlMetric(
                table=dataset,
                metric_name=metric["metric_name"],
                expression=metric["expression"],
            )
        )

    db.session.commit()
    return dataset


def ensure_chart(
    chart_name: str,
    viz_type: str,
    datasource: SqlaTable,
    params: dict,
    owner,
) -> Slice:
    full_params = {
        "datasource": f"{datasource.id}__table",
        "viz_type": viz_type,
        **params,
    }
    chart = db.session.query(Slice).filter_by(slice_name=chart_name).one_or_none()
    if chart is None:
        chart = Slice(
            slice_name=chart_name,
            viz_type=viz_type,
            datasource_type="table",
            datasource_id=datasource.id,
            params=json.dumps(full_params),
            owners=[owner],
            cache_timeout=None,
        )
        db.session.add(chart)
    else:
        chart.viz_type = viz_type
        chart.datasource_type = "table"
        chart.datasource_id = datasource.id
        chart.params = json.dumps(full_params)
        chart.query_context = ""
        chart.owners = [owner]

    db.session.commit()
    return chart


def build_layout(
    row_specs: list[dict],
    chart_lookup: dict[str, Slice],
) -> str:
    position: dict = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
    }

    for row_spec in row_specs:
        row_id = str(row_spec["id"])
        chart_names = list(row_spec["charts"])
        component_ids = [f"CHART-explore-{chart_lookup[name].id}" for name in chart_names]

        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": component_ids,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

        default_width = int(row_spec.get("default_width", 4))
        default_height = int(row_spec.get("default_height", 36))
        chart_sizes = dict(row_spec.get("chart_sizes", {}))

        for chart_name in chart_names:
            chart = chart_lookup[chart_name]
            size = dict(chart_sizes.get(chart_name, {}))
            component_id = f"CHART-explore-{chart.id}"
            position[component_id] = {
                "id": component_id,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "sliceName": chart.slice_name,
                    "width": int(size.get("width", default_width)),
                    "height": int(size.get("height", default_height)),
                },
            }

    return json.dumps(position)


def ensure_dashboard(
    meta_filename: str,
    owner,
    charts: list[Slice],
    row_specs: list[dict],
) -> Dashboard:
    meta = json.loads((DASHBOARD_DIR / meta_filename).read_text(encoding="utf-8"))
    dashboard = (
        db.session.query(Dashboard)
        .filter_by(dashboard_title=meta["dashboard_title"])
        .one_or_none()
    )

    if dashboard is None:
        dashboard = Dashboard(
            dashboard_title=meta["dashboard_title"],
            slug=meta["slug"],
            published=True,
            owners=[owner],
            css="",
            json_metadata=json.dumps({"timed_refresh_immune_slices": [], "expanded_slices": {}}),
            position_json="{}",
        )
        db.session.add(dashboard)
        db.session.commit()

    chart_lookup = {chart.slice_name: chart for chart in charts}
    dashboard.owners = [owner]
    dashboard.published = True
    dashboard.position_json = build_layout(row_specs, chart_lookup)
    dashboard.slices = charts
    db.session.commit()
    return dashboard


def cleanup_empty_default_dashboards() -> None:
    dashboards = (
        db.session.query(Dashboard)
        .filter_by(dashboard_title="[ untitled dashboard ]")
        .all()
    )
    for dashboard in dashboards:
        if not dashboard.slices:
            db.session.delete(dashboard)
    db.session.commit()


def create_delinquency_assets(owner, database: Database) -> None:
    ap = sql_filter("bucket_key = -1")
    np = sql_filter("bucket_key != -1")

    all_ds = ensure_dataset(
        database, "delinquency_par_secure_all_dates",
        (DATASET_DIR / "delinquency_par_secure_all_dates.sql").read_text(encoding="utf-8"),
        owner, "snapshot_date", DELINQUENCY_COLUMNS, DELINQUENCY_METRICS,
    )
    lat_ds = ensure_dataset(
        database, "delinquency_par_secure_latest",
        (DATASET_DIR / "delinquency_par_secure_latest.sql").read_text(encoding="utf-8"),
        owner, "snapshot_date", DELINQUENCY_COLUMNS, DELINQUENCY_METRICS,
    )

    charts = [
        ensure_chart("PAR 30 KPI", "big_number_total", lat_ds, {
            "metric": adhoc_metric("PAR 30 %", "(SUM(par_30_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
            "metrics": [adhoc_metric("PAR 30 %", "(SUM(par_30_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)")],
            "adhoc_filters": [ap],
            "number_format": ",.2f%",
            "subheader": "% of portfolio overdue > 30 days",
        }, owner),
        ensure_chart("PAR 60 KPI", "big_number_total", lat_ds, {
            "metric": adhoc_metric("PAR 60 %", "(SUM(par_60_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
            "metrics": [adhoc_metric("PAR 60 %", "(SUM(par_60_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)")],
            "adhoc_filters": [ap],
            "number_format": ",.2f%",
            "subheader": "% of portfolio overdue > 60 days",
        }, owner),
        ensure_chart("PAR 90 KPI", "big_number_total", lat_ds, {
            "metric": adhoc_metric("PAR 90 %", "(SUM(par_90_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
            "metrics": [adhoc_metric("PAR 90 %", "(SUM(par_90_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)")],
            "adhoc_filters": [ap],
            "number_format": ",.2f%",
            "subheader": "% of portfolio overdue > 90 days",
        }, owner),
        ensure_chart("NPA Exposure KPI", "big_number_total", lat_ds, {
            "metric": adhoc_metric("NPA Amount ($)", "SUM(npa_amount)"),
            "metrics": [adhoc_metric("NPA Amount ($)", "SUM(npa_amount)")],
            "adhoc_filters": [ap],
            "number_format": "$,.0f",
            "subheader": "Outstanding principal in non-performing loans",
        }, owner),
        ensure_chart("PAR Trend Line", "line", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("PAR 30", "SUM(par_30_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 60", "SUM(par_60_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 90", "SUM(par_90_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
            ],
            "adhoc_filters": [ap],
            "row_limit": 5000,
            "y_axis_format": ".1%",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("At-Risk Outstanding Trend", "area", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("PAR 30 Amt", "SUM(par_30_amount)"),
                adhoc_metric("PAR 60 Amt", "SUM(par_60_amount)"),
                adhoc_metric("PAR 90 Amt", "SUM(par_90_amount)"),
            ],
            "adhoc_filters": [ap],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "stacked_style": "stack",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Outstanding Principal by Delinquency Band", "dist_bar", lat_ds, {
            "groupby": ["standard_par_band"],
            "metrics": [adhoc_metric("Outstanding Principal", "SUM(bucket_outstanding_amount)")],
            "adhoc_filters": [np],
            "y_axis_format": "$,.0f",
            "bar_stacked": False,
            "show_bar_value": True,
            "rich_tooltip": True,
            "order_bars": True,
            "bottom_margin": 80,
            "left_margin": 80,
        }, owner),
        ensure_chart("Loan Count by Delinquency Band", "dist_bar", lat_ds, {
            "groupby": ["standard_par_band"],
            "metrics": [adhoc_metric("Loan Count", "SUM(bucket_loan_count)")],
            "adhoc_filters": [np],
            "y_axis_format": ",d",
            "bar_stacked": False,
            "show_bar_value": True,
            "rich_tooltip": True,
            "order_bars": True,
            "bottom_margin": 80,
            "left_margin": 80,
        }, owner),
        ensure_chart("PAR by Branch", "dist_bar", lat_ds, {
            "groupby": ["office_name"],
            "metrics": [
                adhoc_metric("Watch-list", "SUM(watch_list_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 30", "SUM(par_30_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 60", "SUM(par_60_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 90", "SUM(par_90_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
            ],
            "adhoc_filters": [ap],
            "y_axis_format": ".1%",
            "bottom_margin": 100,
            "left_margin": 80,
            "color_scheme": "googleCategory10c",
        }, owner),
        ensure_chart("PAR by Product", "dist_bar", lat_ds, {
            "groupby": ["product_name"],
            "metrics": [
                adhoc_metric("Watch-list", "SUM(watch_list_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 30", "SUM(par_30_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 60", "SUM(par_60_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 90", "SUM(par_90_amount)/NULLIF(SUM(total_portfolio_amount),0)"),
            ],
            "color_scheme": "googleCategory10c",
            "adhoc_filters": [ap],
            "y_axis_format": ".1%",
            "bottom_margin": 150,
            "left_margin": 80,
        }, owner),
        ensure_chart("PAR Summary Table", "table", lat_ds, {
            "groupby": ["office_name", "product_name"],
            "metrics": [
                adhoc_metric("Outstanding Principal ($)", "SUM(total_portfolio_amount)"),
                adhoc_metric("PAR 30 %",    "(SUM(par_30_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 60 %",    "(SUM(par_60_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("PAR 90 %",    "(SUM(par_90_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
                adhoc_metric("NPA Loans (#)", "SUM(npa_loan_count)"),
                adhoc_metric("NPA Ratio %",  "(SUM(npa_amount)*100)/NULLIF(SUM(total_portfolio_amount),0)"),
            ],
            "adhoc_filters": [ap],
            "table_timestamp_format": "%Y-%m-%d",
        }, owner),
    ]

    ensure_dashboard("delinquency_par_dashboard.json", owner, charts, [
        {"id": "ROW-KPIS",   "charts": ["PAR 30 KPI", "PAR 60 KPI", "PAR 90 KPI", "NPA Exposure KPI"],
         "default_width": 3, "default_height": 20},
        {"id": "ROW-TREND",  "charts": ["PAR Trend Line", "At-Risk Outstanding Trend"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-DIST",   "charts": ["Outstanding Principal by Delinquency Band", "Loan Count by Delinquency Band"],
         "default_width": 6, "default_height": 36},
        {"id": "ROW-BRANCH", "charts": ["PAR by Branch", "PAR by Product"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-TABLE",  "charts": ["PAR Summary Table"],
         "default_width": 12, "default_height": 36},
    ])
    print("[assets] Delinquency & PAR dashboard created.")


def create_portfolio_assets(owner, database: Database) -> None:
    all_ds = ensure_dataset(
        database, "portfolio_health_secure_all_dates",
        (DATASET_DIR / "portfolio_health_secure_all_dates.sql").read_text(encoding="utf-8"),
        owner, "snapshot_date", PORTFOLIO_COLUMNS,
    )
    lat_ds = ensure_dataset(
        database, "portfolio_health_secure_latest",
        (DATASET_DIR / "portfolio_health_secure_latest.sql").read_text(encoding="utf-8"),
        owner, "snapshot_date", PORTFOLIO_COLUMNS,
    )

    npa_ratio_expr = "(SUM(npa_outstanding_amount)*100)/NULLIF(SUM(gross_loan_portfolio),0)"

    charts = [
        ensure_chart("Gross Loan Portfolio", "big_number_total", lat_ds, {
            "metric": adhoc_metric("GLP", "SUM(gross_loan_portfolio)"),
            "metrics": [adhoc_metric("GLP", "SUM(gross_loan_portfolio)")],
            "number_format": "$,.0f",
            "subheader": "Active principal outstanding",
        }, owner),
        ensure_chart("Active Loans", "big_number_total", lat_ds, {
            "metric": adhoc_metric("Loans", "SUM(active_loan_count)"),
            "metrics": [adhoc_metric("Loans", "SUM(active_loan_count)")],
            "number_format": ",d",
            "subheader": "Active loan accounts",
        }, owner),
        ensure_chart("Active Borrowers", "big_number_total", lat_ds, {
            "metric": adhoc_metric("Borrowers", "SUM(active_borrower_count)"),
            "metrics": [adhoc_metric("Borrowers", "SUM(active_borrower_count)")],
            "number_format": ",d",
            "subheader": "Unique active borrowers",
        }, owner),
        ensure_chart("NPA Ratio", "big_number_total", lat_ds, {
            "metric": adhoc_metric("NPA Ratio %", npa_ratio_expr),
            "metrics": [adhoc_metric("NPA Ratio %", npa_ratio_expr)],
            "number_format": ",.2f%",
            "subheader": "Non-performing loans as % of total outstanding principal",
        }, owner),
        ensure_chart("Average Loan Size", "big_number_total", lat_ds, {
            "metric": adhoc_metric("Avg Loan Size ($)", "SUM(gross_loan_portfolio)/NULLIF(SUM(active_loan_count),0)"),
            "metrics": [adhoc_metric("Avg Loan Size ($)", "SUM(gross_loan_portfolio)/NULLIF(SUM(active_loan_count),0)")],
            "number_format": "$,.0f",
            "subheader": "Average outstanding principal per active loan",
        }, owner),
        ensure_chart("Portfolio Composition Trend", "area", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Performing", "SUM(performing_outstanding_amount)"),
                adhoc_metric("At Risk",    "SUM(par_outstanding_amount)"),
                adhoc_metric("NPA",        "SUM(npa_outstanding_amount)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "x_axis_format": "%b %Y",
            "stacked_style": "stack",
            "zoomable": False,
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Portfolio Quality Trend", "line", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("GLP",        "SUM(gross_loan_portfolio)"),
                adhoc_metric("Performing", "SUM(performing_outstanding_amount)"),
                adhoc_metric("At Risk",    "SUM(par_outstanding_amount)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Active Borrowers & Loans Trend", "line", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Borrowers", "SUM(active_borrower_count)"),
                adhoc_metric("Loans",     "SUM(active_loan_count)"),
            ],
            "row_limit": 5000,
            "y_axis_format": ",d",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Disbursement vs Collection Trend", "line", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Disbursed", "SUM(disbursed_amount_on_date)"),
                adhoc_metric("Collected", "SUM(collected_amount_on_date)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Net Portfolio Flow", "bar", all_ds, {
            "granularity_sqla": "snapshot_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Net Flow", "SUM(disbursed_amount_on_date) - SUM(collected_amount_on_date)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "x_axis_format": "%b %Y",
            "show_brush": False,
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Portfolio by Branch", "dist_bar", lat_ds, {
            "groupby": ["office_name"],
            "metrics": [
                adhoc_metric("GLP", "SUM(gross_loan_portfolio)"),
                adhoc_metric("At Risk", "SUM(par_outstanding_amount)"),
                adhoc_metric("NPA", "SUM(npa_outstanding_amount)"),
            ],
            "y_axis_format": "$,.0f",
            "bottom_margin": 100,
            "left_margin": 80,
        }, owner),
        ensure_chart("Portfolio by Product", "dist_bar", lat_ds, {
            "groupby": ["product_name"],
            "metrics": [
                adhoc_metric("GLP", "SUM(gross_loan_portfolio)"),
                adhoc_metric("At Risk", "SUM(par_outstanding_amount)"),
                adhoc_metric("NPA", "SUM(npa_outstanding_amount)"),
            ],
            "y_axis_format": "$,.0f",
            "bottom_margin": 150,
            "left_margin": 80,
        }, owner),
        ensure_chart("Portfolio Concentration by Branch", "pie", lat_ds, {
            "groupby": ["office_name"],
            "metric": adhoc_metric("GLP", "SUM(gross_loan_portfolio)"),
            "metrics": [adhoc_metric("GLP", "SUM(gross_loan_portfolio)")],
            "number_format": "$,.0f",
        }, owner),
        ensure_chart("Portfolio Health Summary Table", "table", lat_ds, {
            "groupby": ["office_name", "product_name"],
            "metrics": [
                adhoc_metric("Outstanding Principal ($)", "SUM(gross_loan_portfolio)"),
                adhoc_metric("Active Loans (#)",          "SUM(active_loan_count)"),
                adhoc_metric("Active Borrowers (#)",      "SUM(active_borrower_count)"),
                adhoc_metric("Avg Loan Size ($)",         "SUM(gross_loan_portfolio)/NULLIF(SUM(active_loan_count),0)"),
                adhoc_metric("NPA Loans (#)",             "SUM(npa_loan_count)"),
                adhoc_metric("NPA Ratio %",               "(SUM(npa_outstanding_amount)*100)/NULLIF(SUM(gross_loan_portfolio),0)"),
            ],
            "table_timestamp_format": "%Y-%m-%d",
        }, owner),
    ]

    ensure_dashboard("portfolio_health_dashboard.json", owner, charts, [
        {"id": "ROW-PH-KPIS",      "charts": ["Gross Loan Portfolio", "Active Loans", "Active Borrowers", "NPA Ratio", "Average Loan Size"],
         "default_width": 2, "default_height": 20,
         "chart_sizes": {"Gross Loan Portfolio": {"width": 4, "height": 20}}},
        {"id": "ROW-PH-COMPOSITION", "charts": ["Portfolio Composition Trend", "Portfolio Quality Trend"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-PH-FLOW",      "charts": ["Disbursement vs Collection Trend", "Net Portfolio Flow"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-PH-GROWTH",    "charts": ["Active Borrowers & Loans Trend", "Portfolio Concentration by Branch"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-PH-BREAKDOWN", "charts": ["Portfolio by Branch", "Portfolio by Product"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-PH-TABLE",     "charts": ["Portfolio Health Summary Table"],
         "default_width": 12, "default_height": 40},
    ])
    print("[assets] Portfolio Health dashboard created.")


def create_repayment_assets(owner, database: Database) -> None:
    eff_expr = "SUM(actual_collected_amount)/NULLIF(SUM(contractually_due_amount),0)"

    all_ds = ensure_dataset(
        database, "repayment_behavior_secure_all_dates",
        (DATASET_DIR / "repayment_behavior_secure_all_dates.sql").read_text(encoding="utf-8"),
        owner, "reporting_date", REPAYMENT_COLUMNS,
    )
    lat_ds = ensure_dataset(
        database, "repayment_behavior_secure_latest",
        (DATASET_DIR / "repayment_behavior_secure_latest.sql").read_text(encoding="utf-8"),
        owner, "reporting_date", REPAYMENT_COLUMNS,
    )

    charts = [
        ensure_chart("Collection Efficiency KPI", "big_number_total", all_ds, {
            "metric": adhoc_metric("Efficiency %", f"({eff_expr})*100"),
            "metrics": [adhoc_metric("Efficiency %", f"({eff_expr})*100")],
            "number_format": ",.2f",
            "subheader": "% of due amount collected (principal + interest + fees + penalties)",
        }, owner),
        ensure_chart("Collected Amount KPI", "big_number_total", all_ds, {
            "metric": adhoc_metric("Collected", "SUM(actual_collected_amount)"),
            "metrics": [adhoc_metric("Collected", "SUM(actual_collected_amount)")],
            "number_format": "$,.0f",
            "subheader": "Total cash collected (principal + interest + fees + penalties)",
        }, owner),
        ensure_chart("Repayment Transactions KPI", "big_number_total", all_ds, {
            "metric": adhoc_metric("Txns", "SUM(repayment_transaction_count)"),
            "metrics": [adhoc_metric("Txns", "SUM(repayment_transaction_count)")],
            "number_format": ",d",
            "subheader": "Total payment events (one borrower can have multiple)",
        }, owner),
        ensure_chart("Repaying Borrowers KPI", "big_number_total", all_ds, {
            "metric": adhoc_metric("Borrowers", "COUNT(DISTINCT client_hash)"),
            "metrics": [adhoc_metric("Borrowers", "COUNT(DISTINCT client_hash)")],
            "number_format": ",d",
            "subheader": "Unique clients who made a payment (not loan count)",
        }, owner),
        ensure_chart("Repayment Collection Trend", "line", all_ds, {
            "granularity_sqla": "reporting_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Collected", "SUM(actual_collected_amount)"),
                adhoc_metric("Due",       "SUM(contractually_due_amount)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Collection Efficiency Trend", "line", all_ds, {
            "granularity_sqla": "reporting_date",
            "time_grain_sqla": "P1D",
            "metrics": [adhoc_metric("Efficiency %", f"({eff_expr})*100")],
            "row_limit": 5000,
            "y_axis_format": ",.1f",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Repayment Component Breakdown", "area", all_ds, {
            "granularity_sqla": "reporting_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Principal", "SUM(principal_collected)"),
                adhoc_metric("Interest",  "SUM(interest_collected)"),
                adhoc_metric("Fees",      "SUM(fee_collected)"),
                adhoc_metric("Penalties", "SUM(penalty_collected)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "stacked_style": "stack",
            "zoomable": False,
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Collection Mix", "pie", all_ds, {
            "groupby": ["office_name"],
            "metric": adhoc_metric("Collected", "SUM(actual_collected_amount)"),
            "metrics": [adhoc_metric("Collected", "SUM(actual_collected_amount)")],
            "number_format": "$,.0f",
        }, owner),
        ensure_chart("Collected Amount by Branch", "dist_bar", all_ds, {
            "groupby": ["office_name"],
            "metrics": [
                adhoc_metric("Collected", "SUM(actual_collected_amount)"),
                adhoc_metric("Due",       "SUM(contractually_due_amount)"),
            ],
            "y_axis_format": "$,.0f",
            "bottom_margin": 100,
            "left_margin": 80,
        }, owner),
        ensure_chart("Collected Amount by Product", "dist_bar", all_ds, {
            "groupby": ["product_name"],
            "metrics": [
                adhoc_metric("Collected", "SUM(actual_collected_amount)"),
                adhoc_metric("Due",       "SUM(contractually_due_amount)"),
            ],
            "y_axis_format": "$,.0f",
            "bottom_margin": 150,
            "left_margin": 80,
        }, owner),
        ensure_chart("Payment Discipline Trend", "bar", all_ds, {
            "granularity_sqla": "reporting_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Early",   "SUM(early_payment_count)"),
                adhoc_metric("On Time", "SUM(on_time_payment_count)"),
                adhoc_metric("Late",    "SUM(late_payment_count)"),
            ],
            "row_limit": 5000,
            "y_axis_format": ",d",
            "x_axis_format": "%b %Y",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Waiver & Late Payment Trend", "line", all_ds, {
            "granularity_sqla": "reporting_date",
            "time_grain_sqla": "P1D",
            "metrics": [
                adhoc_metric("Waived",     "SUM(waived_amount)"),
                adhoc_metric("Paid Late",  "SUM(paid_late_amount)"),
                adhoc_metric("Paid Early", "SUM(paid_in_advance_amount)"),
            ],
            "row_limit": 5000,
            "y_axis_format": "$,.0f",
            "x_axis_format": "%b %Y",
            "zoomable": False,
            "show_brush": "no",
            "bottom_margin": 60,
        }, owner),
        ensure_chart("Collection Efficiency by Branch", "dist_bar", all_ds, {
            "groupby": ["office_name"],
            "metrics": [
                adhoc_metric("Efficiency %", f"({eff_expr})*100"),
            ],
            "y_axis_format": ".1f",
            "bottom_margin": 100,
            "left_margin": 80,
            "color_scheme": "googleCategory10c",
        }, owner),
        ensure_chart("Collection Efficiency by Product", "dist_bar", all_ds, {
            "groupby": ["product_name"],
            "metrics": [
                adhoc_metric("Efficiency %", f"({eff_expr})*100"),
            ],
            "y_axis_format": ".1f",
            "bottom_margin": 150,
            "left_margin": 80,
            "color_scheme": "googleCategory10c",
        }, owner),



        ensure_chart("Repayment Summary Table", "table", all_ds, {
            "groupby": ["office_name", "product_name"],
            "metrics": [
                adhoc_metric("Collected ($)",       "SUM(actual_collected_amount)"),
                adhoc_metric("Due ($)",              "SUM(contractually_due_amount)"),
                adhoc_metric("Efficiency %",         f"ROUND(({eff_expr})*100, 2)"),
                adhoc_metric("Principal ($)",        "SUM(principal_collected)"),
                adhoc_metric("Interest ($)",         "SUM(interest_collected)"),
                adhoc_metric("Waived ($)",           "SUM(waived_amount)"),
                adhoc_metric("Penalty Charged ($)",  "SUM(overdue_penalty_charged)"),
                adhoc_metric("Restructured (#)",     "SUM(restructured_installment_count)"),
                adhoc_metric("Late Count (#)",       "SUM(late_payment_count)"),
                adhoc_metric("On-Time Count (#)",    "SUM(on_time_payment_count)"),
                adhoc_metric("Recovery ($)",         "SUM(recovery_repayment_amount)"),
                adhoc_metric("Txns (#)",             "SUM(repayment_transaction_count)"),
            ],
            "table_timestamp_format": "%Y-%m-%d",
        }, owner),
    ]

    ensure_dashboard("repayment_behavior_dashboard.json", owner, charts, [
        {"id": "ROW-RP-KPIS",       "charts": ["Collection Efficiency KPI", "Collected Amount KPI", "Repayment Transactions KPI", "Repaying Borrowers KPI"],
         "default_width": 3, "default_height": 20},
        {"id": "ROW-RP-TRENDS1",    "charts": ["Repayment Collection Trend", "Collection Efficiency Trend"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-RP-TRENDS2",    "charts": ["Repayment Component Breakdown", "Collection Mix"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-RP-BREAKDOWN",  "charts": ["Collected Amount by Branch", "Collected Amount by Product"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-RP-DISCIPLINE", "charts": ["Payment Discipline Trend", "Waiver & Late Payment Trend"],
         "default_width": 6, "default_height": 42},
        {"id": "ROW-RP-EFFBRANCH",  "charts": ["Collection Efficiency by Branch", "Collection Efficiency by Product"],
         "default_width": 6, "default_height": 42},


        {"id": "ROW-RP-TABLE",      "charts": ["Repayment Summary Table"],
         "default_width": 12, "default_height": 40},
    ])
    print("[assets] Repayment Behavior dashboard created.")


def main() -> None:
    with app.app_context():
        admin_user = security_manager.find_user(
            username=os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
        )
        if admin_user is None:
            raise RuntimeError("Superset admin user not found. Run 'superset init' first.")

        database = ensure_database(os.environ["SUPERSET_WAREHOUSE_URI"])

        create_delinquency_assets(admin_user, database)

        if mart_exists(database, "mart_portfolio_health"):
            create_portfolio_assets(admin_user, database)
        else:
            print("[assets] Skipping Portfolio Health dashboard — mart_portfolio_health not yet available.")

        if mart_exists(database, "mart_repayment_behavior"):
            create_repayment_assets(admin_user, database)
        else:
            print("[assets] Skipping Repayment Behavior dashboard — mart_repayment_behavior not yet available.")

        cleanup_empty_default_dashboards()
        print("[assets] Bootstrap complete.")


if __name__ == "__main__":
    main()
