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
from typing import Any, Protocol


class _ChartLike(Protocol):
    id: int
    slice_name: str


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


def build_layout(
    row_specs: list[dict],
    chart_lookup: dict[str, _ChartLike],
) -> str:
    position: dict[str, Any] = {
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
    {"name": "overdue_penalty_charged",                "type": "NUMERIC"},
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
