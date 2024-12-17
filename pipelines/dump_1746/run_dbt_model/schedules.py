# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

materialize_flow_schedule = Schedule(
    clocks=[
        # Every day at 6am
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2020, 1, 1, 6, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[constants.RJ_SEGOVI_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "app_identidade_unica",
                "dbt_alias": False,
                "dbt_model_parameters": {},
                "dbt_model_secret_parameters": [],
                "dbt_project_materialization": None,
                "downstream": None,
                "exclude": None,
                "flags": None,
                "infisical_credential_dict": None,
                "table_id": "1746_chamado_cpf",
                "upstream": None,
                "mode": "prod",
                "materialize_to_datario": False,
            },
        )
    ]
)
