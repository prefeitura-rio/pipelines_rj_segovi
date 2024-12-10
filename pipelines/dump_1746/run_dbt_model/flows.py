# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project.
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun  # noqa
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.dump_1746.run_dbt_model.schedules import materialize_flow_schedule
from pipelines.templates.run_dbt_model.flows import templates__run_dbt_model__flow as materialize_flow

materialize_only_1746_tables = deepcopy(materialize_flow)
materialize_only_1746_tables.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
materialize_only_1746_tables.name = "SEGOVI: 1746 - Materializar tabelas"
materialize_only_1746_tables.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
materialize_only_1746_tables.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)
materialize_only_1746_tables.schedule = materialize_flow_schedule
