# -*- coding: utf-8 -*-
"""
Database dumping flows for segovi project.
"""

from copy import deepcopy

from prefect.run_configs import DockerRun, KubernetesRun  # noqa
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.dump_db.flows import flow as dump_sql_flow
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.dump_1746.dump_db.schedules import _1746_daily_update_schedule

dump_sql_ergon_flow = deepcopy(dump_sql_flow)
dump_sql_ergon_flow.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
dump_sql_ergon_flow.name = "SEGOVI: 1746 - Ingerir tabelas de banco SQL"
dump_sql_ergon_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_sql_ergon_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)
# dump_sql_ergon_flow.run_config = DockerRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[
#         constants.RJ_LOCAL_IPLAN_AGENT_LABEL.value,
#     ],
# )


ergon_default_parameters = {
    "db_database": "P01.PCRJ",
    "db_host": "10.70.6.21",
    "db_port": "1526",
    "db_type": "oracle",
    "infisical_secret_path": "/db-ergon-prod",
    "dataset_id": "recursos_humanos_ergon",
}
dump_sql_ergon_flow = set_default_parameters(
    dump_sql_ergon_flow, default_parameters=ergon_default_parameters
)

dump_sql_ergon_flow.schedule = _1746_daily_update_schedule
