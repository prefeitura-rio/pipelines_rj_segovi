{{
    config(
        materialized='table',
        unique_key='id_chamado',
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {
                "start": 0,
                "end": 99999999999,
                "interval": 10000000
            }
        },
    )
}}

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    cpf
FROM `rj-segovi.adm_central_atendimento_1746_staging.chamado_cpf`
