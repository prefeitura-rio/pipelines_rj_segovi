{{
    config(
        materialized='table',
        unique_key='id_chamado',
        partition_by={
            "field": "id_pessoa",
            "data_type": "int64",
        }
    )
}}

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    SAFE_CAST(
        REGEXP_REPLACE(id_pessoa_fk, r'\.0$', '') AS INT64
    ) id_pessoa
FROM `rj-segovi.adm_central_atendimento_1746_staging.chamado_pessoa`
WHERE id_pessoa_fk IS NOT NULL