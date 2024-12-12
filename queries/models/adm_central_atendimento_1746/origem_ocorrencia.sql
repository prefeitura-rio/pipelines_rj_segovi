{{
    config(
        materialized='table',
        unique_key='id_origem_ocorrencia',
    )
}}

SELECT
    *
FROM `rj-segovi.adm_central_atendimento_1746_staging.origem_ocorrencia`
