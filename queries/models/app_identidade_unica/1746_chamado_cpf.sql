{{
    config(
        materialized='incremental',
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

WITH origem_ocorrencia AS (
    SELECT
        id_origem_ocorrencia,
        no_origem_ocorrencia
    FROM
        `rj-segovi.adm_central_atendimento_1746.origem_ocorrencia`
),
chamado AS (
    SELECT
        *
    FROM
        `rj-segovi.adm_central_atendimento_1746.chamado`
),
chamado_cpf AS (
    SELECT
        id_chamado,
        cpf
    FROM
        `rj-segovi.adm_central_atendimento_1746.chamado_cpf`
)

SELECT
    c.cpf,
    CAST(c.cpf AS INT64) AS cpf_particao,  -- For partitioning purposes
    oo.no_origem_ocorrencia AS origem_ocorrencia,
    c.*
FROM
    chamado c
JOIN
    chamado_cpf cc ON c.id_chamado = cc.id_chamado
LEFT JOIN
    origem_ocorrencia oo ON c.id_origem_ocorrencia = oo.id_origem_ocorrencia

{% if is_incremental() %}
WHERE
    c.data_particao > (SELECT MAX(data_particao) FROM {{ this }})
{% endif %}