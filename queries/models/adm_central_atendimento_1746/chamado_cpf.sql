-- About the partitioning:
-- - Today there are about 8 billion people in the world and BigQuery supports up to 4,000 partitions per table.
-- - Thus, we take the interval to be 2 million.
{{
    config(
        materialized='incremental',
        unique_key='id_chamado',
        partition_by={
            "field": "cpf",
            "data_type": "int64",
            "range": {
                "start": 0,
                "end": 99999999999,
                "interval": 2000000
            }
        },
    )
}}

-- Full refresh or new rows for incremental runs
with base_data as (
    select
        p.cpf,
        c.*  -- Select all columns from the chamado table
    from `rj-segovi.adm_central_atendimento_1746.chamado` c
    left join `rj-segovi.adm_central_atendimento_1746.chamado_pessoa` cp
        on c.id_chamado = cp.id_chamado
    left join `rj-segovi.adm_central_atendimento_1746.pessoa` p
        on cp.id_pessoa = p.id_pessoa
    where
        c.id_chamado is not null
        and cp.id_pessoa is not null
        and p.cpf is not null
),
filtered_increment as (
    {% if is_incremental() %}
    -- Only add data not already in the target table
    select *
    from base_data
    where id_chamado not in (
        select id_chamado
        from {{ this }}
    )
    {% else %}
    -- For full refreshes, include all rows
    select *
    from base_data
    {% endif %}
)
select
    SAFE_CAST(cpf AS INT64) cpf,
    * except(cpf) -- Include all columns except duplicating cpf
from filtered_increment
