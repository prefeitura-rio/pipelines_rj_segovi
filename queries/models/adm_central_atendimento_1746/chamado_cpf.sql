{{
    config(
        materialized='incremental',
        partition_by={"field": "cpf", "data_type": "string"},
        unique_key='id_chamado'
    )
}}

-- Full refresh or new rows for incremental runs
with base_data as (
    select
        c.id_chamado,
        p.cpf,
        to_json_string(c) as data
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
    cpf,
    data
from filtered_increment