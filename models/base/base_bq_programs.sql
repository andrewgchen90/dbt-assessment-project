with raw_source as (

    select *
    from {{ source('conveyor', 'programs') }}

),

final as (

    select *
    from raw_source

)

select * from final
