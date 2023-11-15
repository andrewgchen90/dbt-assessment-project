with raw_source as (

    select *
    from {{ source('conveyor', 'review_answers') }}

),

final as (

    select *
    from raw_source

)

select * from final
