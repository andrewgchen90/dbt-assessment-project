with raw_source as (

    select *
    from {{ source('conveyor', 'reviews') }}

)

, final as (

    select 
        review_id
        , canonical_asset_id
        , program_id
        , status
        , user_id
        , email
        , created_at
        , updated_at
    from raw_source

)

select * from final
