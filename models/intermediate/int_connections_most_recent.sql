


with connections as (
    
    select *
    -- Created_at Specifics
        , extract(year from created_at) as connection_created_year
        , extract(quarter from created_at) as connection_created_quarter
        , extract(month from created_at) as connection_created_month
        , concat('Q',extract(quarter from created_at), ' ', extract(year from created_at)) as connection_created_quarter_year
        , format_date('%Y-%m ',date_trunc(created_at, month)) as connection_created_year_month
    from {{ref('base_bq_connections')}} r   
    where rn=1  -- grab only the most recent version based on stitch batched at of the questionnaire
    
)

select *
from connections