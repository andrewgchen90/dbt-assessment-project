
with questionnaires as (
    select *
    from {{ref('base_bq_questionnaires')}} r   
    where rn=1  -- grab only the most recent version based on stitch batched at of the questionnaire
)

select *
from questionnaires