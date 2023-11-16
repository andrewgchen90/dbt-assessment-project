with raw_source as (

    select *
        , lead(answer_text,1) over (partition by question_id order by created_at) next_answer_text
        , case when user_id is null then 'Generative AI' else 'User Generated' end as user_type        
    from {{ source('conveyor', 'review_answers') }}

)

, final as (

    select *
        , case when answer_text = next_answer_text then TRUE else FALSE end as is_duplicate_answer
    from raw_source

)

select * 
from final
where not is_duplicate_answer
