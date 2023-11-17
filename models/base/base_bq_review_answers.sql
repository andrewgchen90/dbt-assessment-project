with raw_source as (

    select 
        answer_id
        , question_id
        , answer_text
        , email answer_email
        , user_id answer_user_id
        , created_at
        , updated_at
        , lag(answer_text,1) over (partition by question_id order by created_at) previous_answer_text
        , case when user_id is null then 'Generative AI' else 'User Generated' end as user_type        
    from {{ source('conveyor', 'review_answers') }}

)

, final as (

    select *
        , case when answer_text = previous_answer_text then TRUE else FALSE end as is_duplicate_answer
    from raw_source

)

select * 
from final
where not is_duplicate_answer
