
with answers as (
    select answer_id
        , question_id
        , answer_text
        , email
        , user_id
        , created_at
        , updated_at
        , next_answer_text
        , user_type
        , is_duplicate_answer
        , row_number() over (partition by question_id order by created_at) as answer_sequence_asc
        , row_number() over (partition by question_id order by created_at desc) as answer_sequence_desc
        , row_number() over (partition by question_id, user_id order by created_at) as user_answer_sequence_asc
        , row_number() over (partition by question_id, user_id order by created_at desc) as user_answer_sequence_desc
    from {{ref('base_bq_review_answers')}} r   
)

select *
    , case when answer_sequence_asc = 1 and user_type = 'Generative AI' then true else false end as is_generative_ai_attempted
from answers