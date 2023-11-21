
with questions as (

    select question_id
    , source_question_id
    , review_id
    , question_text
    , document_type
    , issue
    , issue_description
    , question_position
    , question_status
    , question_created_at
    , updated_at
    from {{ref('base_bq_review_questions')}} r   
    
)

select *
from questions

