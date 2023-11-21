
with review_questions as (

    select 
        r.review_id
        , r.canonical_asset_id
        , r.program_id
        , r.status
        , r.user_id
        , r.email
        , r.created_at
        , r.updated_at
        , q.question_id
        , q.question_text
        , q.document_type
        , q.issue
        , q.issue_description
        , q.question_position
        , q.question_status
        , q.question_created_at
        , a.answer_id
        , a.answer_text
        , a.answer_email
        , a.answer_user_id
        , a.previous_answer_text
        , a.user_type
        , a.answer_sequence_asc
        , a.answer_sequence_desc
        , a.user_answer_sequence_asc
        , a.user_answer_sequence_desc
        , case when answer_sequence_asc = 1 and user_type = 'Generative AI' and issue is false then 'Confident'
            when answer_sequence_asc = 1 and user_type = 'Generative AI' and issue then 'Not Confident'
            when answer_sequence_asc is null and issue then 'Not Confident'
             when answer_sequence_asc is null then 'No Attempt'
             when answer_sequence_asc = 1 and user_type = 'User Generated' and issue is false then 'No Attempt'
            else null
            end as generative_ai_answer_confidence
    from {{ ref('int_review_questions') }} q
    left join {{ref('base_bq_reviews')}} r    
        on q.review_id = r.review_id
    left join {{ref('int_review_answers')}} a
        on q.question_id = a.question_id

)

select * from review_questions