
with reviews as (

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
    from {{ref('base_bq_reviews')}} r    
    left join {{ref('base_bq_review_questions')}} q
        on r.review_id = q.review_id
    left join {{ref('int_review_answers')}} a
        on q.question_id = a.question_id

)

, final as (

    select review_id
        , count(distinct question_id) as question_total
        , count(distinct case when issue then question_id else NULL end) as issue_count
        , count(distinct case when answer_sequence_desc = 1 and user_type = 'Generative AI' then question_id else null end) as ai_generated_accepted_answer_total
        , count(distinct case when answer_sequence_desc = 1 and user_type = 'User Generated' then question_id else null end) as user_generated_accepted_answer_total
        , count(distinct case when answer_sequence_asc = 1 and user_type = 'Generative AI' then question_id else null end) as attempted_ai_generated_answer_total
        , count(case when user_type = 'Generative AI' then question_id else null end) as ai_generated_answer_total
        , count(case when user_type = 'User Generated' then question_id else null end) as user_generated_answer_total        
    from reviews
    group by 1

)

select * from final