
with reviews as (

    select r.*
        , q.question_id
        , q.content
        , q.question_text
        , q.document_type
        , q.issue
        , q.issue_description
        , q.question_position
        , q.question_status
        , a.answer_id
        , a.answer_text
        , a.email
        , a.user_id
        , a.next_answer_text
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


select * from reviews
