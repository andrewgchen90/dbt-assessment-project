
with questionnaires_qa as (
    select *
    from {{ref('base_bq_reviews')}} r    
    left join {{ref('base_bq_review_questions')}} q
        on r.review_id = q.review_id
    left join {{ref('int_review_answers')}} a
        on q.question_id = a.question_id
)


select * from questionnaires_qa
