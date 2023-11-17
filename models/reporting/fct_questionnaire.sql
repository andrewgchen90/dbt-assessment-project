

with joined as (

    select q.questionnaire_id
        , r.*
    from {{ ref('int_questionnaires_most_recent') }} q
    left join {{ ref('int_review_question_answer') }} r
    on q.review_id = r.review_id

)

select * from joined