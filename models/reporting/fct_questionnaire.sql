

with joined as (

    select q.questionnaire_id
        , q.source_type
        , q.created_at
        , q.started_at
        , q.completed_at
        , q.questions_completed
        , q.hours_spent
        , q.due_at
        , q.workflow_type
        , q.original_format
        , q.connection_id
        , q.status
        , r.review_id
        , r.question_total
        , r.issue_count
        , r.ai_generated_accepted_answer_total
        , r.user_generated_accepted_answer_total
        , r.ai_generated_confident_answer_total
        , r.ai_generated_not_confident_answer_total
        , r.no_attempt_at_answer_total
        , r.attempted_ai_generated_answer_total
        , r.ai_generated_answer_total
        , r.user_generated_answer_total      
    from {{ ref('int_questionnaires_most_recent') }} q
    left join {{ ref('int_review_question_answer') }} r
        on q.review_id = r.review_id
    left join {{ ref('int_connections_most_recent') }} c 
        on q.connection_id = c.connection_id

)

select * 
from joined
order by questionnaire_id, review_id