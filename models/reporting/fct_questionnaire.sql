

with joined as (

    select q.questionnaire_id
        , q.connection_id
        , r.review_id
        
        -- Questionnaire Dimensions
        , q.source_type
        , q.workflow_type
        , q.original_format
        , q.status

        -- Questionnaire Milestone Timestamps
        , q.created_at
        , q.started_at
        , q.completed_at
        , q.due_at

        -- Questionnaire Dates based on Created_at for Reporting
        , questionnaire_year
        , questionnaire_quarter
        , questionnaire_month
        , quarter_year
        , questionnaire_year_month

        -- Time Intervals
        , q.days_to_completion -- Days from Started_at to Completed_at
        , q.seconds_to_start -- Seconds from Created_at to Started_at
        
        -- Total Question and Answer Counts
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

        -- Questionnaire Funnel
        , q.processing_count
        , q.started_count
        , q.ready_for_review_count
        , q.approved_count
        , q.completed_count
    from {{ ref('int_questionnaires_most_recent') }} q
    left join {{ ref('int_review_question_answer') }} r
        on q.review_id = r.review_id
    left join {{ ref('int_connections_most_recent') }} c 
        on q.connection_id = c.connection_id

)

select * 
from joined
order by questionnaire_id, review_id