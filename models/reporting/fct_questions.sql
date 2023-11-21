

with question_answer_categorization as (

    select 
        q.question_id
        , q.question_text
        , q.document_type
        , q.issue
        , q.issue_description
        , q.question_position
        , q.question_status
        , q.question_created_at
        -- Created_at Specifics
        , extract(year from question_created_at) as question_year
        , extract(quarter from question_created_at) as question_quarter
        , extract(month from question_created_at) as question_month
        , concat('Q',extract(quarter from question_created_at), ' ', extract(year from question_created_at)) as question_quarter_year
        , format_date('%Y-%m ',date_trunc(question_created_at, month)) as question_year_month


        -- The reason I used distinct was because there can be potentially multiple answers to a question so I don't want to overcount
        , count(distinct question_id) as question_total
        , count(distinct case when issue then question_id else NULL end) as issue_count
        , count(distinct case when answer_sequence_desc = 1 and user_type = 'Generative AI' then question_id else null end) as ai_generated_accepted_answer_total
        , count(distinct case when answer_sequence_desc = 1 and user_type = 'User Generated' then question_id else null end) as user_generated_accepted_answer_total
        , count(distinct case when generative_ai_answer_confidence = 'Confident' then question_id else null end) as ai_generated_confident_answer_total
        , count(distinct case when generative_ai_answer_confidence = 'Not Confident' then question_id else null end) as ai_generated_not_confident_answer_total
        , count(distinct case when generative_ai_answer_confidence = 'No Attempt' then question_id else null end) as no_attempt_at_answer_total
        , count(distinct case when answer_sequence_asc = 1 and user_type = 'Generative AI' then question_id else null end) as attempted_ai_generated_answer_total
        
        -- No need to use distinct question_id here, counting overall answers
        , count(answer_id) as answer_total
        , count(case when user_type = 'Generative AI' then answer_id else null end) as ai_generated_answer_total
        , count(case when user_type = 'User Generated' then answer_id else null end) as user_generated_answer_total        
    from {{ ref('int_review_question_answer') }} q 
    group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * 
from question_answer_categorization
order by question_id
