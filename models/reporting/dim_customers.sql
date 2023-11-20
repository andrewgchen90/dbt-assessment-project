

with customers as (
    select *
        , row_number() over (order by created_at asc) customer_sequence
    from {{ ref('int_connections_most_recent') }}
)
select c.connection_id
        , c.domain
        , c.created_at
        , c.dataroom_id
        , c.crm_id	
        , c.updated_at
        , c.first_nda_signed_at
        , c.crm_link
        , c.automated_nda_bypass
        , c.bypass_justification
        , c.auto_approve_requests
        , c.customer_sequence
        , count(distinct c.connection_id) as connection_customer_count
        , count(distinct case when q.processing_count >= 1 then c.connection_id else NULL end) as processing_customer_count
        , count(distinct case when q.started_count >= 1 then c.connection_id else NULL end) as started_customer_count
        , count(distinct case when q.ready_for_review_count >= 1 then c.connection_id else NULL end) as ready_for_review_customer_count
        , count(distinct case when q.approved_count >= 1 then c.connection_id else NULL end) as approved_customer_count
        , count(distinct case when q.completed_count >= 1 then c.connection_id else NULL end) as completed_customer_count
        , sum(q.processing_count) as processing_questionnaire_count
        , sum(q.started_count) as started_questionnaire_count
        , sum(q.ready_for_review_count) as ready_for_review_questionnaire_count
        , sum(q.approved_count) as approved_questionnaire_count
        , sum(q.completed_count) as completed_questionnaire_count
from customers c
left join {{ ref('int_questionnaires_most_recent') }} q
    on c.connection_id = q.connection_id
group by 1,2,3,4,5,6,7,8,9,10,11,12