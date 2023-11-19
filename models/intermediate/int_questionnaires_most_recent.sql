

with questionnaires as (
    select questionnaire_id
        , created_at
        , started_at
        , completed_at
        , due_at
        , deleted_at
        , round(date_diff(completed_at,started_at,hour)/24,1)  days_to_completion
        , round(date_diff(started_at,created_at,second),1)  seconds_to_start
        , source_type	
        , review_id	
        , question_count
        , question_group_import_id
        , program_id
        , workflow_type
        , original_format
        , connection_id
        , currently_responsible
        , notes
        , user_id
        , email	
        , conveyor_done_at
        , position
        , updated_at 

        -- Created_at Specifics
        , extract(year from created_at) as questionnaire_year
        , extract(quarter from created_at) as questionnaire_quarter
        , extract(month from created_at) as questionnaire_month
        , concat('Q',extract(quarter from created_at), ' ', extract(year from created_at)) as quarter_year
        , format_date('%Y-%m ',date_trunc(created_at, month)) as questionnaire_year_month

        -- SLA
        , sla_days
        , sla_started_at
        
        -- Status Related
        , status
        , case when status in ('processing', 'started', 'ready_for_review', 'approved', 'completed') then 1 else NULL end as processing_count
        , case when status in ('started', 'ready_for_review', 'approved', 'completed') then 1 else NULL end as started_count
        , case when status in ('ready_for_review', 'approved', 'completed') then 1 else NULL end as ready_for_review_count
        , case when status in ('approved', 'completed') then 1 else NULL end as approved_count
        , case when status in ('completed') then 1 else NULL end as completed_count
       
        -- Stitch
        , _sdc_table_version
        , _sdc_received_at
        , _sdc_sequence
        , _sdc_batched_at
    from {{ref('base_bq_questionnaires')}} r   
    where rn=1  -- grab only the most recent version based on stitch batched at of the questionnaire
)

select *
from questionnaires