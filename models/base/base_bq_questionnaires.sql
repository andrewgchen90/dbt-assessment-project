with raw_source as (

    select *
    from {{ source('conveyor', 'questionnaires') }}

)

, final as (

    select 
        id as questionnaire_id
        , created_at
        , started_at
        , completed_at
        , due_at
        , source_type	
        , review_id	
        , question_count
        , questions_completed -- Missing data 
        , hours_spent -- Missing data 
        , question_group_import_id
        , program_id
        , workflow_type
        , original_format
        , connection_id
        , currently_responsible
        , deleted_at
        , notes
        , sla_days
        , user_id
        , email	
        , sla_started_at
        , status
        , conveyor_done_at
        , position
        , updated_at        
        , _sdc_table_version
        , _sdc_received_at
        , _sdc_sequence
        , _sdc_batched_at
        -- Getting the recent data of the questionnaire to see how things are currently going
        , row_number() over (partition by id order by _sdc_batched_at desc) rn
    from raw_source
    -- Initially Considered filtering out deleted_at but chose to keep it since we wanted to see all-time data
    -- where deleted_at is null   
)

select * 
from final
