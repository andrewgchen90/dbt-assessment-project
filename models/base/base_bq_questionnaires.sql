with raw_source as (

    select *
    from {{ source('conveyor', 'questionnaires') }}

)

, final as (

    select due_at
        , source_type	
        , review_id	
        , id	
        , question_count
        , started_at
        , question_group_import_id
        , program_id
        , workflow_type
        , original_format
        , connection_id
        , currently_responsible
        , deleted_at
        , created_at
        , questions_completed
        , notes
        , sla_days
        , hours_spent
        , completed_at
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
        , row_number() over (partition by id order by _sdc_batched_at desc) rn
    from raw_source
)

select * 
from final
