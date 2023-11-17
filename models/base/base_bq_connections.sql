with raw_source as (

    select *
    from {{ source('conveyor', 'connections') }}

)

, final as (

    select 
        id as connection_id
        , domain
        , dataroom_id
        , crm_id	
        , updated_at
        , first_nda_signed_at
        , created_at
        , crm_link
        , automated_nda_bypass
        , bypass_justification
        , auto_approve_requests
        , _sdc_table_version   
        , _sdc_batched_at             
        , _sdc_received_at
        , _sdc_sequence        
        , row_number() over (partition by id order by _sdc_batched_at desc) rn
    from raw_source

)

select * from final
