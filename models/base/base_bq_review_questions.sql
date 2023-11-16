with raw_source as (

    select *
    from {{ source('conveyor', 'review_questions') }}

)

, final as (

    select question_id
    , source_question_id
    , review_id
    , content as question_text
    , document_type
    , issue
    , issue_description
    , position as question_position
    , status as question_status
    , created_at
    , updated_at
    from raw_source

)

select * from final
