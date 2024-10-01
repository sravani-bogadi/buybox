{{ config(
    materialized='table' 
) }}

With source_data as (
    select  parse_json(message_body) as raw_data from {{source("WELLBEFORE", "bbraw")}}
),
flatten_payload as(
    select raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::Array as doop,
    raw_data:"EventTime"::STRING AS EventTime

    from source_data 
)
select
row_number() over(order by(select null)) as id,
 dummy.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
 dummy.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon,
 dummy.value:"SellerId"::STRING as SellerId,
EventTime,
current_timestamp() as updated_at,
md5(concat(dummy.value:"SellerId",EventTime,IsFeaturedMerchant,IsFulfilledByAmazon,id)) as surrogate_key
 from flatten_payload, lateral flatten(input=> flatten_payload.doop) as dummy