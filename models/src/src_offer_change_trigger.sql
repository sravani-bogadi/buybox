
WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger":Object:"ASIN"::STRING AS ASIN,
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Object:"ItemCondition"::STRING AS ItemCondition,
     raw_data:"NotificationMetadata"::Object:"PublishTime"::STRING as PublishTime,
     from source_data

)

select 
  *
 from flatten_payload

