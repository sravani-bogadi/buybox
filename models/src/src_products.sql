
WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Array AS doop
     from source_data

)

select 
  offer.value:"ASIN"::STRING AS ASIN,
  offer.value:"MarketplaceId"::STRING AS MarketpacedID
 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer

