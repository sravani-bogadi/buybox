WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Array AS doop,
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"SalesRankings"::Array AS dup
     from source_data
)
select 
  offer.value:"ASIN"::STRING AS ASIN,
  offers.value:"ProductCategoryId"::STRING AS ProductCategoryId,
  offers.value:"Rank"::INTEGER AS Rankdbt
 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer,
 lateral flatten(input=> flatten_payload.dup) AS offers