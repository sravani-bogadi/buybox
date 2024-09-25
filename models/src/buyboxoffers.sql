WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"NumberOfOffers"::ARRAY AS doop,
     raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfBuyBoxEligibleOffers"::Array as dup
     from source_data

),
flatten_offers AS (
select 
  offers.value:"Condition":: STRING as EligibleOfferCondition,
  offers.value:"FulfillmentChannel"::STRING AS EligibleOfferFullfillmentChannel,        
  offers.value:"OfferCount"::FLOAT AS EligibleOfferCount,
  offer.value:"Condition"::STRING AS NumberOfferCondition,
  offer.value:"FulfillmentChannel"::STRING AS NumberOfferFullfillmentChannel,
  offer.value:"OfferCount"::STRING AS NumberOfferCount


 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer,
 lateral flatten(input=> flatten_payload.dup) AS offers
)
select
    EligibleOfferCondition,
    EligibleOfferFullfillmentChannel,
    EligibleOfferCount,
    NumberOfferCondition,
    NumberOfferFullfillmentChannel,
    NumberOfferCount
from flatten_offers