WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger":Object:"ASIN"::STRING AS ASIN,
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::Array AS doop,
     raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
     from source_data

),
flatten_offers as(
select
  row_number() over(partition by ASIN order by NotificationId desc) as offerID,NotificationId,
  offer.value:"SellerId"::STRING as SellerId,
  offer.value:"IsBuyBoxWinner"::STRING as IsBuyBoxWinner,
  offer.value:"ListingPrice"::Object:"Amount" as Amount,
  offer.value:"ListingPrice"::Object:"CurrencyCode" as CurrencyCode,
  offer.value:"PrimeInformation"::Object:"IsOfferNationalPrime":: BOOLEAN as IsOfferNationalPrime,
  offer.value:"PrimeInformation"::Object:"IsOfferPrime":: BOOLEAN as IsOfferPrime,
  offer.value:"SubCondition"::STRING as SubCondition
 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer
)

select * from flatten_offers