WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"BuyBoxPrices"::Array AS doop,
     raw_data:"NotificationMetadata"::Array as dup
     from source_data

)
select 
  offers.value:"NotificationId":: STRING as NotificationId,
  offer.value: "Shipping"::Object: "CurrencyCode":: STRING as CurrencyCode,
  offer.value: "Shipping"::Object: "Amount":: STRING as shippingamount,
  offer.value: "ListingPrice"::Object: "Amount":: STRING as listingpriceamount,
  offer.value: "ListingPrice"::Object: "CurrencyCode":: STRING as listingpricecurrencyCode,
  offer.value: "LandedPrice"::Object: "CurrencyCode":: STRING as landedpricecurrencyCode,
  offer.value: "LandedPrice"::Object: "Amount":: STRING as landedpriceamount,
  offer.value: "ListPrice"::Object: "Amount":: STRING as listpriceamount,


 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer,
 lateral flatten(input=> flatten_payload.dup) AS offers

