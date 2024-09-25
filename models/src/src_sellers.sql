With source_data as (
    select parse_json(message_body) as raw_data from {{source("WELLBEFORE", "bbraw")}}
),
flatten_payload as(
    select raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::Array as doop

    from source_data 
)
select
 dummy.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
 dummy.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon,
 dummy.value:"SellerId"::STRING as SellerId
 from flatten_payload, lateral flatten(input=> flatten_payload.doop) as dummy