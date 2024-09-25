WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"offers"::Array AS doop,
     raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
     from source_data

)
select
  NotificationId,
  offer.value:"Shipping"::Object:"Amount"::STRING as Amount,
  offer.value:"Shipping"::Object:"CurrencyCode"::STRING as CurrencyCode,
  offer.value:"Shipping"::Object:"AvailabilityType"::STRING as AvailabilityType,
  offer.value:"Shipping"::Object:"AvailableDate"::STRING as AvailableDate,
  offer.value:"Shipping"::Object:"MaximumHours"::float as MaximumHours,
  offer.value:"Shipping"::Object:"MinimumHours"::float as MinimumHours,
  offer.value:"ShipsDomestically"::BOOLEAN as ShipsDomestically

 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer