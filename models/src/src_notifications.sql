WITH source_data AS(
    SELECT parse_json(message_body) as raw_data
    from {{source("WELLBEFORE","bbraw")}}

),
flatten_payload as (
    select 
     raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::Array AS doop,
     raw_data:"NotificationMetadata"::Array as dup,
     raw_data:"NotificationVersion"::STRING as NotificationVersion,
     raw_data:"NotificationType"::STRING as NotificationType
     from source_data

)
select 
  ASIN
  offers.value:"NotificationId":: STRING as NotificationId,
  offers.value:"PublishTime":: STRING  as PublishTime,
  NotificationType,
  NotificationVersion,
  offers.value:"ApplicationId":: STRING as ApplicationId,
  offers.value:"SubscriptionId":: STRING as SubscriptionId
 from flatten_payload , lateral flatten(input=> flatten_payload.doop) AS offer,
 lateral flatten(input=> flatten_payload.dup) AS offers