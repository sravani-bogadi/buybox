WITH source_data AS (
    SELECT message_id, PARSE_JSON(MESSAGE_BODY) AS raw_data
    from {{source("WELLBEFORE","bbraw")}}
),
flatten_payload AS (
    SELECT
        message_id,
        raw_data:"EventTime"::STRING AS EventTime,
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::OBJECT:"ASIN"::STRING AS ASIN,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT 
        message_id,
        NotificationId,
        EventTime, 
        ASIN, 
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY NotificationId DESC) AS OfferID,
        offers.value:"SellerId"::STRING AS SellerId,
        offers.value:"IsBuyBoxWinner"::STRING AS IsBuyBoxWinner,
        offers.value:"IsFeaturedMerchant"::STRING AS IsFeaturedMerchant,
        offers.value:"ListingPrice"::OBJECT:"Amount"::FLOAT AS ListingPriceAmount,
        offers.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING AS ListingPriceCurrencyCode,
        offers.value:"PrimeInformation"::OBJECT:"IsOfferNationalPrime"::BOOLEAN AS IsOfferNationalPrime,
        offers.value:"PrimeInformation"::OBJECT:"IsOfferPrime"::BOOLEAN AS PrimeInformation,
        offers.value:"SubCondition"::STRING AS SubCondition,
        offers.value:"IsFulfilledByAmazon"::STRING AS IsFulfilledByAmazon,
        offers.value:"SellerFeedbackRating"::OBJECT:"FeedbackCount"::STRING AS FeedbackCount,
        offers.value:"SellerFeedbackRating"::OBJECT:"SellerPositiveFeedbackRating"::STRING AS SellerPositiveFeedbackRating
    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.offers) AS offers
),
ranking as(
    SELECT 
        message_id,
        NotificationId,
        EventTime, 
        ASIN, 
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY IsBuyBoxWinner DESC, NotificationId DESC) AS OfferID,
        SellerId,
        IsBuyBoxWinner,
        IsFeaturedMerchant,
        ListingPriceAmount,
        ListingPriceCurrencyCode,
        IsOfferNationalPrime,
        PrimeInformation,
        SubCondition,
        IsFulfilledByAmazon,
        FeedbackCount,
        SellerPositiveFeedbackRating
    FROM flatten_offers
)
SELECT
    OfferID,
    EventTime,
    ASIN,
    message_id,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    IsFulfilledByAmazon,
    IsFeaturedMerchant,
    SellerPositiveFeedbackRating,
    ListingPriceAmount,
    ListingPriceCurrencyCode
FROM ranking
GROUP BY
    OfferID,
    EventTime,
    ASIN,
    message_id,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    IsFulfilledByAmazon,
    IsFeaturedMerchant,
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    SellerPositiveFeedbackRating
ORDER BY 
    ASIN,              
    OfferID,
    ListingPriceAmount asc, SellerPositiveFeedbackRating desc,
    (IsBuyBoxWinner, IsFulfilledByAmazon) asc