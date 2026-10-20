use super::*;
use crate::common::test_utils::helpers::{
    assert_proto_msg_id, assert_request, assert_request_msg_id, count_proto_msgs, request_message_count, TEST_REQ_ID_FIRST,
};
use crate::contracts::{Contract, Currency, Exchange, SecurityType, Symbol};
use crate::market_data::historical::TickLast;
use crate::market_data::IgnoreSize;
use crate::messages::OutgoingMessages;
use crate::protocol::{Features, ProtocolFeature};
use crate::server_versions;
use crate::stubs::MessageBusStub;
use crate::subscriptions::common::RoutedItem;
use crate::subscriptions::SubscriptionItem;
use crate::testdata::builders::market_data::{head_timestamp_request, histogram_data_request, historical_data_request, historical_ticks_request};
use futures::StreamExt;
use std::sync::Arc;
use std::sync::RwLock;
use time::macros::datetime;

fn test_contract() -> Contract {
    Contract {
        symbol: Symbol::from("GBL"),
        security_type: SecurityType::Future,
        exchange: Exchange::from("EUREX"),
        currency: Currency::from("EUR"),
        last_trade_date_or_contract_month: "202303".to_owned(),
        ..Contract::default()
    }
}

// Pins server_version one below `feature.min_version` and asserts the call fails
// with the feature name in the error. Custom helper (vs. `.expect_err`) because
// subscription return types don't implement Debug.
async fn assert_version_check_fails<F, Fut, T>(feature: ProtocolFeature, call: F)
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus, feature.min_version - 1);
    let Err(err) = call(client).await else {
        panic!("expected version-check failure ({})", feature.name);
    };
    assert!(err.to_string().contains(feature.name), "expected '{}', got: {err}", feature.name);
}

// Feeds a single wire-format response, asserts the call fails with
// `Error::UnexpectedResponse`. Pairs with `assert_version_check_fails`.
async fn assert_unexpected_response<F, Fut, T>(server_version: i32, response: &str, call: F)
where
    F: FnOnce(Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let message_bus = Arc::new(MessageBusStub::with_responses(vec![response.to_owned()]));
    let client = Client::stubbed(message_bus, server_version);
    let Err(err) = call(client).await else {
        panic!("expected UnexpectedResponse failure");
    };
    assert!(matches!(err, Error::UnexpectedResponse(_)), "expected UnexpectedResponse, got: {err:?}");
}

#[tokio::test]
async fn test_head_timestamp() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec!["88|9000|1678838400|".to_owned()], // 2023-03-15 00:00:00 UTC,
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::BOND_ISSUERID);
    let contract = test_contract();
    let what_to_show = WhatToShow::Trades;
    let trading_hours = TradingHours::Regular;

    let result = client.head_timestamp(&contract, what_to_show, trading_hours).await;
    assert!(result.is_ok(), "head_timestamp should succeed");

    let timestamp = result.unwrap();
    assert_eq!(timestamp, datetime!(2023-03-15 00:00:00 UTC), "Wrong timestamp");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &head_timestamp_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .what_to_show(what_to_show)
            .use_rth(trading_hours.use_rth()),
    );
}

#[tokio::test]
async fn test_histogram_data() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec!["96|9000|3|185.50|100|185.75|150|186.00|200|".to_owned()],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::REQ_HISTOGRAM);
    let contract = test_contract();
    let trading_hours = TradingHours::Regular;
    let period = BarSize::Day;

    let result = client.histogram_data(&contract, trading_hours, period).await;
    assert!(result.is_ok(), "histogram_data should succeed");

    let entries = result.unwrap();
    assert_eq!(entries.len(), 3, "Should receive 3 histogram entries");

    // Verify first entry
    assert_eq!(entries[0].price, 185.50, "Wrong price for first entry");
    assert_eq!(entries[0].size, 100, "Wrong size for first entry");

    // Verify second entry
    assert_eq!(entries[1].price, 185.75, "Wrong price for second entry");
    assert_eq!(entries[1].size, 150, "Wrong size for second entry");

    // Verify third entry
    assert_eq!(entries[2].price, 186.00, "Wrong price for third entry");
    assert_eq!(entries[2].size, 200, "Wrong size for third entry");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &histogram_data_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .use_rth(trading_hours.use_rth())
            .period(period),
    );
}

#[tokio::test]
async fn test_historical_data() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            "17|9000|20230315  09:30:00|20230315  10:30:00|2|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|1678890000|185.75|186.25|185.50|186.00|1500|185.85|150|"
                .to_owned(),
        ],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);
    // Set client timezone for test
    client.time_zone = Some(time_tz::timezones::db::UTC);

    let contract = test_contract();
    let end_date = datetime!(2023-03-15 16:00:00 UTC);
    let duration = Duration::seconds(3600);
    let bar_size = BarSize::Min30;
    let what_to_show = WhatToShow::Trades;

    let result = client
        .historical_data(&contract, bar_size)
        .what_to_show(what_to_show)
        .duration(duration)
        .ending(end_date)
        .fetch()
        .await;
    assert!(result.is_ok(), "historical_data should succeed");

    let data = result.unwrap();
    assert_eq!(data.bars.len(), 2, "Should receive 2 bars");

    // Verify first bar
    let bar = &data.bars[0];
    // 1678886400 = 2023-03-15 13:20:00 UTC
    assert_eq!(bar.date, datetime!(2023-03-15 13:20:00 UTC), "Wrong date for first bar");
    assert_eq!(bar.open, Some(185.50), "Wrong open for first bar");
    assert_eq!(bar.high, Some(186.00), "Wrong high for first bar");
    assert_eq!(bar.low, Some(185.25), "Wrong low for first bar");
    assert_eq!(bar.close, Some(185.75), "Wrong close for first bar");
    assert_eq!(bar.volume, Some(1000.0), "Wrong volume for first bar");
    assert_eq!(bar.wap, Some(185.70), "Wrong WAP for first bar");
    assert_eq!(bar.count, Some(100), "Wrong count for first bar");

    // Verify second bar
    let bar = &data.bars[1];
    // 1678890000 = 2023-03-15 14:20:00 UTC
    assert_eq!(bar.date, datetime!(2023-03-15 14:20:00 UTC), "Wrong date for second bar");
    assert_eq!(bar.open, Some(185.75), "Wrong open for second bar");
    assert_eq!(bar.high, Some(186.25), "Wrong high for second bar");
    assert_eq!(bar.low, Some(185.50), "Wrong low for second bar");
    assert_eq!(bar.close, Some(186.00), "Wrong close for second bar");
    assert_eq!(bar.volume, Some(1500.0), "Wrong volume for second bar");
    assert_eq!(bar.wap, Some(185.85), "Wrong WAP for second bar");
    assert_eq!(bar.count, Some(150), "Wrong count for second bar");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_data_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .end_date(Some(end_date))
            .duration(duration)
            .bar_size(bar_size)
            .what_to_show(Some(what_to_show))
            .use_rth(true),
    );
}

#[tokio::test]
async fn test_historical_data_version_check() {
    let mut contract = test_contract();
    contract.trading_class = "ES".to_owned();
    assert_version_check_fails(Features::TRADING_CLASS, |c| async move {
        c.historical_data(&contract, BarSize::Hour).duration(Duration::days(1)).fetch().await
    })
    .await;
}

#[tokio::test]
async fn test_historical_data_adjusted_last_validation() {
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus, server_versions::SIZE_RULES);

    let contract = Contract::stock("AAPL").build();
    let end_date = datetime!(2023-03-15 16:00:00 UTC);

    let result = client
        .historical_data(&contract, BarSize::Day)
        .what_to_show(WhatToShow::AdjustedLast)
        .duration(Duration::days(1))
        .ending(end_date)
        .fetch()
        .await;

    assert!(result.is_err(), "Should fail when end_date is provided with AdjustedLast");
    assert!(
        result.unwrap_err().to_string().contains("end_date must be None"),
        "Error should mention end_date restriction"
    );
}

#[tokio::test]
async fn test_historical_data_error_response() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec!["4|2|9000|162|Historical Market Data Service error message:No market data permissions.|".to_owned()],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus, server_versions::SIZE_RULES);
    let contract = test_contract();

    let result = client.historical_data(&contract, BarSize::Hour).duration(Duration::days(1)).fetch().await;
    assert!(result.is_err(), "Should fail with error response");
    assert!(
        result.unwrap_err().to_string().contains("No market data permissions"),
        "Error should contain the error message"
    );
}

#[tokio::test]
async fn test_historical_data_unexpected_response() {
    // 1 = TickPrice — wrong type for historical_data.
    assert_unexpected_response(server_versions::SIZE_RULES, "1|2|9000|1|185.50|100|7|", |c| async move {
        c.historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .fetch()
            .await
    })
    .await;
}

#[tokio::test]
async fn test_historical_schedules() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            "106|9000|20230313-09:30:00|20230315-16:00:00|UTC|3|20230313-09:30:00|20230313-16:00:00|20230313|20230314-09:30:00|20230314-16:00:00|20230314|20230315-09:30:00|20230315-16:00:00|20230315|"
                .to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::BOND_ISSUERID);
    let contract = Contract::stock("AAPL").build();
    let end_date = datetime!(2023-03-15 16:00:00 UTC);
    let duration = Duration::days(3);

    let result = client.historical_schedules(&contract, duration).ending(end_date).fetch().await;
    assert!(result.is_ok(), "historical_schedules should succeed");

    let schedule = result.unwrap();
    assert_eq!(schedule.time_zone, "UTC", "Wrong time zone");
    // Check that we have sessions
    assert!(!schedule.sessions.is_empty(), "Should have at least 1 session");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_data_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .end_date(Some(end_date))
            .duration(duration)
            .bar_size(BarSize::Day)
            .what_to_show(Some(WhatToShow::Schedule))
            .use_rth(true),
    );
}

#[tokio::test]
async fn test_tick_subscription_methods() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // HistoricalTickBidAsk = 97
            // First response with 2 ticks, not done
            // Format: message_type|request_id|num_ticks|timestamp|mask|bid|ask|bid_size|ask_size|...|done
            "97|9000|2|1678838400|10|185.50|186.00|100|200|1678838401|11|185.55|186.05|105|205|0|".to_owned(),
            // Second response with 1 tick, done
            "97|9000|1|1678838500|10|185.75|186.25|150|250|1|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus, server_versions::HISTORICAL_TICKS);
    let contract = test_contract();

    let mut subscription = client
        .historical_ticks(&contract, 3)
        .bid_ask(IgnoreSize::No)
        .await
        .expect("Failed to create tick subscription");

    // Get first tick
    let tick1 = subscription.next().await;
    assert!(tick1.is_some(), "Should receive first tick");
    let tick1 = tick1.unwrap();
    assert_eq!(tick1.price_bid, 185.50, "Wrong bid price for first tick");
    assert_eq!(tick1.price_ask, 186.00, "Wrong ask price for first tick");
    assert_eq!(tick1.size_bid, 100, "Wrong bid size for first tick");
    assert_eq!(tick1.size_ask, 200, "Wrong ask size for first tick");
    assert!(tick1.tick_attribute_bid_ask.bid_past_low, "Wrong bid past low for first tick");
    assert!(!tick1.tick_attribute_bid_ask.ask_past_high, "Wrong ask past high for first tick");

    // Get second tick
    let tick2 = subscription.next().await;
    assert!(tick2.is_some(), "Should receive second tick");
    let tick2 = tick2.unwrap();
    assert_eq!(tick2.price_bid, 185.55, "Wrong bid price for second tick");
    assert_eq!(tick2.price_ask, 186.05, "Wrong ask price for second tick");

    // Get third tick
    let tick3 = subscription.next().await;
    assert!(tick3.is_some(), "Should receive third tick");
    let tick3 = tick3.unwrap();
    assert_eq!(tick3.price_bid, 185.75, "Wrong bid price for third tick");

    // Should be done now
    let tick4 = subscription.next().await;
    assert!(tick4.is_none(), "Should not receive more ticks after done");
}

#[tokio::test]
async fn test_tick_subscription_buffer_and_iteration() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // HistoricalTickBidAsk = 97
            // Response with 3 ticks at once, done = true
            "97|9000|3|1678838400|8|185.50|186.00|100|200|1678838401|9|185.60|186.10|110|210|1678838402|10|185.70|186.20|120|220|1|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus, server_versions::HISTORICAL_TICKS);
    let contract = test_contract();

    let mut subscription = client
        .historical_ticks(&contract, 3)
        .bid_ask(IgnoreSize::No)
        .await
        .expect("Failed to create tick subscription");

    // Should receive all 3 ticks from buffer
    let mut ticks = Vec::new();
    while let Some(tick) = subscription.next().await {
        ticks.push(tick);
    }

    assert_eq!(ticks.len(), 3, "Should receive exactly 3 ticks");
    assert_eq!(ticks[0].price_bid, 185.50, "Wrong bid price for first tick");
    assert_eq!(ticks[1].price_bid, 185.60, "Wrong bid price for second tick");
    assert_eq!(ticks[2].price_bid, 185.70, "Wrong bid price for third tick");
}

#[tokio::test]
async fn test_tick_subscription_bid_ask() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // HistoricalTickBidAsk = 97
            // mask = 2 (binary 10) = bid_past_low = true, ask_past_high = false
            "97|9000|1|1678838400|2|185.50|186.00|100|200|1|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::HISTORICAL_TICKS);
    let contract = test_contract();
    let start = datetime!(2023-03-15 09:00:00 UTC);
    let end = datetime!(2023-03-15 10:00:00 UTC);
    let number_of_ticks = 1;
    let trading_hours = TradingHours::Regular;

    let mut subscription = client
        .historical_ticks(&contract, number_of_ticks)
        .starting(start)
        .ending(end)
        .trading_hours(trading_hours)
        .bid_ask(IgnoreSize::No)
        .await
        .expect("Failed to create bid/ask tick subscription");

    let tick = subscription.next().await.expect("Should receive a tick");
    assert_eq!(tick.timestamp, datetime!(2023-03-15 00:00:00 UTC), "Wrong timestamp");
    assert_eq!(tick.price_bid, 185.50, "Wrong bid price");
    assert_eq!(tick.price_ask, 186.00, "Wrong ask price");
    assert_eq!(tick.size_bid, 100, "Wrong bid size");
    assert_eq!(tick.size_ask, 200, "Wrong ask size");
    assert!(tick.tick_attribute_bid_ask.bid_past_low, "Wrong bid past low");
    assert!(!tick.tick_attribute_bid_ask.ask_past_high, "Wrong ask past high");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_ticks_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .start(Some(start))
            .end(Some(end))
            .number_of_ticks(number_of_ticks)
            .what_to_show(WhatToShow::BidAsk)
            .use_rth(trading_hours.use_rth())
            .ignore_size(false),
    );
}

#[tokio::test]
async fn test_tick_subscription_midpoint() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // HistoricalTick = 96 (for midpoint)
            // Format: message_type|request_id|num_ticks|timestamp|skip|price|size|...|done
            "96|9000|1|1678838400|0|185.75|100|1|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::HISTORICAL_TICKS);
    let contract = test_contract();

    let mut subscription = client
        .historical_ticks(&contract, 1)
        .mid_point()
        .await
        .expect("Failed to create midpoint tick subscription");

    let tick = subscription.next().await.expect("Should receive a tick");
    assert_eq!(tick.timestamp, datetime!(2023-03-15 00:00:00 UTC), "Wrong timestamp");
    assert_eq!(tick.price, 185.75, "Wrong midpoint price");
    assert_eq!(tick.size, 100, "Wrong size");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_ticks_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .number_of_ticks(1)
            .what_to_show(WhatToShow::MidPoint)
            .use_rth(true),
    );
}

#[tokio::test]
async fn test_historical_ticks_trade() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // HistoricalTickLast = 98
            // Format: message_type|request_id|num_ticks|timestamp|mask|price|size|exchange|conditions|...|done
            "98|9000|1|1678838400|0|185.50|100|ISLAND|APR|1|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let client = Client::stubbed(message_bus.clone(), server_versions::HISTORICAL_TICKS);
    let contract = test_contract();

    let mut subscription = client
        .historical_ticks(&contract, 1)
        .trade()
        .await
        .expect("Failed to create trade tick subscription");

    let tick = subscription.next().await.expect("Should receive a tick");
    assert_eq!(tick.timestamp, datetime!(2023-03-15 00:00:00 UTC), "Wrong timestamp");
    assert_eq!(tick.price, 185.50, "Wrong trade price");
    assert_eq!(tick.size, 100, "Wrong trade size");
    assert_eq!(tick.exchange, "ISLAND", "Wrong exchange");
    assert_eq!(tick.special_conditions, "APR", "Wrong special conditions");
    assert!(!tick.tick_attribute_last.past_limit, "Wrong past limit");
    assert!(!tick.tick_attribute_last.unreported, "Wrong unreported");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_ticks_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .number_of_ticks(1)
            .what_to_show(WhatToShow::Trades)
            .use_rth(true),
    );
}

#[tokio::test]
async fn test_historical_data_time_zone_handling() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec!["17|9000|20230315  09:30:00|20230315  10:30:00|1|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|".to_owned()],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus, server_versions::SIZE_RULES);
    // Set client timezone to Eastern
    client.time_zone = Some(time_tz::timezones::db::america::NEW_YORK);

    let contract = test_contract();
    let result = client
        .historical_data(&contract, BarSize::Hour)
        .duration(Duration::seconds(3600))
        .fetch()
        .await;

    assert!(result.is_ok(), "historical_data should succeed with timezone");
    let data = result.unwrap();
    assert_eq!(data.bars.len(), 1, "Should receive 1 bar");

    // The timestamp should be parsed in the client's timezone
    // 1678886400 = 2023-03-15 12:00:00 UTC = 2023-03-15 08:00:00 EDT
    let bar = &data.bars[0];
    assert_eq!(bar.date.unix_timestamp(), 1678886400, "Timestamp should match");
}

#[tokio::test]
async fn test_time_zone_fallback() {
    let mut client = Client::stubbed(Arc::new(MessageBusStub::default()), server_versions::SIZE_RULES);
    // Client without timezone set
    client.time_zone = None;

    let tz = time_zone(&client);
    assert_eq!(tz, time_tz::timezones::db::UTC, "Should fallback to UTC when timezone not set");
}

#[tokio::test]
async fn test_historical_data_streaming_with_updates() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // Initial historical data (message type 17)
            "17|9000|20230315  09:30:00|20230315  10:30:00|1|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|".to_owned(),
            // Streaming update (message type 90)
            "90|9000|-1|1678890000|185.80|186.10|185.60|185.90|500|185.85|50|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);
    client.time_zone = Some(time_tz::timezones::db::UTC);

    let contract = Contract::stock("SPY").build();

    let mut subscription = client
        .historical_data(&contract, BarSize::Hour)
        .duration(Duration::days(1))
        .stream()
        .await
        .expect("streaming request should succeed");

    // First: receive initial historical data
    let Some(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Historical(data)))) = subscription.next().await else {
        panic!("Expected Historical variant");
    };
    assert_eq!(data.bars.len(), 1, "Should have 1 initial bar");
    assert_eq!(data.bars[0].open, Some(185.50), "Wrong open price");

    // Second: receive streaming update
    let Some(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Update(bar)))) = subscription.next().await else {
        panic!("Expected Update variant");
    };
    assert_eq!(bar.open, Some(185.80), "Wrong open price in update");
    assert_eq!(bar.high, Some(186.10), "Wrong high price in update");
    assert_eq!(bar.close, Some(185.90), "Wrong close price in update");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request(
        &message_bus,
        0,
        &historical_data_request()
            .request_id(TEST_REQ_ID_FIRST)
            .contract(&contract)
            .duration(Duration::days(1))
            .bar_size(BarSize::Hour)
            .what_to_show(Some(WhatToShow::Trades))
            .use_rth(true)
            .keep_up_to_date(true),
    );
}

#[tokio::test]
async fn test_historical_data_streaming_error_response() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![
            // Error response
            "4|2|9000|162|Historical Market Data Service error message:No market data permissions.|".to_owned(),
        ],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus, server_versions::SIZE_RULES);
    client.time_zone = Some(time_tz::timezones::db::UTC);

    let contract = Contract::stock("SPY").build();

    let mut subscription = client
        .historical_data(&contract, BarSize::Hour)
        .duration(Duration::days(1))
        .stream()
        .await
        .expect("streaming request should succeed");

    // Should yield Some(Err(_)) — Subscription<T> surfaces errors through next().
    let Some(Err(err)) = subscription.next().await else {
        panic!("error should arrive as Some(Err(_))");
    };
    assert!(err.to_string().contains("No market data permissions"), "Error should contain the message");
}

#[tokio::test]
async fn test_tick_subscription_sends_cancel_on_drop() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![],
        ordered_responses: vec![],
    });

    let (_tx, rx) = tokio::sync::broadcast::channel(16);
    let internal = AsyncInternalSubscription::new(rx);

    {
        let _subscription: TickSubscription<TickLast> = TickSubscription::new(internal, 9100, message_bus.clone());
        // dropped here, !done so cancel should fire
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(messages.len(), 1, "should send cancel message on drop");
    assert_proto_msg_id(&messages[0], OutgoingMessages::CancelHistoricalTicks);
}

#[tokio::test]
async fn test_tick_subscription_explicit_cancel_prevents_duplicate_on_drop() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![],
        ordered_responses: vec![],
    });

    let (_tx, rx) = tokio::sync::broadcast::channel(16);
    let internal = AsyncInternalSubscription::new(rx);

    {
        let subscription: TickSubscription<TickLast> = TickSubscription::new(internal, 9101, message_bus.clone());
        subscription.cancel().await;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(messages.len(), 1, "should send cancel only once");
}

#[tokio::test]
async fn test_tick_subscription_drop_after_done_does_not_cancel() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![],
        ordered_responses: vec![],
    });

    let (_tx, rx) = tokio::sync::broadcast::channel(16);
    let internal = AsyncInternalSubscription::new(rx);

    {
        let mut subscription: TickSubscription<TickLast> = TickSubscription::new(internal, 9102, message_bus.clone());
        subscription.done = true;
        // drop with done=true → no cancel
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(messages.len(), 0, "completed subscription should not send cancel on drop");
}

#[tokio::test]
async fn test_streaming_subscription_sends_cancel_on_drop() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);
    client.time_zone = Some(time_tz::timezones::db::UTC);
    let contract = Contract::stock("SPY").build();

    {
        let _subscription = client
            .historical_data(&contract, BarSize::Hour)
            .duration(Duration::days(1))
            .stream()
            .await
            .expect("streaming request should succeed");
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(
        count_proto_msgs(&messages, OutgoingMessages::CancelHistoricalData),
        1,
        "should send exactly one cancel message on drop"
    );
}

#[tokio::test]
async fn test_streaming_subscription_cancel_prevents_duplicate_on_drop() {
    let message_bus = Arc::new(MessageBusStub {
        request_messages: RwLock::new(vec![]),
        response_messages: vec![],
        ordered_responses: vec![],
    });

    let mut client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);
    client.time_zone = Some(time_tz::timezones::db::UTC);
    let contract = Contract::stock("SPY").build();

    {
        let subscription = client
            .historical_data(&contract, BarSize::Hour)
            .duration(Duration::days(1))
            .stream()
            .await
            .expect("streaming request should succeed");

        subscription.cancel().await;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(
        count_proto_msgs(&messages, OutgoingMessages::CancelHistoricalData),
        1,
        "should send cancel only once"
    );
}

#[tokio::test]
async fn test_head_timestamp_version_check() {
    assert_version_check_fails(Features::HEAD_TIMESTAMP, |c| async move {
        c.head_timestamp(&test_contract(), WhatToShow::Trades, TradingHours::Regular).await
    })
    .await;
}

#[tokio::test]
async fn test_head_timestamp_unexpected_response() {
    // 17 = HistoricalData — wrong type for head_timestamp.
    assert_unexpected_response(
        server_versions::BOND_ISSUERID,
        "17|9000|20230315  09:30:00|20230315  10:30:00|0|",
        |c| async move { c.head_timestamp(&test_contract(), WhatToShow::Trades, TradingHours::Regular).await },
    )
    .await;
}

#[tokio::test]
async fn test_historical_data_with_end_message() {
    // server_version >= HISTORICAL_DATA_END (196): start/end live on a follow-on
    // HistoricalDataEnd (108), not the HistoricalData (17) frame itself.
    let message_bus = Arc::new(MessageBusStub::with_responses(vec![
        "17|9000|1|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|".to_owned(),
        "108|9000|20230315 09:30:00 UTC|20230315 10:30:00 UTC|".to_owned(),
    ]));

    let mut client = Client::stubbed(message_bus, server_versions::HISTORICAL_DATA_END);
    client.time_zone = Some(time_tz::timezones::db::UTC);

    let data = client
        .historical_data(&test_contract(), BarSize::Hour)
        .duration(Duration::days(1))
        .fetch()
        .await
        .expect("historical_data should succeed");

    assert_eq!(data.bars.len(), 1, "should have one bar");
    assert_eq!(data.start, Some(datetime!(2023-03-15 09:30:00 UTC)), "start populated from end-message");
    assert_eq!(data.end, Some(datetime!(2023-03-15 10:30:00 UTC)), "end populated from end-message");
}

#[tokio::test]
async fn test_historical_data_interrupted_is_not_retried() {
    // Empty responses → broadcast channel closes immediately. The single
    // attempt ends Interrupted with its request id, and nothing else is sent
    // (this sent five requests under five ids before).
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);

    let result = client
        .historical_data(&test_contract(), BarSize::Hour)
        .duration(Duration::days(1))
        .fetch()
        .await;

    assert!(
        matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
        "expected Interrupted, got {result:?}"
    );
    assert_eq!(request_message_count(&message_bus), 1, "an interrupted request is not sent again");
}

#[tokio::test]
async fn test_head_timestamp_interrupted_is_not_retried() {
    // This recursed into a new request, without bound, on every closed channel.
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus.clone(), server_versions::BOND_ISSUERID);

    let result = client.head_timestamp(&test_contract(), WhatToShow::Trades, TradingHours::Regular).await;

    assert!(
        matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
        "expected Interrupted, got {result:?}"
    );
    assert_eq!(request_message_count(&message_bus), 1, "an interrupted request is not sent again");
}

#[tokio::test]
async fn test_histogram_data_interrupted_is_not_retried() {
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus.clone(), server_versions::SIZE_RULES);

    let result = client.histogram_data(&test_contract(), TradingHours::Regular, BarSize::Day).await;

    assert!(
        matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
        "expected Interrupted, got {result:?}"
    );
    assert_eq!(request_message_count(&message_bus), 1, "an interrupted request is not sent again");
}

#[tokio::test]
async fn test_historical_schedules_interrupted_is_not_retried() {
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus.clone(), server_versions::HISTORICAL_SCHEDULE);

    let result = client.historical_schedules(&test_contract(), Duration::days(1)).fetch().await;

    assert!(
        matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
        "expected Interrupted, got {result:?}"
    );
    assert_eq!(request_message_count(&message_bus), 1, "an interrupted request is not sent again");
}

#[tokio::test]
async fn test_historical_schedules_ending_now_version_check() {
    assert_version_check_fails(Features::HISTORICAL_SCHEDULE, |c| async move {
        c.historical_schedules(&test_contract(), Duration::days(1)).fetch().await
    })
    .await;
}

#[tokio::test]
async fn test_historical_schedules_ending_now_trading_class_version_check() {
    // contract.trading_class triggers the earlier TRADING_CLASS gate ahead of the
    // HISTORICAL_SCHEDULE gate — pin below TRADING_CLASS so the former fires first.
    let mut contract = test_contract();
    contract.trading_class = "ES".to_owned();
    assert_version_check_fails(Features::TRADING_CLASS, |c| async move {
        c.historical_schedules(&contract, Duration::days(1)).fetch().await
    })
    .await;
}

#[tokio::test]
async fn test_historical_schedules_unexpected_response() {
    // 17 = HistoricalData — wrong type for the schedule decoder (expects 106).
    assert_unexpected_response(
        server_versions::BOND_ISSUERID,
        "17|9000|20230315  09:30:00|20230315  10:30:00|0|",
        |c| async move { c.historical_schedules(&test_contract(), Duration::days(3)).fetch().await },
    )
    .await;
}

#[tokio::test]
async fn test_historical_ticks_bid_ask_version_check() {
    assert_version_check_fails(Features::HISTORICAL_TICKS, |c| async move {
        c.historical_ticks(&test_contract(), 1).bid_ask(IgnoreSize::No).await
    })
    .await;
}

#[tokio::test]
async fn test_historical_ticks_mid_point_version_check() {
    assert_version_check_fails(Features::HISTORICAL_TICKS, |c| async move {
        c.historical_ticks(&test_contract(), 1).mid_point().await
    })
    .await;
}

#[tokio::test]
async fn test_historical_ticks_trade_version_check() {
    assert_version_check_fails(Features::HISTORICAL_TICKS, |c| async move {
        c.historical_ticks(&test_contract(), 1).trade().await
    })
    .await;
}

#[tokio::test]
async fn test_cancel_historical_ticks() {
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus.clone(), server_versions::CANCEL_CONTRACT_DATA);

    client.cancel_historical_ticks(9000).await.expect("cancel should succeed");

    assert_eq!(request_message_count(&message_bus), 1);
    assert_request_msg_id(&message_bus, 0, OutgoingMessages::CancelHistoricalTicks);
}

#[tokio::test]
async fn test_cancel_historical_ticks_version_check() {
    assert_version_check_fails(Features::CANCEL_CONTRACT_DATA, |c| async move { c.cancel_historical_ticks(9000).await }).await;
}

#[tokio::test]
async fn test_histogram_data_version_check() {
    assert_version_check_fails(Features::HISTOGRAM, |c| async move {
        c.histogram_data(&test_contract(), TradingHours::Regular, BarSize::Day).await
    })
    .await;
}

#[tokio::test]
async fn test_historical_data_streaming_trading_class_version_check() {
    let mut contract = test_contract();
    contract.trading_class = "ES".to_owned();
    assert_version_check_fails(Features::TRADING_CLASS, |c| async move {
        c.historical_data(&contract, BarSize::Hour).duration(Duration::days(1)).stream().await
    })
    .await;
}

#[tokio::test]
async fn test_tick_subscription_cancel_idempotent() {
    let message_bus = Arc::new(MessageBusStub::default());

    let (_tx, rx) = tokio::sync::broadcast::channel(16);
    let internal = AsyncInternalSubscription::new(rx);

    let subscription: TickSubscription<TickLast> = TickSubscription::new(internal, 9200, message_bus.clone());
    subscription.cancel().await;
    subscription.cancel().await;

    drop(subscription);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let messages = message_bus.request_messages.read().unwrap();
    assert_eq!(messages.len(), 1, "cancel + cancel + drop should send exactly one message");
}

#[tokio::test]
async fn test_tick_subscription_skips_unexpected_message_then_yields() {
    let message_bus = Arc::new(MessageBusStub::with_responses(vec![
        // 17 = HistoricalData — unexpected for a tick subscription, should be skipped.
        "17|9000|20230315  09:30:00|20230315  10:30:00|0|".to_owned(),
        // 98 = HistoricalTickLast — one tick, done=1.
        "98|9000|1|1678838400|0|185.50|100|ISLAND|APR|1|".to_owned(),
    ]));

    let client = Client::stubbed(message_bus, server_versions::HISTORICAL_TICKS);

    let mut subscription = client
        .historical_ticks(&test_contract(), 1)
        .trade()
        .await
        .expect("subscription should be created");

    let tick = subscription.next().await.expect("should receive tick after skipping unexpected");
    assert_eq!(tick.price, 185.50, "wrong price");
    assert!(subscription.next().await.is_none(), "should be done");
}

#[tokio::test]
async fn test_tick_subscription_errors_terminate_stream() {
    let message_bus = Arc::new(MessageBusStub::default());

    let (tx, rx) = tokio::sync::broadcast::channel(16);
    tx.send(RoutedItem::Error(Error::ConnectionReset)).unwrap();
    drop(tx);

    let internal = AsyncInternalSubscription::new(rx);
    let mut subscription: TickSubscription<TickLast> = TickSubscription::new(internal, 9300, message_bus);

    assert!(subscription.next().await.is_none(), "error on channel terminates next()");
}

#[tokio::test]
async fn test_tick_subscription_returns_none_on_closed_channel() {
    // Empty response_messages closes the broadcast channel immediately; the first
    // fill_buffer sees None and next() returns None.
    let message_bus = Arc::new(MessageBusStub::default());
    let client = Client::stubbed(message_bus, server_versions::HISTORICAL_TICKS);

    let mut subscription = client
        .historical_ticks(&test_contract(), 1)
        .mid_point()
        .await
        .expect("subscription should be created");

    assert!(subscription.next().await.is_none(), "closed channel yields None");
}

// === D14 item 1: the end marker is required ===

mod end_marker {
    use super::*;
    use crate::common::test_utils::helpers::proto_response;
    use crate::messages::IncomingMessages;
    use crate::testdata::builders::market_data::{full_historical_bar, HistoricalDataEndResponse, HistoricalDataResponse};
    use crate::testdata::builders::ResponseProtoEncoder;

    async fn fetch(message_bus: Arc<MessageBusStub>, server_version: i32) -> Result<HistoricalData, Error> {
        let mut client = Client::stubbed(message_bus, server_version);
        client.time_zone = Some(time_tz::timezones::db::UTC);
        client
            .historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .fetch()
            .await
    }

    fn proto_data() -> crate::messages::ResponseMessage {
        proto_response(
            IncomingMessages::HistoricalData,
            HistoricalDataResponse::default().bar(full_historical_bar()).encode_proto(),
        )
    }

    fn proto_end() -> crate::messages::ResponseMessage {
        proto_response(IncomingMessages::HistoricalDataEnd, HistoricalDataEndResponse::default().encode_proto())
    }

    #[tokio::test]
    async fn text_answer_without_end_marker_is_interrupted() {
        // The bars arrive, then the channel closes before HistoricalDataEnd.
        // This returned Ok(bars) with a window of now_utc() before.
        let message_bus = Arc::new(MessageBusStub::with_responses(vec![
            "17|9000|1|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|".to_owned(),
        ]));

        let result = fetch(message_bus.clone(), server_versions::HISTORICAL_DATA_END).await;

        assert!(
            matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
            "expected Interrupted, got {result:?}"
        );
        assert_eq!(request_message_count(&message_bus), 1);
    }

    #[tokio::test]
    async fn proto_answer_without_end_marker_is_interrupted() {
        let message_bus = Arc::new(MessageBusStub::with_ordered_responses(vec![proto_data()]));

        let result = fetch(message_bus.clone(), server_versions::PROTOBUF_HISTORICAL_DATA).await;

        assert!(
            matches!(result, Err(Error::Interrupted(TEST_REQ_ID_FIRST))),
            "expected Interrupted, got {result:?}"
        );
        assert_eq!(request_message_count(&message_bus), 1);
    }

    #[tokio::test]
    async fn proto_answer_with_end_marker_is_complete_with_ibs_window() {
        // Negative control for the two above: the same answer with its marker.
        let message_bus = Arc::new(MessageBusStub::with_ordered_responses(vec![proto_data(), proto_end()]));

        let data = fetch(message_bus, server_versions::PROTOBUF_HISTORICAL_DATA)
            .await
            .expect("complete answer");

        assert_eq!(data.bars.len(), 1);
        assert_eq!(data.bars[0].close, Some(185.75));
        assert_eq!(data.start, Some(datetime!(2023-03-15 09:30:00 UTC)), "start from the end marker");
        assert_eq!(data.end, Some(datetime!(2023-03-15 10:30:00 UTC)), "end from the end marker");
    }

    #[tokio::test]
    async fn end_marker_before_any_data_is_not_an_answer() {
        let message_bus = Arc::new(MessageBusStub::with_ordered_responses(vec![proto_end()]));

        let result = fetch(message_bus, server_versions::PROTOBUF_HISTORICAL_DATA).await;

        assert!(matches!(result, Err(Error::UnexpectedResponse(_))), "got {result:?}");
    }

    #[tokio::test]
    async fn stream_initial_batch_has_no_placeholder_window() {
        // The streaming variant decodes the same data message: its window is
        // unknown until IB states it, never now_utc().
        let message_bus = Arc::new(MessageBusStub::with_ordered_responses(vec![proto_data()]));
        let client = Client::stubbed(message_bus, server_versions::PROTOBUF_HISTORICAL_DATA);

        let mut subscription = client
            .historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .stream()
            .await
            .expect("stream");

        match subscription.next().await {
            Some(Ok(SubscriptionItem::Data(HistoricalBarUpdate::Historical(data)))) => {
                assert_eq!((data.start, data.end), (None, None));
                assert_eq!(data.bars.len(), 1);
            }
            other => panic!("expected the initial batch, got {other:?}"),
        }
    }
}

// === D14 item 3: request id, cancel and a per-call timeout ===

mod request_handle {
    use super::*;
    use crate::common::test_utils::helpers::{decode_request_proto, proto_response};
    use crate::messages::IncomingMessages;
    use crate::testdata::builders::market_data::{full_historical_bar, HistoricalDataEndResponse, HistoricalDataResponse};
    use crate::testdata::builders::ResponseProtoEncoder;
    use crate::transport::AsyncInternalSubscription;
    use std::time::Duration as StdDuration;
    use tokio::sync::broadcast;

    /// A request whose channel stays open until the test drops the sender, as
    /// a Gateway that is slow to answer.
    fn open_request(message_bus: Arc<MessageBusStub>) -> (broadcast::Sender<RoutedItem>, HistoricalDataRequest) {
        let (sender, receiver) = broadcast::channel(16);
        let request = HistoricalDataRequest {
            request_id: TEST_REQ_ID_FIRST,
            subscription: AsyncInternalSubscription::new(receiver),
            message_bus,
            answer: common::HistoricalAnswer::new(TEST_REQ_ID_FIRST, server_versions::PROTOBUF_HISTORICAL_DATA, time_tz::timezones::db::UTC),
            state: RequestState::Open,
            ended: None,
        };
        (sender, request)
    }

    fn proto_data() -> RoutedItem {
        proto_response(
            IncomingMessages::HistoricalData,
            HistoricalDataResponse::default().bar(full_historical_bar()).encode_proto(),
        )
        .into()
    }

    fn proto_end() -> RoutedItem {
        proto_response(IncomingMessages::HistoricalDataEnd, HistoricalDataEndResponse::default().encode_proto()).into()
    }

    fn cancels(message_bus: &MessageBusStub) -> usize {
        count_proto_msgs(&message_bus.request_messages.read().unwrap(), OutgoingMessages::CancelHistoricalData)
    }

    #[tokio::test]
    async fn send_exposes_the_request_id_it_wrote() {
        let message_bus = Arc::new(MessageBusStub::default());
        let client = Client::stubbed(message_bus.clone(), server_versions::PROTOBUF_HISTORICAL_DATA);

        let request = client
            .historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .send()
            .await
            .expect("sent");

        assert_eq!(request.request_id(), TEST_REQ_ID_FIRST);
        let written: crate::proto::HistoricalDataRequest = decode_request_proto(&message_bus, 0);
        assert_eq!(written.req_id, Some(request.request_id()), "the id exposed is the id written");
    }

    #[tokio::test]
    async fn slow_answer_times_out_and_is_cancelled_by_its_id() {
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        sender.send(proto_data()).unwrap();

        // The bars arrived, the end marker did not: the per-call timeout fires.
        let result = request.answer_within(StdDuration::from_millis(20)).await;
        assert!(
            matches!(result, Err(Error::TimedOut(TEST_REQ_ID_FIRST))),
            "expected TimedOut, got {result:?}"
        );
        assert!(request.is_open(), "a timeout leaves the request open at IB");
        assert_eq!(cancels(&message_bus), 0, "a timeout alone writes nothing");

        request.cancel().await.expect("cancel written");
        assert!(!request.is_open());
        assert_eq!(cancels(&message_bus), 1);
        let cancel: crate::proto::CancelHistoricalData = decode_request_proto(&message_bus, 0);
        assert_eq!(cancel.req_id, Some(TEST_REQ_ID_FIRST), "the cancel names this request");

        // Cancelled is terminal and idempotent: no second cancel, no answer.
        request.cancel().await.expect("idempotent");
        assert_eq!(cancels(&message_bus), 1);
        assert!(matches!(request.answer().await, Err(Error::Cancelled)));
        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        assert_eq!(cancels(&message_bus), 1, "a cancelled request sends no cancel on drop");
    }

    #[tokio::test]
    async fn answer_after_a_timeout_keeps_what_arrived() {
        // Negative control for the timeout: the end marker arrives late, and
        // the answer is complete with the bars that came before the timeout.
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        sender.send(proto_data()).unwrap();

        assert!(matches!(
            request.answer_within(StdDuration::from_millis(20)).await,
            Err(Error::TimedOut(_))
        ));
        sender.send(proto_end()).unwrap();

        let data = request.answer_within(StdDuration::from_secs(5)).await.expect("complete answer");
        assert_eq!(data.bars.len(), 1, "the bar received before the timeout is kept");
        assert_eq!(data.end, Some(datetime!(2023-03-15 10:30:00 UTC)));
        assert!(!request.is_open());

        // Ended by IB: nothing to cancel, now or on drop.
        request.cancel().await.expect("nothing to cancel");
        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        assert_eq!(cancels(&message_bus), 0);
    }

    #[tokio::test]
    async fn ended_answer_is_repeated() {
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus);
        sender.send(proto_data()).unwrap();
        sender.send(proto_end()).unwrap();

        let first = request.answer().await.expect("complete");
        let second = request.answer().await.expect("same answer");
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn cancel_that_cannot_be_written_leaves_the_request_open() {
        let message_bus = Arc::new(MessageBusStub::default());
        let (_sender, mut request) = open_request(message_bus.clone());
        message_bus.set_fail_sends(true);

        let result = request.cancel().await;
        assert!(result.is_err(), "the write error is reported");
        assert!(request.is_open(), "an unwritten cancel does not close the request");

        message_bus.set_fail_sends(false);
        request.cancel().await.expect("written on retry");
        assert_eq!(cancels(&message_bus), 1);
    }

    #[tokio::test]
    async fn dropping_an_open_request_cancels_it() {
        let message_bus = Arc::new(MessageBusStub::default());
        let (_sender, request) = open_request(message_bus.clone());

        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        assert_eq!(cancels(&message_bus), 1);
    }

    #[tokio::test]
    async fn fetch_writes_no_cancel_after_a_complete_answer() {
        // Text path: a complete answer through fetch() leaves nothing to cancel.
        let message_bus = Arc::new(MessageBusStub::with_responses(vec![
            "17|9000|1|1678886400|185.50|186.00|185.25|185.75|1000|185.70|100|".to_owned(),
            "108|9000|20230315 09:30:00 UTC|20230315 10:30:00 UTC|".to_owned(),
        ]));
        let mut client = Client::stubbed(message_bus.clone(), server_versions::HISTORICAL_DATA_END);
        client.time_zone = Some(time_tz::timezones::db::UTC);

        client
            .historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .fetch()
            .await
            .expect("complete");
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        assert_eq!(request_message_count(&message_bus), 1, "the request only, no cancel");
    }
}

// === D14 item 5: IB's codes reach the caller with their text ===

mod codes {
    use super::*;
    use crate::common::test_utils::helpers::{proto_response, text_response};
    use crate::messages::{IncomingMessages, Notice};
    use crate::testdata::builders::market_data::{full_historical_bar, HistoricalDataEndResponse, HistoricalDataResponse};
    use crate::testdata::builders::ResponseProtoEncoder;
    use crate::transport::AsyncInternalSubscription;
    use prost::Message;
    use tokio::sync::broadcast;

    const PACING: &str = "Historical Market Data Service error message:Historical data request pacing violation";
    const COMPETING: &str = "No market data during competing live session";
    const NO_TIME_ZONE: &str = "Warning: You submitted request with date-time attributes without explicit time zone.";

    fn proto_data() -> crate::messages::ResponseMessage {
        proto_response(
            IncomingMessages::HistoricalData,
            HistoricalDataResponse::default().bar(full_historical_bar()).encode_proto(),
        )
    }

    fn proto_end() -> crate::messages::ResponseMessage {
        proto_response(IncomingMessages::HistoricalDataEnd, HistoricalDataEndResponse::default().encode_proto())
    }

    fn proto_error(code: i32, text: &str) -> crate::messages::ResponseMessage {
        let error = crate::proto::ErrorMessage {
            id: Some(TEST_REQ_ID_FIRST),
            error_code: Some(code),
            error_msg: Some(text.to_owned()),
            ..Default::default()
        };
        proto_response(IncomingMessages::Error, error.encode_to_vec())
    }

    /// An error frame in the text encoding of servers at or above ERROR_TIME.
    fn text_error(code: i32, text: &str) -> crate::messages::ResponseMessage {
        let mut message = text_response(format!("4|{TEST_REQ_ID_FIRST}|{code}|{text}||"));
        message.server_version = server_versions::ERROR_TIME;
        message
    }

    async fn send(responses: Vec<crate::messages::ResponseMessage>) -> HistoricalDataRequest {
        let message_bus = Arc::new(MessageBusStub::with_ordered_responses(responses));
        let mut client = Client::stubbed(message_bus, server_versions::PROTOBUF_HISTORICAL_DATA);
        client.time_zone = Some(time_tz::timezones::db::UTC);
        client
            .historical_data(&test_contract(), BarSize::Hour)
            .duration(Duration::days(1))
            .send()
            .await
            .expect("sent")
    }

    fn codes(notices: &[Notice]) -> Vec<(i32, &str)> {
        notices.iter().map(|n| (n.code, n.message.as_str())).collect()
    }

    #[tokio::test]
    async fn informational_codes_are_kept_and_do_not_end_the_answer() {
        // Text and protobuf error frames alike: 2174 is IB telling the caller
        // something about a request it still serves.
        for notice in [text_error(2174, NO_TIME_ZONE), proto_error(2174, NO_TIME_ZONE)] {
            let mut request = send(vec![notice, proto_data(), proto_end()]).await;

            let data = request.answer().await.expect("the answer still completes");

            assert_eq!(data.bars.len(), 1);
            assert_eq!(codes(&data.notices), vec![(2174, NO_TIME_ZONE)]);
            assert_eq!(codes(request.notices()), vec![(2174, NO_TIME_ZONE)]);
        }
    }

    #[tokio::test]
    async fn terminal_codes_end_the_answer_with_their_code_and_text() {
        // Negative control for the above: 162 (pacing) and 10197 (competing
        // session) end the request, raw, on both encodings.
        for (code, text) in [(162, PACING), (10197, COMPETING)] {
            for error in [text_error(code, text), proto_error(code, text)] {
                let mut request = send(vec![text_error(2174, NO_TIME_ZONE), error]).await;

                match request.answer().await {
                    Err(Error::Notice(notice)) => assert_eq!((notice.code, notice.message.as_str()), (code, text)),
                    other => panic!("expected IB's {code}, got {other:?}"),
                }
                assert_eq!(
                    codes(request.notices()),
                    vec![(2174, NO_TIME_ZONE), (code, text)],
                    "every code for the request, in order"
                );
                assert!(!request.is_open(), "IB ended it: nothing to cancel");
            }
        }
    }

    #[tokio::test]
    async fn dispatcher_classified_codes_are_kept_in_order() {
        // As the live dispatcher delivers them: a 2100-2169 warning as a
        // Notice item, any other code as an Error item.
        let (sender, receiver) = broadcast::channel(16);
        let mut request = HistoricalDataRequest {
            request_id: TEST_REQ_ID_FIRST,
            subscription: AsyncInternalSubscription::new(receiver),
            message_bus: Arc::new(MessageBusStub::default()),
            answer: common::HistoricalAnswer::new(TEST_REQ_ID_FIRST, server_versions::PROTOBUF_HISTORICAL_DATA, time_tz::timezones::db::UTC),
            state: RequestState::Open,
            ended: None,
        };
        let notice = |code: i32, text: &str| Notice {
            code,
            message: text.to_owned(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        sender
            .send(RoutedItem::Notice(notice(2107, "HMDS data farm connection is inactive")))
            .unwrap();
        sender.send(RoutedItem::Error(Error::Notice(notice(2174, NO_TIME_ZONE)))).unwrap();
        sender.send(proto_data().into()).unwrap();
        sender.send(RoutedItem::Error(Error::Notice(notice(162, PACING)))).unwrap();

        assert!(matches!(request.answer().await, Err(Error::Notice(n)) if n.code == 162));
        assert_eq!(
            codes(request.notices()),
            vec![(2107, "HMDS data farm connection is inactive"), (2174, NO_TIME_ZONE), (162, PACING)]
        );
    }
}

// === Review D14.R1: a local end does not stop the cancel ===

mod local_end_still_cancels {
    use super::*;
    use crate::common::test_utils::helpers::{proto_response, text_response};
    use crate::messages::IncomingMessages;
    use crate::testdata::builders::market_data::{full_historical_bar, HistoricalDataEndResponse, HistoricalDataResponse};
    use crate::testdata::builders::ResponseProtoEncoder;
    use crate::transport::AsyncInternalSubscription;
    use std::time::Duration as StdDuration;
    use tokio::sync::broadcast;

    fn open_request(message_bus: Arc<MessageBusStub>) -> (broadcast::Sender<RoutedItem>, HistoricalDataRequest) {
        let (sender, receiver) = broadcast::channel(16);
        let request = HistoricalDataRequest {
            request_id: TEST_REQ_ID_FIRST,
            subscription: AsyncInternalSubscription::new(receiver),
            message_bus,
            answer: common::HistoricalAnswer::new(TEST_REQ_ID_FIRST, server_versions::PROTOBUF_HISTORICAL_DATA, time_tz::timezones::db::UTC),
            state: RequestState::Open,
            ended: None,
        };
        (sender, request)
    }

    fn cancels(message_bus: &MessageBusStub) -> usize {
        count_proto_msgs(&message_bus.request_messages.read().unwrap(), OutgoingMessages::CancelHistoricalData)
    }

    fn unreadable_bar() -> RoutedItem {
        let bar = crate::proto::HistoricalDataBar {
            date: None,
            ..full_historical_bar()
        };
        proto_response(
            IncomingMessages::HistoricalData,
            HistoricalDataResponse::default().bar(bar).encode_proto(),
        )
        .into()
    }

    fn proto_data() -> RoutedItem {
        proto_response(
            IncomingMessages::HistoricalData,
            HistoricalDataResponse::default().bar(full_historical_bar()).encode_proto(),
        )
        .into()
    }

    fn proto_end() -> RoutedItem {
        proto_response(IncomingMessages::HistoricalDataEnd, HistoricalDataEndResponse::default().encode_proto()).into()
    }

    /// End the answer with a local error, then cancel explicitly.
    async fn cancel_after(item: RoutedItem) -> usize {
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        sender.send(item).unwrap();

        assert!(request.answer().await.is_err(), "the answer ends with the local error");
        assert!(request.is_open(), "IB did not end it: still open");
        request.cancel().await.expect("cancel written");
        assert!(!request.is_open());
        cancels(&message_bus)
    }

    #[tokio::test]
    async fn lagged_error_then_cancel_writes_the_cancel() {
        assert_eq!(cancel_after(RoutedItem::Error(Error::Lagged(1))).await, 1);
    }

    #[tokio::test]
    async fn decode_error_then_cancel_writes_the_cancel() {
        assert_eq!(cancel_after(unreadable_bar()).await, 1);
    }

    #[tokio::test]
    async fn unexpected_message_then_cancel_writes_the_cancel() {
        let tick_price = text_response("1|2|9000|1|185.50|100|7|").into();
        assert_eq!(cancel_after(tick_price).await, 1);
    }

    #[tokio::test]
    async fn drop_after_a_local_error_writes_the_cancel() {
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        sender.send(unreadable_bar()).unwrap();
        assert!(request.answer().await.is_err());

        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        assert_eq!(cancels(&message_bus), 1);
    }

    #[tokio::test]
    async fn after_ibs_end_marker_neither_cancel_nor_drop_writes() {
        // Control: IB ended the request, so there is nothing to cancel.
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        sender.send(proto_data()).unwrap();
        sender.send(proto_end()).unwrap();
        request.answer().await.expect("complete");

        request.cancel().await.expect("nothing to cancel");
        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        assert_eq!(cancels(&message_bus), 0);
    }

    #[tokio::test]
    async fn after_the_session_is_lost_neither_cancel_nor_drop_writes() {
        // Control: a closed channel or a reset is the session gone, and IB
        // drops its requests with it.
        for item in [Some(RoutedItem::Error(Error::ConnectionReset)), None] {
            let message_bus = Arc::new(MessageBusStub::default());
            let (sender, mut request) = open_request(message_bus.clone());
            match item {
                Some(item) => {
                    sender.send(item).unwrap();
                }
                None => drop(sender),
            }
            assert!(matches!(request.answer().await, Err(Error::Interrupted(TEST_REQ_ID_FIRST))));
            assert!(!request.is_open());

            request.cancel().await.expect("nothing to cancel");
            drop(request);
            tokio::time::sleep(StdDuration::from_millis(20)).await;

            assert_eq!(cancels(&message_bus), 0);
        }
    }

    #[tokio::test]
    async fn after_an_ib_error_code_neither_cancel_nor_drop_writes() {
        // Control: a code that ends the request is IB ending it.
        let message_bus = Arc::new(MessageBusStub::default());
        let (sender, mut request) = open_request(message_bus.clone());
        let pacing = crate::messages::Notice {
            code: 162,
            message: "Historical Market Data Service error message:Historical data request pacing violation".to_owned(),
            error_time: None,
            advanced_order_reject_json: String::new(),
        };
        sender.send(RoutedItem::Error(Error::Notice(pacing))).unwrap();
        assert!(matches!(request.answer().await, Err(Error::Notice(_))));
        assert!(!request.is_open());

        request.cancel().await.expect("nothing to cancel");
        drop(request);
        tokio::time::sleep(StdDuration::from_millis(20)).await;

        assert_eq!(cancels(&message_bus), 0);
    }
}
