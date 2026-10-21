use super::*;
use crate::messages::IncomingMessages;
use time::macros::{date, datetime};
use time_tz;

#[test]
fn test_decode_head_timestamp() {
    let mut message = ResponseMessage::from("88\09000\01560346200\0");

    let head_timestamp = super::decode_head_timestamp(&mut message, None).expect("error decoding trade tick");

    assert_eq!(head_timestamp, datetime!(2019-06-12 13:30).assume_utc(), "head_timestamp");
}

#[test]
fn test_decode_historical_schedule() {
    let time_zone: &Tz = time_tz::timezones::db::america::NEW_YORK;

    let mut message =
        ResponseMessage::from("106\09000\020230414-09:30:00\020230414-16:00:00\0US/Eastern\01\020230414-09:30:00\020230414-16:00:00\020230414\0");

    let schedule = decode_historical_schedule(&mut message).expect("error decoding historical schedule");

    assert_eq!(
        schedule.start,
        datetime!(2023-04-14 9:30:00).assume_timezone(time_zone).unwrap(),
        "schedule.start"
    );
    assert_eq!(
        schedule.end,
        datetime!(2023-04-14 16:00:00).assume_timezone(time_zone).unwrap(),
        "schedule.end"
    );
    assert_eq!(schedule.time_zone, "US/Eastern", "schedule.time_zone");

    assert_eq!(schedule.sessions.len(), 1, "schedule.sessions.len()");
    assert_eq!(schedule.sessions[0].reference, date!(2023 - 04 - 14), "schedule.sessions[0].reference");
    assert_eq!(
        schedule.sessions[0].start,
        datetime!(2023-04-14 9:30:00).assume_timezone(time_zone).unwrap(),
        "schedule.sessions[0].start"
    );
    assert_eq!(
        schedule.sessions[0].end,
        datetime!(2023-04-14 16:00:00.0).assume_timezone(time_zone).unwrap(),
        "schedule.sessions[0].end"
    );
}

#[test]
fn test_decode_historical_schedule_unknown_timezone_errors() {
    // Gateway sent an unmappable timezone — must surface as Error, not panic.
    let mut message = ResponseMessage::from(
        "106\09000\020230414-09:30:00\020230414-16:00:00\0Bogus Standard Time\01\020230414-09:30:00\020230414-16:00:00\020230414\0",
    );

    let err = decode_historical_schedule(&mut message).expect_err("unknown tz must error");
    assert!(matches!(err, Error::UnsupportedTimeZone(ref name) if name == "Bogus Standard Time"));
    let rendered = err.to_string();
    assert!(rendered.contains("Bogus Standard Time"), "missing tz name: {rendered}");
    assert!(
        rendered.contains("register_timezone_alias"),
        "missing programmatic-fix pointer: {rendered}"
    );
    assert!(rendered.contains("IBAPI_TIMEZONE_ALIASES"), "missing env-var pointer: {rendered}");
}

#[test]
fn test_decode_historical_data() {
    let mut message = ResponseMessage::from("17\09000\020230413  16:31:22\020230415  16:31:22\02\020230413\0182.9400\0186.5000\0180.9400\0185.9000\0948837.22\0184.869\0324891\020230414\0183.8800\0186.2800\0182.0100\0185.0000\0810998.27\0183.9865\0277547\0");

    let server_version = server_versions::HISTORICAL_SCHEDULE;
    let time_zone: &Tz = time_tz::timezones::db::america::NEW_YORK;

    let historical_data = decode_historical_data(server_version, time_zone, &mut message).expect("error decoding historical data");

    assert_eq!(
        historical_data.start,
        Some(datetime!(2023-04-13 16:31:22).assume_timezone(time_zone).unwrap()),
        "historical_data.start"
    );
    assert_eq!(
        historical_data.end,
        Some(datetime!(2023-04-15 16:31:22).assume_timezone(time_zone).unwrap()),
        "historical_data.end"
    );

    assert_eq!(historical_data.bars.len(), 2, "historical_data.bars.len()");
    assert_eq!(
        historical_data.bars[0].date,
        datetime!(2023-04-13 0:00:00 UTC),
        "historical_data.bars[0].date"
    );
    assert_eq!(historical_data.bars[0].open, Some(182.94), "historical_data.bars[0].open");
    assert_eq!(historical_data.bars[0].high, Some(186.50), "historical_data.bars[0].high");
    assert_eq!(historical_data.bars[0].low, Some(180.94), "historical_data.bars[0].low");
    assert_eq!(historical_data.bars[0].close, Some(185.90), "historical_data.bars[0].close");
    assert_eq!(historical_data.bars[0].volume, Some(948837.22), "historical_data.bars[0].volume");
    assert_eq!(historical_data.bars[0].wap, Some(184.869), "historical_data.bars[0].wap");
    assert_eq!(historical_data.bars[0].count, Some(324891), "historical_data.bars[0].count");
}

#[test]
fn test_decode_historical_tick_bid_ask() {
    let sample_message = "97\09000\04\01681133399\00\011.63\011.83\02800\0100\01681133400\00\011.63\011.83\02800\0200\01681133400\00\011.63\011.72\02800\0100\01681133400\00\011.63\011.83\02800\0200\01\0";
    let mut message = ResponseMessage::from(sample_message);

    let (ticks, done) = decode_historical_ticks_bid_ask(&mut message).unwrap();

    assert_eq!(ticks.len(), 4, "ticks.len()");
    assert!(done, "done");

    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:29:59 UTC), "ticks[0].timestamp");
    assert_eq!(
        ticks[0].tick_attribute_bid_ask,
        TickAttributeBidAsk {
            bid_past_low: false,
            ask_past_high: false
        },
        "ticks[0].tick_attribute_bid_ask"
    );
    assert_eq!(ticks[0].price_bid, 11.63, "ticks[0].price_bid");
    assert_eq!(ticks[0].price_ask, 11.83, "ticks[0].price_ask");
    assert_eq!(ticks[0].size_bid, 2800, "ticks[0].size_bid");
    assert_eq!(ticks[0].size_ask, 100, "ticks[0].size_ask");

    assert_eq!(ticks[3].timestamp, datetime!(2023-04-10 13:30:00 UTC), "ticks[0].timestamp");
    assert_eq!(
        ticks[3].tick_attribute_bid_ask,
        TickAttributeBidAsk {
            bid_past_low: false,
            ask_past_high: false
        },
        "ticks[0].tick_attribute_bid_ask"
    );
    assert_eq!(ticks[3].price_bid, 11.63, "ticks[0].price_bid");
    assert_eq!(ticks[3].price_ask, 11.83, "ticks[0].price_ask");
    assert_eq!(ticks[3].size_bid, 2800, "ticks[0].size_bid");
    assert_eq!(ticks[3].size_ask, 200, "ticks[0].size_ask");
}

#[test]
fn test_decode_historical_tick_last() {
    let sample_message = "98\09000\07\01681133400\00\011.63\024547\0ISLAND\0 O X\01681133400\02\011.73\01\0DRCTEDGE\0   I\01681133401\00\011.63\0179\0FINRA\0\01681133401\02\011.73\01\0FINRA\0   I\01681133402\02\011.63\01\0FINRA\0 4 I\01681133402\02\011.73\01\0FINRA\0   I\01681133402\02\011.73\01\0FINRA\0   I\01\0";
    let mut message = ResponseMessage::from(sample_message);

    let (ticks, done) = decode_historical_ticks_last(&mut message).unwrap();

    assert_eq!(ticks.len(), 7, "ticks.len()");
    assert!(done, "done");

    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:30:0 UTC), "ticks[0].timestamp");
    assert_eq!(
        ticks[0].tick_attribute_last,
        TickAttributeLast {
            past_limit: false,
            unreported: false
        },
        "ticks[0].tick_attribute_last"
    );
    assert_eq!(ticks[0].price, 11.63, "ticks[0].price");
    assert_eq!(ticks[0].size, 24547, "ticks[0].size");
    assert_eq!(ticks[0].exchange, "ISLAND", "ticks[0].exchange");
    assert_eq!(ticks[0].special_conditions, " O X", "ticks[0].special_conditions");

    assert_eq!(ticks[6].timestamp, datetime!(2023-04-10 13:30:02 UTC), "ticks[6].timestamp");
    assert_eq!(
        ticks[6].tick_attribute_last,
        TickAttributeLast {
            past_limit: false,
            unreported: true
        },
        "ticks[6].tick_attribute_last"
    );
    assert_eq!(ticks[6].price, 11.73, "ticks[6].price");
    assert_eq!(ticks[6].size, 1, "ticks[6].size");
    assert_eq!(ticks[6].exchange, "FINRA", "ticks[6].exchange");
    assert_eq!(ticks[6].special_conditions, "   I", "ticks[6].special_conditions");
}

#[test]
fn test_decode_historical_tick_midpoint() {
    let sample_message = "96\09000\024\01681133398\00\091.36\00\01681133400\00\091.355\00\01681133400\00\091.35\00\01681133400\00\091.345\00\01681133400\00\091.35\00\01681133400\00\091.355\00\01681133400\00\091.35\00\01681133400\00\091.34\00\01681133400\00\091.345\00\01681133400\00\091.34\00\01681133400\00\091.345\00\01681133400\00\091.34\00\01681133400\00\091.335\00\01681133400\00\091.33\00\01681133400\00\091.325\00\01681133400\00\091.32\00\01681133400\00\091.325\00\01681133400\00\091.32\00\01681133400\00\091.315\00\01681133400\00\091.32\00\01681133400\00\091.325\00\01681133400\00\091.32\00\01681133400\00\091.315\00\01681133400\00\091.31\00\01\0";
    let mut message = ResponseMessage::from(sample_message);

    let (ticks, done) = decode_historical_ticks_mid_point(&mut message).unwrap();

    assert_eq!(ticks.len(), 24, "ticks.len()");
    assert!(done, "done");

    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:29:58 UTC), "ticks[0].timestamp");
    assert_eq!(ticks[0].price, 91.36, "ticks[0].price");
    assert_eq!(ticks[0].size, 0, "ticks[0].size");

    assert_eq!(ticks[23].timestamp, datetime!(2023-04-10 13:30:00 UTC), "ticks[0].timestamp");
    assert_eq!(ticks[23].price, 91.31, "ticks[0].price");
    assert_eq!(ticks[23].size, 0, "ticks[0].size");
}

#[test]
fn test_decode_historical_data_update() {
    let time_zone: &Tz = time_tz::timezones::db::america::NEW_YORK;

    // Message format: message_type|request_id|bar_count|timestamp|open|high|low|close|volume|wap|count
    let mut message = ResponseMessage::from("90\09000\0-1\01681133400\0185.50\0186.00\0185.00\0185.75\01000.5\0185.625\0150\0");

    let bar = decode_historical_data_update(time_zone, &mut message).expect("error decoding historical data update");

    assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC), "bar.date");
    assert_eq!(bar.open, Some(185.50), "bar.open");
    assert_eq!(bar.high, Some(186.00), "bar.high");
    assert_eq!(bar.low, Some(185.00), "bar.low");
    assert_eq!(bar.close, Some(185.75), "bar.close");
    assert_eq!(bar.volume, Some(1000.5), "bar.volume");
    assert_eq!(bar.wap, Some(185.625), "bar.wap");
    assert_eq!(bar.count, Some(150), "bar.count");
}

#[test]
fn test_decode_historical_data_update_without_count() {
    let time_zone: &Tz = time_tz::timezones::db::america::NEW_YORK;

    // Message without count field (optional in streaming updates)
    let mut message = ResponseMessage::from("90\09000\0-1\01681133400\0185.50\0186.00\0185.00\0185.75\01000.5\0185.625\0");

    let bar = decode_historical_data_update(time_zone, &mut message).expect("error decoding historical data update");

    assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC), "bar.date");
    assert_eq!(bar.open, Some(185.50), "bar.open");
    assert_eq!(bar.high, Some(186.00), "bar.high");
    assert_eq!(bar.low, Some(185.00), "bar.low");
    assert_eq!(bar.close, Some(185.75), "bar.close");
    assert_eq!(bar.volume, Some(1000.5), "bar.volume");
    assert_eq!(bar.wap, Some(185.625), "bar.wap");
    assert_eq!(bar.count, None, "an update without a count has no count, not zero");
}

#[test]
fn test_parse_bar_date_yyyymmdd() {
    let tz = time_tz::timezones::db::UTC;
    let result = parse_bar_date("20230414", tz).unwrap();
    assert_eq!(result, datetime!(2023-04-14 0:00:00 UTC));
}

#[test]
fn test_parse_bar_date_unix_timestamp() {
    let tz = time_tz::timezones::db::america::NEW_YORK;
    let result = parse_bar_date("1681133400", tz).unwrap();
    assert_eq!(result, datetime!(2023-04-10 9:30:00).assume_timezone(tz).unwrap());
}

#[test]
fn test_parse_bar_date_invalid_timestamp() {
    let tz = time_tz::timezones::db::UTC;
    let err = parse_bar_date("not_a_number", tz).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("not_a_number"), "error should include the bad value: {msg}");
    assert!(msg.contains("invalid digit"), "error should include parse reason: {msg}");
}

#[test]
fn test_decode_historical_data_proto() {
    use prost::Message;

    let proto_msg = crate::proto::HistoricalData {
        req_id: Some(1),
        historical_data_bars: vec![
            crate::proto::HistoricalDataBar {
                date: Some("1681133400".into()),
                open: Some(185.50),
                high: Some(186.00),
                low: Some(185.00),
                close: Some(185.75),
                volume: Some("1000".into()),
                wap: Some("185.625".into()),
                bar_count: Some(150),
            },
            crate::proto::HistoricalDataBar {
                date: Some("1681219800".into()),
                open: Some(186.00),
                high: Some(187.00),
                low: Some(185.50),
                close: Some(186.50),
                volume: Some("2000".into()),
                wap: Some("186.25".into()),
                bar_count: Some(300),
            },
        ],
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let bars = decode_historical_data_proto(&bytes, time_tz::timezones::db::UTC).unwrap();
    assert_eq!(bars.len(), 2);

    assert_eq!(bars[0].date, datetime!(2023-04-10 13:30:00 UTC));
    assert_eq!(bars[0].open, Some(185.50));
    assert_eq!(bars[0].high, Some(186.00));
    assert_eq!(bars[0].low, Some(185.00));
    assert_eq!(bars[0].close, Some(185.75));
    assert_eq!(bars[0].volume, Some(1000.0));
    assert_eq!(bars[0].wap, Some(185.625));
    assert_eq!(bars[0].count, Some(150));

    assert_eq!(bars[1].open, Some(186.00));
    assert_eq!(bars[1].count, Some(300));
}

#[test]
fn test_decode_historical_ticks_last_proto() {
    use prost::Message;

    let proto_msg = crate::proto::HistoricalTicksLast {
        req_id: Some(1),
        historical_ticks_last: vec![
            crate::proto::HistoricalTickLast {
                time: Some(1681133400),
                tick_attrib_last: Some(crate::proto::TickAttribLast {
                    past_limit: Some(true),
                    unreported: Some(false),
                }),
                price: Some(11.63),
                size: Some("100".into()),
                exchange: Some("ISLAND".into()),
                special_conditions: Some("O X".into()),
            },
            crate::proto::HistoricalTickLast {
                time: Some(1681133401),
                tick_attrib_last: Some(crate::proto::TickAttribLast {
                    past_limit: Some(false),
                    unreported: Some(true),
                }),
                price: Some(11.73),
                size: Some("50".into()),
                exchange: Some("FINRA".into()),
                special_conditions: Some("I".into()),
            },
        ],
        is_done: Some(true),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let (ticks, done) = decode_historical_ticks_last_proto(&bytes).unwrap();
    assert!(done);
    assert_eq!(ticks.len(), 2);

    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:30:00 UTC));
    assert!(ticks[0].tick_attribute_last.past_limit);
    assert!(!ticks[0].tick_attribute_last.unreported);
    assert_eq!(ticks[0].price, 11.63);
    assert_eq!(ticks[0].size, 100);
    assert_eq!(ticks[0].exchange, "ISLAND");
    assert_eq!(ticks[0].special_conditions, "O X");

    assert_eq!(ticks[1].timestamp, datetime!(2023-04-10 13:30:01 UTC));
    assert!(!ticks[1].tick_attribute_last.past_limit);
    assert!(ticks[1].tick_attribute_last.unreported);
    assert_eq!(ticks[1].size, 50);
    assert_eq!(ticks[1].exchange, "FINRA");
}

#[test]
fn test_decode_head_timestamp_proto() {
    use prost::Message;

    let proto_msg = crate::proto::HeadTimestamp {
        req_id: Some(1),
        head_timestamp: Some("1609459200".into()),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_head_timestamp_proto(&bytes).unwrap();
    assert_eq!(result, "1609459200");
}

#[test]
fn test_decode_historical_ticks_proto() {
    use prost::Message;

    let proto_msg = crate::proto::HistoricalTicks {
        req_id: Some(1),
        historical_ticks: vec![
            crate::proto::HistoricalTick {
                time: Some(1681133400),
                price: Some(150.0),
                size: Some("100".into()),
            },
            crate::proto::HistoricalTick {
                time: Some(1681133401),
                price: Some(150.5),
                size: Some("200".into()),
            },
        ],
        is_done: Some(false),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let (ticks, done) = decode_historical_ticks_proto(&bytes).unwrap();
    assert_eq!(ticks.len(), 2);
    assert!(!done);
    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:30:00 UTC));
    assert_eq!(ticks[0].price, 150.0);
    assert_eq!(ticks[0].size, 100);
}

#[test]
fn test_decode_historical_ticks_bid_ask_proto() {
    use prost::Message;

    let proto_msg = crate::proto::HistoricalTicksBidAsk {
        req_id: Some(1),
        historical_ticks_bid_ask: vec![crate::proto::HistoricalTickBidAsk {
            time: Some(1681133400),
            tick_attrib_bid_ask: Some(crate::proto::TickAttribBidAsk {
                bid_past_low: Some(true),
                ask_past_high: Some(false),
            }),
            price_bid: Some(149.0),
            price_ask: Some(151.0),
            size_bid: Some("100".into()),
            size_ask: Some("200".into()),
        }],
        is_done: Some(true),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let (ticks, done) = decode_historical_ticks_bid_ask_proto(&bytes).unwrap();
    assert_eq!(ticks.len(), 1);
    assert!(done);
    assert_eq!(ticks[0].timestamp, datetime!(2023-04-10 13:30:00 UTC));
    assert!(ticks[0].tick_attribute_bid_ask.bid_past_low);
    assert!(!ticks[0].tick_attribute_bid_ask.ask_past_high);
    assert_eq!(ticks[0].price_bid, 149.0);
    assert_eq!(ticks[0].price_ask, 151.0);
    assert_eq!(ticks[0].size_bid, 100);
    assert_eq!(ticks[0].size_ask, 200);
}

#[test]
fn test_decode_histogram_data_proto() {
    use prost::Message;
    let proto_msg = crate::proto::HistogramData {
        req_id: Some(1),
        histogram_data_entries: vec![
            crate::proto::HistogramDataEntry {
                price: Some(100.5),
                size: Some("50".into()),
            },
            crate::proto::HistogramDataEntry {
                price: Some(101.0),
                size: Some("75".into()),
            },
        ],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_histogram_data_proto(&bytes).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].price, 100.5);
    assert_eq!(result[0].size, 50);
    assert_eq!(result[1].price, 101.0);
    assert_eq!(result[1].size, 75);
}

#[test]
fn test_decode_historical_data_end_proto() {
    use prost::Message;
    // Wire format for start/end uses "YYYYMMDD HH:MM:SS TZ".
    let proto_msg = crate::proto::HistoricalDataEnd {
        req_id: Some(1),
        start_date_str: Some("20260101 09:30:00 US/Eastern".into()),
        end_date_str: Some("20260105 16:00:00 US/Eastern".into()),
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let (start, end) = decode_historical_data_end_proto(&bytes).unwrap();
    assert!(start < end);
    assert_eq!(start.year(), 2026);
    assert_eq!(end.year(), 2026);
}

#[test]
fn test_decode_historical_schedule_proto() {
    use prost::Message;
    let proto_msg = crate::proto::HistoricalSchedule {
        req_id: Some(1),
        start_date_time: Some("20260101-09:30:00".into()),
        end_date_time: Some("20260105-16:00:00".into()),
        time_zone: Some("US/Eastern".into()),
        historical_sessions: vec![crate::proto::HistoricalSession {
            start_date_time: Some("20260102-09:30:00".into()),
            end_date_time: Some("20260102-16:00:00".into()),
            ref_date: Some("20260102".into()),
        }],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_historical_schedule_proto(&bytes).unwrap();
    assert_eq!(result.time_zone, "US/Eastern");
    assert_eq!(result.sessions.len(), 1);
    assert_eq!(result.sessions[0].reference, date!(2026 - 01 - 02));
}

#[test]
fn test_decode_historical_data_update_proto() {
    use prost::Message;
    let proto_msg = crate::proto::HistoricalDataUpdate {
        req_id: Some(1),
        historical_data_bar: Some(crate::proto::HistoricalDataBar {
            date: Some("1681133400".into()), // unix timestamp
            open: Some(150.0),
            high: Some(151.0),
            low: Some(149.5),
            close: Some(150.75),
            volume: Some("1000".into()),
            wap: Some("150.5".into()),
            bar_count: Some(42),
        }),
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_historical_data_update_proto(&bytes, time_tz::timezones::db::UTC).unwrap();
    assert_eq!(result.open, Some(150.0));
    assert_eq!(result.high, Some(151.0));
    assert_eq!(result.low, Some(149.5));
    assert_eq!(result.close, Some(150.75));
    assert_eq!(result.volume, Some(1000.0));
    assert_eq!(result.wap, Some(150.5));
    assert_eq!(result.count, Some(42));
    assert_eq!(result.date, datetime!(2023-04-10 13:30:00 UTC));
}

#[test]
fn test_decode_historical_data_update_proto_missing_bar_is_an_error() {
    use prost::Message;
    let proto_msg = crate::proto::HistoricalDataUpdate {
        req_id: Some(1),
        historical_data_bar: None,
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    // No bar is not a bar at UNIX_EPOCH with a count of zero.
    let result = decode_historical_data_update_proto(&bytes, time_tz::timezones::db::UTC);
    assert!(matches!(result, Err(Error::Parse(..))), "expected a parse error, got {result:?}");
}

// === D14 item 4: absent values stay absent ===

fn text_historical_data(fields: &[&str]) -> ResponseMessage {
    // message type, request id, one bar, then the bar's fields.
    let mut text = String::from("17\09000\01\0");
    for field in fields {
        text.push_str(field);
        text.push('\0');
    }
    ResponseMessage::from(text.as_str())
}

fn proto_historical_data(bar: crate::proto::HistoricalDataBar) -> ResponseMessage {
    use prost::Message;
    let proto_msg = crate::proto::HistoricalData {
        req_id: Some(9000),
        historical_data_bars: vec![bar],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();
    ResponseMessage::from_protobuf(IncomingMessages::HistoricalData as i32, bytes, server_versions::PROTOBUF_HISTORICAL_DATA)
}

fn full_proto_bar() -> crate::proto::HistoricalDataBar {
    crate::proto::HistoricalDataBar {
        date: Some("1681133400".into()),
        open: Some(185.50),
        high: Some(186.00),
        low: Some(185.00),
        close: Some(185.75),
        volume: Some("1000".into()),
        wap: Some("185.625".into()),
        bar_count: Some(150),
    }
}

const UTC: &Tz = time_tz::timezones::db::UTC;

#[test]
fn test_text_bar_with_empty_values_keeps_them_absent() {
    let mut message = text_historical_data(&["1681133400", "", "", "", "", "", "", ""]);
    let data = decode_historical_data(server_versions::HISTORICAL_DATA_END, UTC, &mut message).expect("decodes");

    let bar = &data.bars[0];
    assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC));
    assert_eq!(
        (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap, bar.count),
        (None, None, None, None, None, None, None),
        "an empty field is a value IB did not state, never 0.0"
    );
}

#[test]
fn test_text_bar_keeps_stated_zero_and_negative_values() {
    // Negative control: a stated 0 and IB's own -1 are values, not absence.
    let mut message = text_historical_data(&["1681133400", "0", "0.0", "0", "0", "-1", "-1", "-1"]);
    let data = decode_historical_data(server_versions::HISTORICAL_DATA_END, UTC, &mut message).expect("decodes");

    let bar = &data.bars[0];
    assert_eq!(bar.open, Some(0.0));
    assert_eq!(bar.high, Some(0.0));
    assert_eq!(bar.close, Some(0.0));
    assert_eq!(bar.volume, Some(-1.0));
    assert_eq!(bar.wap, Some(-1.0));
    assert_eq!(bar.count, Some(-1));
}

#[test]
fn test_text_bar_with_unreadable_value_is_an_error() {
    let mut message = text_historical_data(&["1681133400", "abc", "1", "1", "1", "1", "1", "1"]);
    let result = decode_historical_data(server_versions::HISTORICAL_DATA_END, UTC, &mut message);
    assert!(matches!(result, Err(Error::Parse(..))), "expected a parse error, got {result:?}");
}

#[test]
fn test_text_bar_with_empty_or_unreadable_time_is_an_error() {
    for date in ["", "yesterday", "99999999999999999999"] {
        let mut message = text_historical_data(&[date, "1", "1", "1", "1", "1", "1", "1"]);
        let result = decode_historical_data(server_versions::HISTORICAL_DATA_END, UTC, &mut message);
        assert!(result.is_err(), "bar time {date:?} must not decode, got {result:?}");
    }
}

#[test]
fn test_text_update_with_empty_values_keeps_them_absent() {
    let mut message = ResponseMessage::from("90\09000\0-1\01681133400\0\0\0\0\0\0\0\0");
    let bar = decode_historical_data_update(UTC, &mut message).expect("decodes");
    assert_eq!((bar.open, bar.close, bar.volume, bar.wap, bar.count), (None, None, None, None, None));
}

#[test]
fn test_proto_bar_with_absent_values_keeps_them_absent() {
    let mut message = proto_historical_data(crate::proto::HistoricalDataBar {
        date: Some("1681133400".into()),
        ..Default::default()
    });
    let data = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut message).expect("decodes");

    let bar = &data.bars[0];
    assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC));
    assert_eq!(
        (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.wap, bar.count),
        (None, None, None, None, None, None, None),
        "an absent field is a value IB did not state, never 0.0 and never -1"
    );
}

#[test]
fn test_proto_bar_keeps_stated_values() {
    // Negative control: every field present, zero included, comes through as stated.
    let mut message = proto_historical_data(crate::proto::HistoricalDataBar {
        open: Some(0.0),
        volume: Some("0".into()),
        bar_count: Some(-1),
        ..full_proto_bar()
    });
    let data = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut message).expect("decodes");

    let bar = &data.bars[0];
    assert_eq!(bar.open, Some(0.0));
    assert_eq!(bar.high, Some(186.00));
    assert_eq!(bar.low, Some(185.00));
    assert_eq!(bar.close, Some(185.75));
    assert_eq!(bar.volume, Some(0.0));
    assert_eq!(bar.wap, Some(185.625));
    assert_eq!(bar.count, Some(-1));
}

#[test]
fn test_proto_bar_with_unreadable_volume_is_an_error() {
    let mut message = proto_historical_data(crate::proto::HistoricalDataBar {
        volume: Some("lots".into()),
        ..full_proto_bar()
    });
    let result = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut message);
    assert!(matches!(result, Err(Error::Parse(..))), "expected a parse error, got {result:?}");
}

#[test]
fn test_proto_bar_with_absent_or_unreadable_time_is_an_error() {
    for date in [None, Some(""), Some("yesterday"), Some("99999999999999999999")] {
        let mut message = proto_historical_data(crate::proto::HistoricalDataBar {
            date: date.map(String::from),
            ..full_proto_bar()
        });
        let result = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut message);
        assert!(result.is_err(), "bar time {date:?} must not decode as UNIX_EPOCH, got {result:?}");
    }
}

#[test]
fn test_proto_daily_bar_time_reads_as_on_the_text_path() {
    let mut message = proto_historical_data(crate::proto::HistoricalDataBar {
        date: Some("20230413".into()),
        ..full_proto_bar()
    });
    let data = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut message).expect("decodes");
    assert_eq!(data.bars[0].date, datetime!(2023-04-13 0:00:00 UTC));
}

// === D14 item 1: no placeholder window ===

#[test]
fn test_data_message_alone_states_no_window_on_either_path() {
    let mut text = text_historical_data(&["1681133400", "1", "1", "1", "1", "1", "1", "1"]);
    let data = decode_historical_data(server_versions::HISTORICAL_DATA_END, UTC, &mut text).expect("decodes");
    assert_eq!(
        (data.start, data.end),
        (None, None),
        "text: the window is the end marker's, not now_utc()"
    );

    let mut proto = proto_historical_data(full_proto_bar());
    let data = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, UTC, &mut proto).expect("decodes");
    assert_eq!(
        (data.start, data.end),
        (None, None),
        "protobuf: the window is the end marker's, not now_utc()"
    );
}

// === Bar time literal: the time field as IB sent it ===

/// Decodes one bar whose time field is `time` on all four bar paths: text and
/// protobuf, data message and streaming update.
fn decode_bar_on_every_path(time: &str, time_zone: &Tz) -> Vec<(&'static str, Result<Bar, Error>)> {
    let fields = [time, "1", "1", "1", "1", "1", "1", "1"];

    let mut text = text_historical_data(&fields);
    let text_bars = decode_historical_data(server_versions::HISTORICAL_DATA_END, time_zone, &mut text).map(|data| data.bars[0].clone());

    let mut text_update = ResponseMessage::from(format!("90\09000\0-1\0{}\0", fields.join("\0")).as_str());
    let text_update = decode_historical_data_update(time_zone, &mut text_update);

    let bar = crate::proto::HistoricalDataBar {
        date: Some(time.to_string()),
        ..full_proto_bar()
    };
    let mut proto = proto_historical_data(bar.clone());
    let proto_bars = decode_historical_data(server_versions::PROTOBUF_HISTORICAL_DATA, time_zone, &mut proto).map(|data| data.bars[0].clone());

    let mut proto_update = proto_historical_data_update(bar);
    let proto_update = decode_historical_data_update(time_zone, &mut proto_update);

    vec![
        ("text data", text_bars),
        ("text update", text_update),
        ("protobuf data", proto_bars),
        ("protobuf update", proto_update),
    ]
}

fn proto_historical_data_update(bar: crate::proto::HistoricalDataBar) -> ResponseMessage {
    use prost::Message;
    let proto_msg = crate::proto::HistoricalDataUpdate {
        req_id: Some(9000),
        historical_data_bar: Some(bar),
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();
    ResponseMessage::from_protobuf(
        IncomingMessages::HistoricalDataUpdate as i32,
        bytes,
        server_versions::PROTOBUF_HISTORICAL_DATA,
    )
}

#[test]
fn test_bar_keeps_a_date_literal_on_every_path() {
    for (path, bar) in decode_bar_on_every_path("20261016", UTC) {
        let bar = bar.unwrap_or_else(|e| panic!("{path}: {e}"));
        assert_eq!(bar.date, datetime!(2026-10-16 0:00:00 UTC), "{path}");
        assert_eq!(bar.time_text.as_deref(), Some("20261016"), "{path}");
    }
}

#[test]
fn test_bar_keeps_an_epoch_literal_on_every_path() {
    let new_york: &Tz = time_tz::timezones::db::america::NEW_YORK;
    for (path, bar) in decode_bar_on_every_path("1681133400", new_york) {
        let bar = bar.unwrap_or_else(|e| panic!("{path}: {e}"));
        assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC), "{path}");
        assert_eq!(bar.time_text.as_deref(), Some("1681133400"), "{path}");
    }
}

#[test]
fn test_bar_at_midnight_utc_epoch_is_told_apart_from_a_date() {
    // Negative control: 1792108800 is 2026-10-16 00:00:00 UTC, so `date` is the
    // same as for "20261016" and cannot tell them apart. Only `time_text` can; a
    // literal recomposed from `date` would make both bars equal.
    let dates = decode_bar_on_every_path("20261016", UTC);
    let epochs = decode_bar_on_every_path("1792108800", UTC);
    for ((path, date_bar), (_, epoch_bar)) in dates.into_iter().zip(epochs) {
        let (date_bar, epoch_bar) = (date_bar.expect(path), epoch_bar.expect(path));
        assert_eq!(date_bar.date, epoch_bar.date, "{path}: both are midnight UTC");
        assert_eq!(date_bar.time_text.as_deref(), Some("20261016"), "{path}");
        assert_eq!(epoch_bar.time_text.as_deref(), Some("1792108800"), "{path}");
        assert_ne!(date_bar, epoch_bar, "{path}: a date and an epoch are different bars");
    }
}

#[test]
fn test_bar_time_literal_is_the_bytes_sent_not_a_recomposition() {
    // Negative control: "01681133400" decodes to the same instant as
    // "1681133400"; a literal rebuilt from `date` would drop the leading zero.
    for (path, bar) in decode_bar_on_every_path("01681133400", UTC) {
        let bar = bar.unwrap_or_else(|e| panic!("{path}: {e}"));
        assert_eq!(bar.date, datetime!(2023-04-10 13:30:00 UTC), "{path}");
        assert_eq!(bar.time_text.as_deref(), Some("01681133400"), "{path}");
    }
}

#[test]
fn test_bar_with_a_zoned_datetime_or_malformed_time_is_still_an_error() {
    // The decoder reads only `YYYYMMDD` and unix seconds (the request asks for
    // formatDate 2). A datetime with a zone, like any other unreadable time,
    // stays a decode error: no bar, so no literal is invented for it.
    for time in ["20261016 09:30:00 US/Eastern", "", "yesterday", "2026-10-16", "99999999999999999999"] {
        for (path, bar) in decode_bar_on_every_path(time, UTC) {
            assert!(bar.is_err(), "{path}: bar time {time:?} must not decode, got {bar:?}");
        }
    }
}
