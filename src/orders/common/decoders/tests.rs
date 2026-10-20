use super::*;
use crate::contracts::Symbol;
use crate::messages::ResponseMessage;
use crate::orders::{Action, OrderStatusKind};
use crate::server_versions;

#[test]
fn test_decode_open_order_proto() {
    use prost::Message;

    let proto_msg = crate::proto::OpenOrder {
        order_id: Some(42),
        contract: Some(crate::proto::Contract {
            con_id: Some(265598),
            symbol: Some("AAPL".into()),
            sec_type: Some("STK".into()),
            exchange: Some("SMART".into()),
            currency: Some("USD".into()),
            ..Default::default()
        }),
        order: Some(crate::proto::Order {
            order_id: Some(42),
            action: Some("BUY".into()),
            total_quantity: Some("100".into()),
            order_type: Some("LMT".into()),
            lmt_price: Some(150.0),
            ..Default::default()
        }),
        order_state: Some(crate::proto::OrderState {
            status: Some("Submitted".into()),
            ..Default::default()
        }),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_open_order_proto(&bytes).unwrap();
    assert_eq!(result.order_id, 42);
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.contract.symbol.to_string(), "AAPL");
    assert_eq!(result.order.order_id, 42);
    assert_eq!(result.order.action, Action::Buy);
    assert_eq!(result.order.total_quantity, 100.0);
    assert_eq!(result.order.order_type, "LMT");
    assert_eq!(result.order.limit_price, Some(150.0));
    assert_eq!(result.order_state.status, OrderStatusKind::Submitted);
}

#[test]
fn test_decode_order_status_proto() {
    use prost::Message;

    let proto_msg = crate::proto::OrderStatus {
        order_id: Some(99),
        status: Some("Filled".into()),
        filled: Some("50".into()),
        remaining: Some("0".into()),
        avg_fill_price: Some(152.5),
        perm_id: Some(123456),
        parent_id: Some(10),
        last_fill_price: Some(152.75),
        client_id: Some(7),
        why_held: Some("locate".into()),
        mkt_cap_price: Some(1.23),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_order_status_proto(&bytes).unwrap();
    assert_eq!(result.order_id, 99);
    assert_eq!(result.status, OrderStatusKind::Filled);
    assert_eq!(result.filled, Some(50.0));
    assert_eq!(result.remaining, Some(0.0));
    assert_eq!(result.average_fill_price, Some(152.5));
    assert_eq!(result.perm_id, 123456);
    assert_eq!(result.parent_id, 10);
    assert_eq!(result.last_fill_price, Some(152.75));
    assert_eq!(result.client_id, 7);
    assert_eq!(result.why_held, "locate");
    assert_eq!(result.market_cap_price, Some(1.23));
}

#[test]
fn test_decode_order_status_proto_missing_doubles() {
    // Regression: previously decoded to Some(0.0) via unwrap_or_default().
    use prost::Message;

    let proto_msg = crate::proto::OrderStatus {
        order_id: Some(99),
        status: Some("Submitted".into()),
        filled: Some("0".into()),
        remaining: Some("100".into()),
        avg_fill_price: None,
        perm_id: Some(123456),
        parent_id: Some(0),
        last_fill_price: None,
        client_id: Some(7),
        why_held: None,
        mkt_cap_price: None,
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_order_status_proto(&bytes).unwrap();
    assert_eq!(result.average_fill_price, None);
    assert_eq!(result.last_fill_price, None);
    assert_eq!(result.market_cap_price, None);
}

#[test]
fn test_decode_order_status_proto_rejects_empty_status() {
    // Missing or empty status must error rather than silently defaulting to
    // Submitted; matches the text decoder which fails on empty status fields.
    use prost::Message;

    for status in [None, Some(String::new())] {
        let proto_msg = crate::proto::OrderStatus {
            order_id: Some(99),
            status,
            filled: Some("0".into()),
            remaining: Some("100".into()),
            avg_fill_price: None,
            perm_id: Some(1),
            parent_id: Some(0),
            last_fill_price: None,
            client_id: Some(0),
            why_held: None,
            mkt_cap_price: None,
        };

        let mut bytes = Vec::new();
        proto_msg.encode(&mut bytes).unwrap();

        assert!(matches!(decode_order_status_proto(&bytes), Err(crate::Error::Parse(..))));
    }
}

#[test]
fn test_decode_commission_report_proto() {
    use prost::Message;

    let proto_msg = crate::proto::CommissionAndFeesReport {
        exec_id: Some("exec123".into()),
        commission_and_fees: Some(1.25),
        currency: Some("USD".into()),
        realized_pnl: Some(500.0),
        bond_yield: Some(f64::MAX),
        yield_redemption_date: Some("20260101".into()),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_commission_report_proto(&bytes).unwrap();
    assert_eq!(result.execution_id, "exec123");
    assert_eq!(result.commission, Some(1.25));
    assert_eq!(result.currency, "USD");
    assert_eq!(result.realized_pnl, Some(500.0));
    assert_eq!(result.yields, None); // f64::MAX filtered out
    assert_eq!(result.yield_redemption_date, "20260101");
}

#[test]
fn test_decode_execution_data_proto() {
    use prost::Message;

    let proto_msg = crate::proto::ExecutionDetails {
        req_id: Some(42),
        contract: Some(crate::proto::Contract {
            con_id: Some(265598),
            symbol: Some("AAPL".into()),
            sec_type: Some("STK".into()),
            ..Default::default()
        }),
        execution: Some(crate::proto::Execution {
            order_id: Some(100),
            exec_id: Some("exec001".into()),
            time: Some("20260101 12:00:00".into()),
            acct_number: Some("DU1234".into()),
            side: Some("BOT".into()),
            shares: Some("50".into()),
            price: Some(152.5),
            perm_id: Some(99999),
            ..Default::default()
        }),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_execution_data_proto(&bytes).unwrap();
    assert_eq!(result.request_id, 42);
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.execution.execution_id, "exec001");
    assert_eq!(result.execution.shares, 50.0);
    assert_eq!(result.execution.price, 152.5);
    assert_eq!(result.execution.perm_id, 99999);
    assert_eq!(result.execution.side, crate::orders::ExecutionSide::Bought);
}

#[test]
fn test_decode_completed_order_proto() {
    use prost::Message;

    let proto_msg = crate::proto::CompletedOrder {
        contract: Some(crate::proto::Contract {
            con_id: Some(265598),
            symbol: Some("AAPL".into()),
            sec_type: Some("STK".into()),
            ..Default::default()
        }),
        order: Some(crate::proto::Order {
            order_id: Some(200),
            action: Some("SELL".into()),
            total_quantity: Some("200".into()),
            order_type: Some("MKT".into()),
            ..Default::default()
        }),
        order_state: Some(crate::proto::OrderState {
            status: Some("Filled".into()),
            completed_time: Some("20260101 12:00:00".into()),
            completed_status: Some("Filled".into()),
            ..Default::default()
        }),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_completed_order_proto(&bytes).unwrap();
    // Completed orders always report `order_id = -1` (legacy text-decoder sentinel).
    assert_eq!(result.order_id, -1);
    assert_eq!(result.order.order_id, 200);
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.contract.symbol, Symbol::from("AAPL"));
    assert_eq!(result.order.action, Action::Sell);
    assert_eq!(result.order_state.completed_time, "20260101 12:00:00");
}

// =============================================================================
// Builder → production-decoder integration tests
// =============================================================================

#[test]
fn test_decode_open_order_proto_round_trips_via_builder() {
    use crate::testdata::builders::orders::open_order;
    use crate::testdata::builders::ResponseProtoEncoder;

    let bytes = open_order()
        .order_id(42)
        .contract_id(265598)
        .symbol("AAPL")
        .security_type("STK")
        .order_type("LMT")
        .limit_price(Some(150.0))
        .status(OrderStatusKind::Submitted)
        .encode_proto();

    let result = super::decode_open_order_proto(&bytes).unwrap();
    assert_eq!(result.order_id, 42);
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.contract.symbol, Symbol::from("AAPL"));
    assert_eq!(result.order.action, Action::Buy);
    assert_eq!(result.order.order_type, "LMT");
    assert_eq!(result.order.limit_price, Some(150.0));
    assert_eq!(result.order_state.status, OrderStatusKind::Submitted);
}

#[test]
fn test_decode_completed_order_proto_round_trips_via_builder() {
    use crate::testdata::builders::orders::completed_order;
    use crate::testdata::builders::ResponseProtoEncoder;

    let bytes = completed_order()
        .symbol("AAPL")
        .security_type("STK")
        .total_quantity(100.0)
        .completed_time("20260101 12:00:00")
        .completed_status("Filled by Trader")
        .encode_proto();

    let result = super::decode_completed_order_proto(&bytes).unwrap();
    assert_eq!(result.contract.symbol, Symbol::from("AAPL"));
    assert_eq!(result.order.total_quantity, 100.0);
    assert_eq!(result.order_state.status, OrderStatusKind::Filled);
    assert_eq!(result.order_state.completed_time, "20260101 12:00:00");
    assert_eq!(result.order_state.completed_status, "Filled by Trader");
}

#[test]
fn test_decode_order_status_proto_round_trips_via_builder() {
    use crate::testdata::builders::orders::order_status;
    use crate::testdata::builders::ResponseProtoEncoder;

    let bytes = order_status()
        .order_id(99)
        .status(OrderStatusKind::Filled)
        .filled(50.0)
        .remaining(0.0)
        .average_fill_price(Some(152.5))
        .perm_id(123456)
        .last_fill_price(Some(152.75))
        .client_id(7)
        .market_cap_price(Some(1.23))
        .encode_proto();

    let result = super::decode_order_status_proto(&bytes).unwrap();
    assert_eq!(result.order_id, 99);
    assert_eq!(result.status, OrderStatusKind::Filled);
    assert_eq!(result.filled, Some(50.0));
    assert_eq!(result.remaining, Some(0.0));
    assert_eq!(result.average_fill_price, Some(152.5));
    assert_eq!(result.perm_id, 123456);
    assert_eq!(result.last_fill_price, Some(152.75));
    assert_eq!(result.client_id, 7);
    assert_eq!(result.market_cap_price, Some(1.23));
}

#[test]
fn test_decode_commission_report_proto_round_trips_via_builder() {
    use crate::testdata::builders::orders::commission_report;
    use crate::testdata::builders::ResponseProtoEncoder;

    let bytes = commission_report()
        .execution_id("exec123")
        .commission(1.25)
        .currency("USD")
        .realized_pnl(Some(500.0))
        .yields(Some(f64::MAX))
        .encode_proto();

    let result = super::decode_commission_report_proto(&bytes).unwrap();
    assert_eq!(result.execution_id, "exec123");
    assert_eq!(result.commission, Some(1.25));
    assert_eq!(result.currency, "USD");
    assert_eq!(result.realized_pnl, Some(500.0));
    assert_eq!(result.yields, None); // f64::MAX is the IBKR sentinel for "unset"
}

#[test]
fn test_decode_execution_data_proto_round_trips_via_builder() {
    use crate::testdata::builders::orders::execution_data;
    use crate::testdata::builders::ResponseProtoEncoder;

    let bytes = execution_data()
        .request_id(42)
        .order_id(100)
        .contract_id(265598)
        .symbol("AAPL")
        .security_type("STK")
        .execution_id("exec001")
        .side("BOT")
        .shares(50.0)
        .price(152.5)
        .perm_id(99999)
        .encode_proto();

    let result = super::decode_execution_data_proto(&bytes).unwrap();
    assert_eq!(result.request_id, 42);
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.execution.execution_id, "exec001");
    assert_eq!(result.execution.shares, 50.0);
    assert_eq!(result.execution.price, 152.5);
    assert_eq!(result.execution.perm_id, 99999);
    assert_eq!(result.execution.side, crate::orders::ExecutionSide::Bought);
}

// =============================================================================
// Text-framing rejection (rule 20)
// =============================================================================
//
// Servers ≥ floor (PROTOBUF_SCAN_DATA = 210) always emit these messages in
// proto framing. Text-framed arrival skip-classifies via UnexpectedResponse
// rather than terminating the subscription.

#[test]
fn test_decode_open_order_rejects_text_framing() {
    let _ = server_versions::PROTOBUF_SCAN_DATA;
    let mut message = ResponseMessage::from("5\013\076792991\0AAPL\0STK\0");
    let err = decode_open_order(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_completed_order_rejects_text_framing() {
    let mut message = ResponseMessage::from("101\0265598\0AAPL\0STK\0");
    let err = decode_completed_order(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_execution_data_rejects_text_framing() {
    let mut message = ResponseMessage::from("11\09000\042\0265598\0AAPL\0STK\0");
    let err = decode_execution_data(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_commission_report_rejects_text_framing() {
    let mut message = ResponseMessage::from("59\01\0exec001\02.5\0USD\0");
    let err = decode_commission_report(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_order_status_rejects_text_framing() {
    let mut message = ResponseMessage::from("3\013\0PreSubmitted\00\0100\00.0\01376327563\00\00.0\0100\0\00.0\0");
    let err = decode_order_status(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_open_order_without_an_action_is_an_error_not_a_buy() {
    // An order the broker described without a side. The decoder used to
    // default it to BUY, so a working sell described this way would have been
    // reported as a buy.
    use prost::Message;

    let proto_msg = crate::proto::OpenOrder {
        order_id: Some(42),
        contract: Some(crate::proto::Contract {
            con_id: Some(265598),
            ..Default::default()
        }),
        order: Some(crate::proto::Order {
            order_id: Some(42),
            action: None,
            total_quantity: Some("100".into()),
            ..Default::default()
        }),
        order_state: Some(crate::proto::OrderState {
            status: Some("Submitted".into()),
            ..Default::default()
        }),
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_open_order_proto(&bytes);
    assert!(result.is_err(), "an order without a side decoded as {:?}", result.map(|o| o.order.action));
}

#[test]
fn test_decode_open_order_with_an_unknown_action_is_an_error_not_a_panic() {
    use prost::Message;

    let proto_msg = crate::proto::OpenOrder {
        order_id: Some(42),
        order: Some(crate::proto::Order {
            order_id: Some(42),
            action: Some("HOLD".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    assert!(decode_open_order_proto(&bytes).is_err());
}

#[test]
fn test_decode_commission_report_without_a_commission_is_none_not_zero() {
    // A fee that was not reported and a fee of nothing are different facts,
    // and only the second may be added to a total.
    use prost::Message;

    let absent = crate::proto::CommissionAndFeesReport {
        exec_id: Some("0001.01".into()),
        commission_and_fees: None,
        currency: Some("USD".into()),
        ..Default::default()
    };
    let mut bytes = Vec::new();
    absent.encode(&mut bytes).unwrap();
    let result = decode_commission_report_proto(&bytes).unwrap();
    assert_eq!(result.commission, None);

    let zero = crate::proto::CommissionAndFeesReport {
        exec_id: Some("0001.01".into()),
        commission_and_fees: Some(0.0),
        currency: Some("USD".into()),
        ..Default::default()
    };
    let mut bytes = Vec::new();
    zero.encode(&mut bytes).unwrap();
    let result = decode_commission_report_proto(&bytes).unwrap();
    assert_eq!(result.commission, Some(0.0));
}

// Fill counts keep three cases apart: absent, a stated number (zero
// included), and malformed. `parse_f64` answered 0.0 for all three, which
// read an order IB said nothing about as one that filled nothing.

fn order_status_bytes(filled: Option<&str>, remaining: Option<&str>) -> Vec<u8> {
    use prost::Message;
    crate::proto::OrderStatus {
        order_id: Some(7),
        status: Some("Submitted".into()),
        filled: filled.map(str::to_owned),
        remaining: remaining.map(str::to_owned),
        perm_id: Some(700001),
        client_id: Some(101),
        ..Default::default()
    }
    .encode_to_vec()
}

#[test]
fn test_order_status_counts_keep_absent_zero_and_stated_apart() {
    let absent = super::decode_order_status_proto(&order_status_bytes(None, None)).unwrap();
    assert_eq!(absent.filled, None);
    assert_eq!(absent.remaining, None);
    let empty = super::decode_order_status_proto(&order_status_bytes(Some(""), Some(""))).unwrap();
    assert_eq!(empty.filled, None, "an empty string states nothing");
    let zero = super::decode_order_status_proto(&order_status_bytes(Some("0"), Some("10"))).unwrap();
    assert_eq!(zero.filled, Some(0.0), "a stated zero is a zero");
    assert_eq!(zero.remaining, Some(10.0));
    let decimal = super::decode_order_status_proto(&order_status_bytes(Some("2.5"), Some("7.5"))).unwrap();
    assert_eq!(decimal.filled, Some(2.5));
    assert_eq!(decimal.remaining, Some(7.5));
    // IB's unset maximum is no count.
    let unset = super::decode_order_status_proto(&order_status_bytes(Some(&f64::MAX.to_string()), None)).unwrap();
    assert_eq!(unset.filled, None);
}

#[test]
fn test_order_status_with_a_malformed_count_fails_to_decode() {
    for (filled, remaining) in [(Some("ten"), Some("0")), (Some("1"), Some("NaN")), (Some("inf"), None)] {
        let decoded = super::decode_order_status_proto(&order_status_bytes(filled, remaining));
        assert!(
            matches!(decoded, Err(Error::Parse(..))),
            "{filled:?} {remaining:?} decoded as {decoded:?}"
        );
    }
}

fn order_with_filled(filled_quantity: Option<&str>) -> crate::proto::Order {
    crate::proto::Order {
        client_id: Some(101),
        order_id: Some(7),
        perm_id: Some(700001),
        action: Some("BUY".into()),
        total_quantity: Some("10".into()),
        order_type: Some("LMT".into()),
        filled_quantity: filled_quantity.map(str::to_owned),
        ..Default::default()
    }
}

#[test]
fn test_open_and_completed_orders_keep_the_filled_quantity_cases_apart() {
    use prost::Message;
    let open = |filled: Option<&str>| {
        crate::proto::OpenOrder {
            order_id: Some(7),
            order: Some(order_with_filled(filled)),
            ..Default::default()
        }
        .encode_to_vec()
    };
    let completed = |filled: Option<&str>| {
        crate::proto::CompletedOrder {
            order: Some(order_with_filled(filled)),
            ..Default::default()
        }
        .encode_to_vec()
    };
    assert_eq!(super::decode_open_order_proto(&open(None)).unwrap().order.filled_quantity, None);
    assert_eq!(super::decode_open_order_proto(&open(Some("0"))).unwrap().order.filled_quantity, Some(0.0));
    assert_eq!(super::decode_open_order_proto(&open(Some("3"))).unwrap().order.filled_quantity, Some(3.0));
    assert!(matches!(super::decode_open_order_proto(&open(Some("x"))), Err(Error::Parse(..))));
    assert_eq!(super::decode_completed_order_proto(&completed(None)).unwrap().order.filled_quantity, None);
    assert_eq!(
        super::decode_completed_order_proto(&completed(Some("0"))).unwrap().order.filled_quantity,
        Some(0.0)
    );
    assert!(matches!(
        super::decode_completed_order_proto(&completed(Some("-"))),
        Err(Error::Parse(..))
    ));
}

#[test]
fn test_a_text_framed_order_status_is_refused_not_decoded() {
    // The connection floor is PROTOBUF_SCAN_DATA: a text-framed status has no
    // supported decoding, so its counts are never read as text zeros.
    let mut message = ResponseMessage::from("3\07\0Submitted\00\010\0");
    assert!(super::decode_order_status(&mut message).is_err());
}
