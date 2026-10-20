use super::*;
use crate::contracts::tick_types::TickType;

#[test]
fn test_next_raw_double() {
    let mut message = ResponseMessage::from_simple("1.25||-1.0|-2.0|abc|");

    assert_eq!(next_raw_double(&mut message).unwrap(), Some(1.25));
    assert_eq!(next_raw_double(&mut message).unwrap(), None, "an empty field is not 0.0");
    assert_eq!(next_raw_double(&mut message).unwrap(), Some(-1.0), "a sentinel is kept as sent");
    assert_eq!(next_raw_double(&mut message).unwrap(), Some(-2.0));
    assert!(next_raw_double(&mut message).is_err(), "a value that is not a number is an error");
}

#[test]
fn test_decode_option_computation() {
    // Message format: message_type, request_id, tick_type, tick_attribute, implied_vol, delta, option_price, dividend, gamma, vega, theta, underlying_price
    let mut message = ResponseMessage::from_simple("10|123|13|1|0.25|0.45|155.25|0.75|0.05|0.15|0.10|150.0|");

    let computation = decode_option_computation(server_versions::PRICE_BASED_VOLATILITY, &mut message).expect("error decoding option computation");

    assert_eq!(computation.field, TickType::ModelOption, "computation.field");
    assert_eq!(computation.tick_attribute, Some(1), "computation.tick_attribute");
    assert_eq!(computation.implied_volatility, Some(0.25), "computation.implied_volatility");
    assert_eq!(computation.delta, Some(0.45), "computation.delta");
    assert_eq!(computation.option_price, Some(155.25), "computation.option_price");
    assert_eq!(computation.present_value_dividend, Some(0.75), "computation.present_value_dividend");
    assert_eq!(computation.gamma, Some(0.05), "computation.gamma");
    assert_eq!(computation.vega, Some(0.15), "computation.vega");
    assert_eq!(computation.theta, Some(0.10), "computation.theta");
    assert_eq!(computation.underlying_price, Some(150.0), "computation.underlying_price");
}

// === D14 item 6: option computation, text path ===

fn decode_text_computation(fields: &str) -> Result<OptionComputation, Error> {
    let mut message = ResponseMessage::from_simple(fields);
    decode_option_computation(server_versions::PRICE_BASED_VOLATILITY, &mut message)
}

#[test]
fn test_option_computation_text_not_computed_sentinels_are_absent_and_kept_raw() {
    let computation = decode_text_computation("10|123|13|1|-1|-2|-1|-1|-2|-2|-2|-1|").expect("decodes");

    assert_eq!(
        (
            computation.implied_volatility,
            computation.delta,
            computation.option_price,
            computation.present_value_dividend,
            computation.gamma,
            computation.vega,
            computation.theta,
            computation.underlying_price
        ),
        (None, None, None, None, None, None, None, None)
    );
    let raw = computation.raw;
    assert_eq!(raw.tick_type, 13);
    assert_eq!(
        (raw.implied_volatility, raw.delta, raw.present_value_dividend, raw.theta),
        (Some(-1.0), Some(-2.0), Some(-1.0), Some(-2.0))
    );
}

#[test]
fn test_option_computation_text_empty_and_unset_values_are_absent() {
    // An empty field was 0.0 through next_double; IB's unset maximum is kept raw.
    let computation = decode_text_computation("10|123|13|1||1.7976931348623157E308|||||||").expect("decodes");

    assert_eq!(computation.implied_volatility, None);
    assert_eq!(computation.raw.implied_volatility, None, "empty: not sent");
    assert_eq!(computation.delta, None);
    assert_eq!(computation.raw.delta, Some(f64::MAX), "unset: kept as sent");
    assert_eq!(computation.underlying_price, None);
}

#[test]
fn test_option_computation_text_sentinels_are_per_field() {
    // Negative control: a theta of -1 and a stated zero are values.
    let computation = decode_text_computation("10|123|13|1|0|-0.5|5|0|0.1|0.2|-1|-2|").expect("decodes");

    assert_eq!(computation.implied_volatility, Some(0.0));
    assert_eq!(computation.delta, Some(-0.5));
    assert_eq!(computation.theta, Some(-1.0));
    assert_eq!(computation.underlying_price, Some(-2.0));
}

#[test]
fn test_option_computation_text_without_tick_type_is_an_error() {
    let result = decode_text_computation("10|123||1|0.25|0.5|5|0|0.1|0.2|-0.1|150|");
    assert!(matches!(result, Err(Error::Parse(..))), "got {result:?}");
}

use prost::Message;

#[test]
fn test_decode_contract_data_proto() {
    let proto_msg = crate::proto::ContractData {
        req_id: Some(1),
        contract: Some(crate::proto::Contract {
            con_id: Some(265598),
            symbol: Some("AAPL".into()),
            sec_type: Some("STK".into()),
            exchange: Some("SMART".into()),
            currency: Some("USD".into()),
            local_symbol: Some("AAPL".into()),
            trading_class: Some("NMS".into()),
            ..Default::default()
        }),
        contract_details: Some(crate::proto::ContractDetails {
            market_name: Some("NMS".into()),
            min_tick: Some("0.01".into()),
            long_name: Some("APPLE INC".into()),
            industry: Some("Technology".into()),
            category: Some("Computers".into()),
            subcategory: Some("Consumer Electronics".into()),
            ..Default::default()
        }),
    };

    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_contract_data_proto(&bytes).unwrap();
    assert_eq!(result.contract.contract_id, 265598);
    assert_eq!(result.contract.symbol.to_string(), "AAPL");
    assert_eq!(result.contract.currency.to_string(), "USD");
    assert_eq!(result.contract.local_symbol, "AAPL");
    assert_eq!(result.market_name, "NMS");
    assert_eq!(result.min_tick, 0.01);
    assert_eq!(result.long_name, "APPLE INC");
    assert_eq!(result.industry, "Technology");
    assert_eq!(result.category, "Computers");
    assert_eq!(result.subcategory, "Consumer Electronics");
}

/// Contract data with tag 4 (`last_trade_date_or_contract_month`) and, when given, tag 21 (`last_trade_date`).
fn contract_data_bytes(last_trade_date: Option<&str>) -> Vec<u8> {
    let proto_msg = crate::proto::ContractData {
        req_id: Some(1),
        contract: Some(crate::proto::Contract {
            con_id: Some(1),
            symbol: Some("SPX".into()),
            sec_type: Some("OPT".into()),
            last_trade_date_or_contract_month: Some("20261016".into()),
            last_trade_date: last_trade_date.map(String::from),
            ..Default::default()
        }),
        contract_details: Some(crate::proto::ContractDetails::default()),
    };
    proto_msg.encode_to_vec()
}

#[test]
fn test_decode_contract_data_proto_keeps_last_trade_date_verbatim() {
    let result = decode_contract_data_proto(&contract_data_bytes(Some("20261016 16:00:00 US/Eastern"))).unwrap();

    assert_eq!(result.contract.last_trade_date_or_contract_month, "20261016");
    assert_eq!(result.contract.last_trade_date.as_deref(), Some("20261016 16:00:00 US/Eastern"));
    assert_eq!(result.last_trade_time, "", "tag 21 does not fill ContractDetails.last_trade_time");
}

#[test]
fn test_decode_contract_data_proto_absent_last_trade_date_is_none() {
    let result = decode_contract_data_proto(&contract_data_bytes(None)).unwrap();

    assert_eq!(result.contract.last_trade_date_or_contract_month, "20261016");
    assert_eq!(result.contract.last_trade_date, None);
}

#[test]
fn test_decode_contract_data_proto_empty_last_trade_date_is_none() {
    let result = decode_contract_data_proto(&contract_data_bytes(Some(""))).unwrap();

    assert_eq!(result.contract.last_trade_date, None);
}

#[test]
fn test_decode_symbol_samples_proto() {
    let proto_msg = crate::proto::SymbolSamples {
        req_id: Some(1),
        contract_descriptions: vec![
            crate::proto::ContractDescription {
                contract: Some(crate::proto::Contract {
                    con_id: Some(265598),
                    symbol: Some("AAPL".into()),
                    sec_type: Some("STK".into()),
                    primary_exch: Some("NASDAQ".into()),
                    currency: Some("USD".into()),
                    ..Default::default()
                }),
                derivative_sec_types: vec!["OPT".into(), "WAR".into()],
            },
            crate::proto::ContractDescription {
                contract: Some(crate::proto::Contract {
                    con_id: Some(76792991),
                    symbol: Some("TSLA".into()),
                    sec_type: Some("STK".into()),
                    primary_exch: Some("NASDAQ".into()),
                    currency: Some("USD".into()),
                    ..Default::default()
                }),
                derivative_sec_types: vec![],
            },
        ],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_symbol_samples_proto(&bytes).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].contract.contract_id, 265598);
    assert_eq!(result[0].contract.symbol.to_string(), "AAPL");
    assert_eq!(result[0].derivative_security_types, vec!["OPT", "WAR"]);
    assert_eq!(result[1].contract.contract_id, 76792991);
    assert!(result[1].derivative_security_types.is_empty());
}

#[test]
fn test_decode_market_rule_proto() {
    let proto_msg = crate::proto::MarketRule {
        market_rule_id: Some(26),
        price_increments: vec![
            crate::proto::PriceIncrement {
                low_edge: Some(0.0),
                increment: Some(0.01),
            },
            crate::proto::PriceIncrement {
                low_edge: Some(1000.0),
                increment: Some(0.05),
            },
        ],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_market_rule_proto(&bytes).unwrap();
    assert_eq!(result.market_rule_id, 26);
    assert_eq!(result.price_increments.len(), 2);
    assert_eq!(result.price_increments[0].low_edge, 0.0);
    assert_eq!(result.price_increments[0].increment, 0.01);
    assert_eq!(result.price_increments[1].low_edge, 1000.0);
    assert_eq!(result.price_increments[1].increment, 0.05);
}

#[test]
fn test_decode_option_chain_proto() {
    let proto_msg = crate::proto::SecDefOptParameter {
        req_id: Some(1),
        exchange: Some("SMART".into()),
        underlying_con_id: Some(265598),
        trading_class: Some("AAPL".into()),
        multiplier: Some("100".into()),
        expirations: vec!["20260619".into(), "20260918".into()],
        strikes: vec![150.0, 175.0, 200.0],
    };
    let mut bytes = Vec::new();
    proto_msg.encode(&mut bytes).unwrap();

    let result = decode_option_chain_proto(&bytes).unwrap();
    assert_eq!(result.exchange, "SMART");
    assert_eq!(result.underlying_contract_id, 265598);
    assert_eq!(result.trading_class, "AAPL");
    assert_eq!(result.multiplier, "100");
    assert_eq!(result.expirations, vec!["20260619", "20260918"]);
    assert_eq!(result.strikes, vec![150.0, 175.0, 200.0]);
}

// Servers ≥ PROTOBUF_SCAN_DATA (210) always emit ContractData / SymbolSamples /
// MarketRule / SecurityDefinitionOptionParameter in proto. Text-framed arrival
// skip-classifies via `UnexpectedResponse` (rule 20) rather than terminating
// the subscription.

#[test]
fn test_decode_contract_details_rejects_text_framing() {
    let mut message = ResponseMessage::from("10\09001\0AAPL\0STK\0\00\0\0SMART\0USD\0AAPL\0NMS\0NMS\0265598\00.01\0\0");
    let err = decode_contract_details(server_versions::PROTOBUF_SCAN_DATA, &mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_contract_descriptions_rejects_text_framing() {
    let mut message = ResponseMessage::from("79\09000\01\012345\0AAPL\0STK\0NASDAQ\0USD\00\0APPLE INC\0\0");
    let err = decode_contract_descriptions(server_versions::PROTOBUF_SCAN_DATA, &mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_market_rule_rejects_text_framing() {
    let mut message = ResponseMessage::from("87\026\01\00\00.01\0");
    let err = decode_market_rule(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}

#[test]
fn test_decode_option_chain_rejects_text_framing() {
    let mut message = ResponseMessage::from("75\09000\0SMART\0265598\0AAPL\0100\01\020260619\01\0150.0\0");
    let err = decode_option_chain(&mut message).expect_err("text framing must be rejected");
    assert!(
        matches!(err, Error::UnexpectedResponse(_)),
        "expected Error::UnexpectedResponse, got {err:?}"
    );
}
