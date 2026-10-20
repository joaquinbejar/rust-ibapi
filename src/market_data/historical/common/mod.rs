pub(crate) mod decoders;
pub(crate) mod encoders;

use time::OffsetDateTime;
use time_tz::Tz;

use crate::contracts::Contract;
use crate::market_data::historical::{is_historical_data_notice, HistoricalData, WhatToShow};
use crate::messages::{IncomingMessages, Notice, ResponseMessage};
use crate::protocol::{check_version, Features};
use crate::server_versions;
use crate::subscriptions::common::RoutedItem;
use crate::Error;

/// Assembles the answer to one historical bar request from what its channel
/// delivers, for the sync and async clients alike.
///
/// The answer is complete only on IB's own end marker (`HistoricalDataEnd`),
/// or, on servers older than `HISTORICAL_DATA_END`, on the data message that
/// then carries the window. A channel that closes or resets before that ends
/// the answer as [`Error::Interrupted`]: the bars received so far are never
/// returned as if they were complete.
///
/// Every error and notice IB sends for the request is kept, in order, with
/// its code and text. The informational ones
/// ([`is_historical_data_notice`]) do not end the answer; any other ends it as
/// [`Error::Notice`].
pub(crate) struct HistoricalAnswer {
    request_id: i32,
    server_version: i32,
    time_zone: &'static Tz,
    data: Option<HistoricalData>,
    notices: Vec<Notice>,
}

impl HistoricalAnswer {
    pub(crate) fn new(request_id: i32, server_version: i32, time_zone: &'static Tz) -> Self {
        Self {
            request_id,
            server_version,
            time_zone,
            data: None,
            notices: Vec::new(),
        }
    }

    /// Every error and notice IB sent for the request so far, in order, the
    /// one that ended it included.
    #[cfg_attr(not(feature = "async"), allow(dead_code))] // read by the async request handle
    pub(crate) fn notices(&self) -> &[Notice] {
        &self.notices
    }

    /// Take the next item read from the request's channel (`None` for a
    /// closed channel). Returns `None` while the answer is still open, and the
    /// call's result once it has ended.
    pub(crate) fn accept(&mut self, item: Option<RoutedItem>) -> Option<Result<HistoricalData, Error>> {
        match item {
            Some(RoutedItem::Response(mut message)) => self.accept_message(&mut message),
            Some(RoutedItem::Notice(notice)) | Some(RoutedItem::Error(Error::Notice(notice))) => self.accept_notice(notice),
            Some(RoutedItem::Error(Error::ConnectionReset)) | None => Some(Err(Error::Interrupted(self.request_id))),
            Some(RoutedItem::Error(e)) => Some(Err(e)),
        }
    }

    /// An IB code for this request: kept, and the end of the answer unless it
    /// is informational.
    fn accept_notice(&mut self, notice: Notice) -> Option<Result<HistoricalData, Error>> {
        self.notices.push(notice.clone());
        if is_historical_data_notice(notice.code) {
            None
        } else {
            Some(Err(Error::Notice(notice)))
        }
    }

    fn accept_message(&mut self, message: &mut ResponseMessage) -> Option<Result<HistoricalData, Error>> {
        match message.message_type() {
            IncomingMessages::HistoricalData if self.data.is_none() => {
                let data = match decoders::decode_historical_data(self.server_version, self.time_zone, message) {
                    Ok(data) => data,
                    Err(e) => return Some(Err(e)),
                };
                if self.server_version < server_versions::HISTORICAL_DATA_END {
                    // No end marker on these servers: the data message is the whole answer.
                    return Some(Ok(self.with_notices(data)));
                }
                self.data = Some(data);
                None
            }
            IncomingMessages::HistoricalDataEnd if self.data.is_some() => {
                let (start, end) = match decoders::decode_historical_data_end(self.server_version, self.time_zone, message) {
                    Ok(window) => window,
                    Err(e) => return Some(Err(e)),
                };
                let mut data = self.data.take()?;
                data.start = Some(start);
                data.end = Some(end);
                Some(Ok(self.with_notices(data)))
            }
            // An error frame that reached the channel undecoded (no dispatcher
            // classification): classified here the same way.
            IncomingMessages::Error => self.accept_notice(Notice::from(&*message)),
            _ => Some(Err(Error::unexpected_response(message))),
        }
    }

    fn with_notices(&self, mut data: HistoricalData) -> HistoricalData {
        data.notices = self.notices.clone();
        data
    }
}

/// Validate preconditions for historical_data requests.
pub(crate) fn validate_historical_data(
    server_version: i32,
    contract: &Contract,
    end_date: Option<OffsetDateTime>,
    what_to_show: Option<WhatToShow>,
) -> Result<(), Error> {
    if !contract.trading_class.is_empty() || contract.contract_id > 0 {
        check_version(server_version, Features::TRADING_CLASS)?;
    }

    if what_to_show == Some(WhatToShow::Schedule) {
        check_version(server_version, Features::HISTORICAL_SCHEDULE)?;
    }

    if end_date.is_some() && what_to_show == Some(WhatToShow::AdjustedLast) {
        return Err(Error::InvalidArgument(
            "end_date must be None when requesting WhatToShow::AdjustedLast.".into(),
        ));
    }

    Ok(())
}
