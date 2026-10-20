use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log::{debug, error, warn};
use time::OffsetDateTime;

use crate::client::ClientRequestBuilders;
use crate::contracts::Contract;
use crate::messages::{IncomingMessages, Notice};
use crate::protocol::{check_version, Features};
use crate::subscriptions::r#async::Subscription;
use crate::transport::{AsyncInternalSubscription, AsyncMessageBus};
use crate::{Client, Error};

use super::common::{self, decoders, encoders};
use super::{BarSize, Duration, HistogramEntry, HistoricalBarUpdate, HistoricalData, Schedule, TickDecoder, WhatToShow};
use crate::market_data::TradingHours;

// === Public API Functions ===

impl Client {
    /// Returns the timestamp of earliest available historical data for a contract and data type.
    ///
    /// # Arguments
    /// * `contract` - [Contract] to retrieve the head timestamp for.
    /// * `what_to_show` - requested bar type: [WhatToShow].
    /// * `trading_hours` - Use [TradingHours::Regular] for data generated only during regular trading hours, or [TradingHours::Extended] to include data from outside regular trading hours.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::Client;
    /// use ibapi::contracts::Contract;
    /// use ibapi::market_data::historical::WhatToShow;
    /// use ibapi::market_data::TradingHours;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///
    ///     let contract = Contract::stock("MSFT").build();
    ///     let result = client
    ///         .head_timestamp(&contract, WhatToShow::Trades, TradingHours::Regular)
    ///         .await
    ///         .expect("head timestamp failed");
    ///
    ///     println!("head_timestamp: {result:?}");
    /// }
    /// ```
    pub async fn head_timestamp(&self, contract: &Contract, what_to_show: WhatToShow, trading_hours: TradingHours) -> Result<OffsetDateTime, Error> {
        check_version(self.server_version(), Features::HEAD_TIMESTAMP)?;

        let builder = self.request();
        let request_id = builder.request_id();
        let request = encoders::encode_request_head_timestamp(request_id, contract, what_to_show, trading_hours.use_rth())?;
        let mut subscription = builder.send_raw(request).await?;

        // One request, one answer: a reset is reported, never answered by a new request.
        match subscription.next().await {
            Some(Ok(mut message)) if message.message_type() == IncomingMessages::HeadTimestamp => {
                Ok(decoders::decode_head_timestamp(&mut message, self.time_zone())?)
            }
            Some(Ok(message)) => Err(Error::unexpected_response(&message)),
            Some(Err(Error::ConnectionReset)) | None => Err(Error::Interrupted(request_id)),
            Some(Err(e)) => Err(e),
        }
    }

    /// Build a request for historical bar data.
    ///
    /// Required: a date spec via either [`HistoricalDataBuilder::duration`](super::HistoricalDataBuilder::duration)
    /// (with optional [`HistoricalDataBuilder::ending`](super::HistoricalDataBuilder::ending)) or
    /// [`HistoricalDataBuilder::between`](super::HistoricalDataBuilder::between). Terminals:
    /// [`HistoricalDataBuilder::fetch`](super::HistoricalDataBuilder::fetch) for a one-shot
    /// [`HistoricalData`] result; [`HistoricalDataBuilder::stream`](super::HistoricalDataBuilder::stream)
    /// for a `Subscription<HistoricalBarUpdate>` that yields bars as they arrive.
    ///
    /// # Arguments
    /// * `contract` - Contract object that is subject of query
    /// * `bar_size` - Bar size (resolution)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::prelude::*;
    /// use time::macros::datetime;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///     let contract = Contract::stock("AAPL").build();
    ///
    ///     // IBKR-native: amount of data ending at a specific time (or now)
    ///     let bars = client
    ///         .historical_data(&contract, HistoricalBarSize::Hour)
    ///         .what_to_show(HistoricalWhatToShow::Trades)
    ///         .duration(7.days())
    ///         .fetch()
    ///         .await
    ///         .expect("historical data request failed");
    ///
    ///     // Convenience: explicit date range (computes duration internally)
    ///     let bars = client
    ///         .historical_data(&contract, HistoricalBarSize::Hour)
    ///         .between(datetime!(2023-04-08 0:00 UTC), datetime!(2023-04-15 0:00 UTC))
    ///         .fetch()
    ///         .await
    ///         .expect("historical data request failed");
    ///     let _ = bars;
    /// }
    /// ```
    pub fn historical_data<'a>(&'a self, contract: &'a Contract, bar_size: BarSize) -> super::HistoricalDataBuilder<'a, Self> {
        super::HistoricalDataBuilder::new(self, contract, bar_size)
    }

    /// Build a request for [`Schedule`] data over the given duration.
    ///
    /// Defaults to anchoring at the current time. Use [`HistoricalScheduleBuilder::ending`](super::HistoricalScheduleBuilder::ending)
    /// to anchor at a specific end date.
    ///
    /// # Arguments
    /// * `contract` - [Contract] to retrieve [Schedule] for.
    /// * `duration` - [Duration] of the interval to retrieve.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::prelude::*;
    /// use time::macros::datetime;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///     let contract = Contract::stock("GM").build();
    ///
    ///     // Ending now:
    ///     let schedule = client
    ///         .historical_schedules(&contract, 30.days())
    ///         .fetch()
    ///         .await
    ///         .expect("historical schedule request failed");
    ///
    ///     // Anchored to a specific end date:
    ///     let schedule = client
    ///         .historical_schedules(&contract, 30.days())
    ///         .ending(datetime!(2023-04-15 0:00 UTC))
    ///         .fetch()
    ///         .await
    ///         .expect("historical schedule request failed");
    ///
    ///     for session in &schedule.sessions {
    ///         println!("{session:?}");
    ///     }
    /// }
    /// ```
    pub fn historical_schedules<'a>(&'a self, contract: &'a Contract, duration: Duration) -> super::HistoricalScheduleBuilder<'a, Self> {
        super::HistoricalScheduleBuilder::new(self, contract, duration)
    }

    /// Build a request for historical time & sales data (tick-by-tick).
    ///
    /// The terminal method selects the tick type:
    /// [`HistoricalTicksBuilder::trade`](super::HistoricalTicksBuilder::trade) /
    /// `.mid_point()` / `.bid_ask(IgnoreSize)`. Use
    /// [`HistoricalTicksBuilder::starting`](super::HistoricalTicksBuilder::starting) /
    /// `.ending()` to anchor the query (at least one is required per IBKR).
    ///
    /// # Arguments
    /// * `contract` - [Contract] object that is subject of query
    /// * `number_of_ticks` - Number of distinct data points. Max currently 1000 per request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::prelude::*;
    /// use ibapi::market_data::IgnoreSize;
    /// use time::macros::datetime;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///     let contract = Contract::stock("TSLA").build();
    ///
    ///     // Trade ticks anchored at a start date:
    ///     let mut trades = client
    ///         .historical_ticks(&contract, 100)
    ///         .starting(datetime!(2023-04-15 0:00 UTC))
    ///         .trade()
    ///         .await
    ///         .expect("historical ticks request failed");
    ///
    ///     while let Some(tick) = trades.next().await {
    ///         println!("{tick:?}");
    ///     }
    ///
    ///     // Bid/ask ticks anchored at an end date, ignoring tick sizes:
    ///     let _quotes = client
    ///         .historical_ticks(&contract, 100)
    ///         .ending(datetime!(2023-04-15 0:00 UTC))
    ///         .bid_ask(IgnoreSize::Yes)
    ///         .await
    ///         .expect("historical ticks request failed");
    /// }
    /// ```
    pub fn historical_ticks<'a>(&'a self, contract: &'a Contract, number_of_ticks: i32) -> super::HistoricalTicksBuilder<'a, Self> {
        super::HistoricalTicksBuilder::new(self, contract, number_of_ticks)
    }

    /// Cancels an in-flight historical ticks request.
    ///
    /// # Arguments
    /// * `request_id` - The request ID of the historical ticks subscription to cancel.
    pub async fn cancel_historical_ticks(&self, request_id: i32) -> Result<(), Error> {
        check_version(self.server_version(), Features::CANCEL_CONTRACT_DATA)?;

        let message = encoders::encode_cancel_historical_ticks(request_id)?;
        self.send_message(message).await?;
        Ok(())
    }

    /// Requests data histogram of specified contract.
    ///
    /// # Arguments
    /// * `contract`      - [Contract] to retrieve [HistogramEntry] data for.
    /// * `trading_hours` - Regular trading hours only, or include extended hours.
    /// * `period`        - The time period of each histogram bar (e.g., `BarSize::Day`, `BarSize::Week`, `BarSize::Month`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::Client;
    /// use ibapi::contracts::Contract;
    /// use ibapi::market_data::historical::BarSize;
    /// use ibapi::market_data::TradingHours;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///
    ///     let contract = Contract::stock("GM").build();
    ///     let histogram = client
    ///         .histogram_data(&contract, TradingHours::Regular, BarSize::Week)
    ///         .await
    ///         .expect("histogram request failed");
    ///
    ///     for item in &histogram {
    ///         println!("{item:?}");
    ///     }
    /// }
    /// ```
    pub async fn histogram_data(&self, contract: &Contract, trading_hours: TradingHours, period: BarSize) -> Result<Vec<HistogramEntry>, Error> {
        check_version(self.server_version(), Features::HISTOGRAM)?;

        let builder = self.request();
        let request_id = builder.request_id();
        let request = encoders::encode_request_histogram_data(request_id, contract, trading_hours.use_rth(), period)?;
        let mut subscription = builder.send_raw(request).await?;

        match subscription.next().await {
            Some(Ok(mut message)) => decoders::decode_histogram_data(&mut message),
            Some(Err(Error::ConnectionReset)) | None => Err(Error::Interrupted(request_id)),
            Some(Err(e)) => Err(e),
        }
    }
}

pub(crate) fn time_zone(client: &Client) -> &'static time_tz::Tz {
    if let Some(tz) = client.time_zone {
        tz
    } else {
        warn!("server timezone unknown. assuming UTC, but that may be incorrect!");
        time_tz::timezones::db::UTC
    }
}

/// Send one historical bar request and return its handle.
pub(crate) async fn historical_data(
    client: &Client,
    contract: &Contract,
    end_date: Option<OffsetDateTime>,
    duration: Duration,
    bar_size: BarSize,
    what_to_show: WhatToShow,
    trading_hours: TradingHours,
) -> Result<HistoricalDataRequest, Error> {
    common::validate_historical_data(client.server_version(), contract, end_date, Some(what_to_show))?;

    let builder = client.request();
    let request_id = builder.request_id();
    let request = encoders::encode_request_historical_data(
        request_id,
        contract,
        end_date,
        duration,
        bar_size,
        Some(what_to_show),
        trading_hours.use_rth(),
        false,
        &Vec::<crate::contracts::TagValue>::default(),
    )?;

    let subscription = builder.send_raw(request).await?;

    Ok(HistoricalDataRequest {
        request_id,
        subscription,
        message_bus: Arc::clone(&client.message_bus),
        answer: common::HistoricalAnswer::new(request_id, client.server_version(), time_zone(client)),
        state: RequestState::Open,
        ended: None,
    })
}

// === HistoricalDataRequest ===

/// Where a [`HistoricalDataRequest`] stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestState {
    /// Sent, and neither ended by IB nor cancelled: IB may still be serving
    /// it, even after a local error ended the answer.
    Open,
    /// Not live at IB: IB ended it (its end marker, or a code that ends the
    /// request), or the session it was sent on is gone (interrupted).
    Ended,
    /// A cancel request was written for it.
    Cancelled,
}

/// One historical bar request in flight: its IB request id, its answer, and
/// its cancellation.
///
/// Returned by [`HistoricalDataBuilder::send`](super::HistoricalDataBuilder::send).
/// The request was written when this handle was returned; nothing else is
/// ever sent for it except a cancel, and it is never retried.
///
/// - [`request_id`](Self::request_id) is IB's id for it, the one every answer,
///   error and notice for it carries.
/// - [`answer`](Self::answer) waits for the answer, which is complete only on
///   IB's end marker. It is cancel-safe: a caller can bound it with its own
///   timer, and what arrived so far is kept for the next call.
/// - [`answer_within`](Self::answer_within) is the same with a per-call
///   timeout. A timeout leaves the request open at IB: call
///   [`cancel`](Self::cancel) to end it there.
/// - [`cancel`](Self::cancel) writes `cancelHistoricalData` for this id and
///   says whether it was written.
///
/// Dropping an open handle (not ended by IB, not cancelled) sends the cancel on a
/// spawned task, as [`Subscription`](crate::subscriptions::Subscription)
/// does; call [`cancel`](Self::cancel) to know whether it went out.
#[must_use = "HistoricalDataRequest must be answered or cancelled; dropping it while open cancels the request"]
pub struct HistoricalDataRequest {
    request_id: i32,
    subscription: AsyncInternalSubscription,
    message_bus: Arc<dyn AsyncMessageBus>,
    answer: common::HistoricalAnswer,
    state: RequestState,
    ended: Option<Result<HistoricalData, Error>>,
}

impl HistoricalDataRequest {
    /// IB's request id for this request.
    pub fn request_id(&self) -> i32 {
        self.request_id
    }

    /// Whether the request may still be live at IB: sent, not ended by IB,
    /// its session not lost, not cancelled. A local error that ended
    /// [`answer`](Self::answer) (a lagged channel, an undecodable or
    /// unexpected message) leaves it open.
    pub fn is_open(&self) -> bool {
        self.state == RequestState::Open
    }

    /// Every error and notice IB sent for this request so far, in the order
    /// they arrived, each with IB's code and text; the one that ended it, if
    /// IB ended it with an error, included. Readable after a timeout or an
    /// error, when there is no [`HistoricalData`] to carry them.
    pub fn notices(&self) -> &[Notice] {
        self.answer.notices()
    }

    /// Wait for the answer.
    ///
    /// `Ok` only once IB's end marker arrived, with the bars and IB's window.
    /// A channel that closes or resets first is [`Error::Interrupted`]; an IB
    /// error for this request is [`Error::Notice`] with its code and text; a
    /// cancelled request is [`Error::Cancelled`]. Once ended, every later call
    /// returns the same result.
    ///
    /// Cancel-safe: dropping this future (a timer around it, a `select!`)
    /// loses nothing that already arrived.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await.expect("connection failed");
    ///     let contract = Contract::stock("AAPL").build();
    ///
    ///     let mut request = client
    ///         .historical_data(&contract, HistoricalBarSize::Min)
    ///         .duration(1.days())
    ///         .send()
    ///         .await
    ///         .expect("request not sent");
    ///     println!("request id {}", request.request_id());
    ///
    ///     let data = request.answer().await.expect("no complete answer");
    ///     println!("{} bars from {:?} to {:?}", data.bars.len(), data.start, data.end);
    /// }
    /// ```
    pub async fn answer(&mut self) -> Result<HistoricalData, Error> {
        if let Some(ended) = &self.ended {
            return ended.clone();
        }
        if self.state == RequestState::Cancelled {
            return Err(Error::Cancelled);
        }
        loop {
            let item = self.subscription.next_item().await;
            if let Some(result) = self.answer.accept(item) {
                // A local end (lagged channel, undecodable or unexpected
                // message) proves nothing about IB: the request stays open, so
                // cancel() and Drop still write the cancel.
                if !self.answer.may_be_live_at_ib() {
                    self.state = RequestState::Ended;
                }
                self.ended = Some(result.clone());
                return result;
            }
        }
    }

    /// Wait for the answer for at most `timeout`.
    ///
    /// As [`answer`](Self::answer); when `timeout` passes first, returns
    /// [`Error::TimedOut`] with this request's id and leaves the request open:
    /// IB may still be serving it. Call [`cancel`](Self::cancel) to end it at
    /// IB, or `answer` again to keep waiting with what arrived so far.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration as StdDuration;
    /// use ibapi::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ibapi::Error> {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await?;
    ///     let contract = Contract::stock("AAPL").build();
    ///
    ///     let mut request = client.historical_data(&contract, HistoricalBarSize::Min).duration(1.days()).send().await?;
    ///     match request.answer_within(StdDuration::from_secs(30)).await {
    ///         Ok(data) => println!("{} bars", data.bars.len()),
    ///         Err(ibapi::Error::TimedOut(request_id)) => {
    ///             request.cancel().await?;
    ///             println!("request {request_id} timed out and was cancelled");
    ///         }
    ///         Err(e) => return Err(e),
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn answer_within(&mut self, timeout: std::time::Duration) -> Result<HistoricalData, Error> {
        match tokio::time::timeout(timeout, self.answer()).await {
            Ok(result) => result,
            Err(_elapsed) => Err(Error::TimedOut(self.request_id)),
        }
    }

    /// Cancel the request at IB: write `cancelHistoricalData` for its id.
    ///
    /// `Ok(())` when the cancel was written, by this call or an earlier one,
    /// and when there is nothing to cancel: IB already ended the request (its
    /// end marker, or a code that ends it), or its session is gone
    /// ([`Error::Interrupted`]). After a local error (a lagged channel, an
    /// undecodable message) the cancel is written.
    /// On a write error the request stays open (a later call tries again), and
    /// the error says what happened: only [`Error::Closed`] and
    /// [`Error::Refused`] prove nothing was written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ibapi::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ibapi::Error> {
    ///     let client = Client::connect("127.0.0.1:4002", 100).await?;
    ///     let contract = Contract::stock("AAPL").build();
    ///
    ///     let mut request = client.historical_data(&contract, HistoricalBarSize::Min).duration(1.days()).send().await?;
    ///     request.cancel().await?;
    ///     assert!(!request.is_open());
    ///     Ok(())
    /// }
    /// ```
    pub async fn cancel(&mut self) -> Result<(), Error> {
        if self.state != RequestState::Open {
            return Ok(());
        }
        let message = encoders::encode_cancel_historical_data(self.request_id)?;
        self.message_bus.cancel_subscription(self.request_id, message).await?;
        self.state = RequestState::Cancelled;
        Ok(())
    }
}

impl Drop for HistoricalDataRequest {
    fn drop(&mut self) {
        if self.state != RequestState::Open {
            return;
        }
        let request_id = self.request_id;
        let message_bus = Arc::clone(&self.message_bus);
        if let Ok(message) = encoders::encode_cancel_historical_data(request_id) {
            tokio::spawn(async move {
                if let Err(e) = message_bus.cancel_subscription(request_id, message).await {
                    warn!("error sending cancel historical data in drop: {e}");
                }
            });
        }
    }
}

pub(crate) async fn historical_data_stream(
    client: &Client,
    contract: &Contract,
    duration: Duration,
    bar_size: BarSize,
    what_to_show: WhatToShow,
    trading_hours: TradingHours,
) -> Result<Subscription<HistoricalBarUpdate>, Error> {
    if !contract.trading_class.is_empty() || contract.contract_id > 0 {
        check_version(client.server_version(), Features::TRADING_CLASS)?;
    }

    let builder = client.request();
    let request = encoders::encode_request_historical_data(
        builder.request_id(),
        contract,
        None, // IBKR requires end_date=None when keep_up_to_date=true
        duration,
        bar_size,
        Some(what_to_show),
        trading_hours.use_rth(),
        true, // keep_up_to_date — the whole point of .stream()
        &Vec::<crate::contracts::TagValue>::default(),
    )?;

    builder.send::<HistoricalBarUpdate>(request).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn historical_ticks<T: TickDecoder<T> + Send>(
    client: &Client,
    contract: &Contract,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    number_of_ticks: i32,
    what_to_show: WhatToShow,
    trading_hours: TradingHours,
    ignore_size: bool,
) -> Result<TickSubscription<T>, Error> {
    check_version(client.server_version(), Features::HISTORICAL_TICKS)?;

    let builder = client.request();
    let request = encoders::encode_request_historical_ticks(
        builder.request_id(),
        contract,
        start,
        end,
        number_of_ticks,
        what_to_show,
        trading_hours.use_rth(),
        ignore_size,
    )?;
    let request_id = builder.request_id();
    let subscription = builder.send_raw(request).await?;

    Ok(TickSubscription::new(subscription, request_id, Arc::clone(&client.message_bus)))
}

pub(crate) async fn historical_schedule(
    client: &Client,
    contract: &Contract,
    end_date: Option<OffsetDateTime>,
    duration: Duration,
) -> Result<Schedule, Error> {
    common::validate_historical_data(client.server_version(), contract, end_date, Some(WhatToShow::Schedule))?;

    let builder = client.request();
    let request_id = builder.request_id();
    let request = encoders::encode_request_historical_data(
        request_id,
        contract,
        end_date,
        duration,
        BarSize::Day,
        Some(WhatToShow::Schedule),
        true,
        false,
        &Vec::<crate::contracts::TagValue>::default(),
    )?;

    let mut subscription = builder.send_raw(request).await?;

    match subscription.next().await {
        Some(Ok(mut message)) if message.message_type() == IncomingMessages::HistoricalSchedule => decoders::decode_historical_schedule(&mut message),
        Some(Ok(message)) => Err(Error::unexpected_response(&message)),
        Some(Err(Error::ConnectionReset)) | None => Err(Error::Interrupted(request_id)),
        Some(Err(e)) => Err(e),
    }
}

// === TickSubscription and related types ===

/// Async subscription handle that decodes historical tick batches as they arrive.
#[must_use = "TickSubscription must be polled (.next().await) to receive ticks; dropping it cancels the request"]
pub struct TickSubscription<T: TickDecoder<T> + Send> {
    done: bool,
    messages: AsyncInternalSubscription,
    buffer: VecDeque<T>,
    error: Option<Error>,
    request_id: i32,
    message_bus: Arc<dyn AsyncMessageBus>,
    cancelled: AtomicBool,
}

impl<T: TickDecoder<T> + Send> TickSubscription<T> {
    fn new(messages: AsyncInternalSubscription, request_id: i32, message_bus: Arc<dyn AsyncMessageBus>) -> Self {
        Self {
            done: false,
            messages,
            buffer: VecDeque::new(),
            error: None,
            request_id,
            message_bus,
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the historical-ticks request. Safe to call after completion (no-op).
    /// Also fired automatically on `Drop` for unfinished subscriptions; explicit calls are idempotent.
    pub async fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::Relaxed) {
            return;
        }

        match encoders::encode_cancel_historical_ticks(self.request_id) {
            Ok(message) => {
                if let Err(e) = self.message_bus.cancel_subscription(self.request_id, message).await {
                    warn!("error cancelling historical ticks subscription: {e}");
                }
            }
            Err(e) => error!("error encoding cancel historical ticks: {e}"),
        }
    }

    /// Block until the next tick batch is available.
    pub async fn next(&mut self) -> Option<T> {
        self.clear_error();

        loop {
            if let Some(tick) = self.next_buffered() {
                return Some(tick);
            }

            if self.done {
                return None;
            }

            match self.fill_buffer().await {
                Ok(()) => continue,
                Err(()) => return None,
            }
        }
    }

    async fn fill_buffer(&mut self) -> Result<(), ()> {
        match self.messages.next().await {
            Some(Ok(mut message)) if message.message_type() == T::MESSAGE_TYPE => {
                let (ticks, done) = T::decode(&mut message).unwrap();
                self.buffer.extend(ticks);
                self.done = done;
                Ok(())
            }
            Some(Ok(message)) => {
                debug!("unexpected message: {message:?}");
                Ok(())
            }
            Some(Err(_)) => Err(()),
            None => Err(()),
        }
    }

    fn next_buffered(&mut self) -> Option<T> {
        self.buffer.pop_front()
    }

    #[allow(dead_code)]
    fn set_error(&mut self, e: Error) {
        self.error = Some(e);
    }

    fn clear_error(&mut self) {
        self.error = None;
    }
}

impl<T: TickDecoder<T> + Send> Drop for TickSubscription<T> {
    fn drop(&mut self) {
        if self.done || self.cancelled.swap(true, Ordering::Relaxed) {
            return;
        }
        let request_id = self.request_id;
        let message_bus = self.message_bus.clone();
        if let Ok(message) = encoders::encode_cancel_historical_ticks(request_id) {
            tokio::spawn(async move {
                if let Err(e) = message_bus.cancel_subscription(request_id, message).await {
                    warn!("error sending cancel historical ticks in drop: {e}");
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "async_tests.rs"]
mod tests;
