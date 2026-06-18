//! Rate-limited HTTP client with respectful retry backoff.
//!
//! Two layers, used independently or together:
//!
//!  - [`RespectfulRetry`] — a pure, IO-free rate-limit state machine: steady-state
//!    request spacing, multiplicative backoff on throttling (HTTP 429/403), linear
//!    recovery on success, and `Retry-After` honored. It does no sleeping and no
//!    network — you feed it outcomes and it returns how long to wait. Own one per
//!    host (or per logical stream) and drive it yourself.
//!  - [`HttpClient`] / [`ReqwestHttpClient`] / [`RateLimitedHttpClient`] — an async
//!    client trait, a pooled `reqwest` backend, and a decorator that paces and retries
//!    a *sequential* caller transparently. The decorator paces successive calls; it
//!    does **not** gate a concurrent burst, so for concurrent multi-host work, shard
//!    requests per host and give each single worker its own [`RespectfulRetry`].
//!
//! Callers supply their own `User-Agent` to [`ReqwestHttpClient::new`] — polite API
//! consumers identify themselves (product + contact); this crate stays identity-neutral.

mod error;
mod http;
mod respectful_retry;

pub use error::{Error, Result};
pub use http::{HttpClient, HttpResponse, RateLimitedHttpClient, ReqwestHttpClient};
pub use respectful_retry::{RequestOutcome, RespectfulRetry, RetryConfig};
