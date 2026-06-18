//! Typed errors for `respectful-http`.
//!
//! The crate was extracted from a codebase that returned `anyhow::Result`; callers now
//! get a concrete enum to match on (e.g. to distinguish a transport failure from
//! exhausted rate-limit retries).

/// Anything that can go wrong issuing a request through this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The underlying HTTP transport failed (connect, TLS, timeout, body read, …).
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),
    /// The server kept throttling (HTTP 429/403) past the configured retry budget.
    #[error("HTTP {status}: rate limit exceeded after {retries} retries")]
    RateLimited { status: u16, retries: u32 },
    /// The operation isn't supported by this client (e.g. POST on a GET-only client).
    #[error("{0} not supported by this client")]
    Unsupported(&'static str),
}

/// Convenience alias for results from this crate.
pub type Result<T> = std::result::Result<T, Error>;
