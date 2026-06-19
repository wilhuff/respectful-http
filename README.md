# respectful-http

A rate-limited HTTP client with respectful retry backoff. Be a well-behaved API
consumer: space requests out, back off on `429`/`403`, honor `Retry-After`, and
recover gradually once the server is happy again.

Two layers, usable independently:

- **`RespectfulRetry`** — a pure, IO-free rate-limit state machine. Steady-state
  spacing at `safe_interval`; multiplicative backoff on throttling; linear recovery
  on success; `Retry-After` honored (max of backoff vs. the server's ask). It does no
  sleeping and no network — you feed it outcomes, it returns how long to wait. Own one
  per host (or per logical stream) and drive it yourself.
- **`HttpClient` / `ReqwestHttpClient` / `RateLimitedHttpClient`** — an async client
  trait, a pooled `reqwest` backend, and a decorator that paces and retries a
  *sequential* caller transparently.

## Concurrency

The decorator paces *successive* calls (it sleeps after each request); it does **not**
serialize a concurrent burst. For concurrent, multi-host work, shard requests per host
onto a single worker each, and give every worker its own `RespectfulRetry`. That keeps
each host strictly paced while different hosts proceed in parallel — with no shared
lock on the hot path.

## User-Agent

Polite consumers identify themselves. The caller passes a `User-Agent` string (product
+ contact) to `ReqwestHttpClient::new`; this crate stays identity-neutral.

## License

Licensed under either of

- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.

---

Extracted from the `books/net` crate in MediaTracker.
