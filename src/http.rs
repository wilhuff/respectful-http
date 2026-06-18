use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::HeaderMap;

use crate::error::{Error, Result};
use crate::respectful_retry::{RequestOutcome, RespectfulRetry, RetryConfig};

/// HTTP response: status code, headers, and raw body bytes.
#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

/// Trait for making HTTP requests.
///
/// Dyn-compatible via `async-trait`. The vtable overhead is negligible
/// compared to network latency.
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn get(&self, url: &str) -> Result<HttpResponse>;

    /// GET with additional request headers (e.g. `Accept`).
    ///
    /// The default implementation ignores `headers` and delegates to
    /// [`get`](Self::get); clients that support per-request headers (the
    /// reqwest-backed one) override it. Decorators that wrap another client
    /// (e.g. [`RateLimitedHttpClient`]) should forward `headers` to the inner
    /// client.
    async fn get_with_headers(&self, url: &str, _headers: &[(&str, &str)]) -> Result<HttpResponse> {
        self.get(url).await
    }

    /// POST with a urlencoded form body and additional request headers.
    ///
    /// The default implementation refuses; clients that support POST (the
    /// reqwest-backed one) override it, and decorators forward it.
    async fn post_form(
        &self,
        _url: &str,
        _form: &[(&str, &str)],
        _headers: &[(&str, &str)],
    ) -> Result<HttpResponse> {
        Err(Error::Unsupported("POST"))
    }
}

/// Production HTTP client backed by reqwest.
///
/// Wraps a `reqwest::Client` which maintains a connection pool.
/// Create once and reuse.
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    /// Polite API consumers identify themselves on every request with a product
    /// name and contact address (required by e.g. the Wikimedia User-Agent policy;
    /// good etiquette everywhere else). The caller supplies that string — this crate
    /// stays identity-neutral, so derive it from your own binary's package metadata.
    pub fn new(user_agent: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(user_agent)
                .build()
                .expect("reqwest client construction cannot fail with these options"),
        }
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn get(&self, url: &str) -> Result<HttpResponse> {
        self.get_with_headers(url, &[]).await
    }

    async fn get_with_headers(&self, url: &str, headers: &[(&str, &str)]) -> Result<HttpResponse> {
        let mut request = self.client.get(url);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        finish(request).await
    }

    async fn post_form(
        &self,
        url: &str,
        form: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> Result<HttpResponse> {
        let mut request = self.client.post(url).form(form);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        finish(request).await
    }
}

/// Send a prepared request and collect it into an [`HttpResponse`].
async fn finish(request: reqwest::RequestBuilder) -> Result<HttpResponse> {
    let response = request.send().await?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = response.bytes().await?.to_vec();
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

/// HTTP client decorator that paces requests and retries on throttling.
///
/// Every request is spaced at least `safe_interval` apart. On 429/403,
/// backs off multiplicatively and recovers linearly. Respects `Retry-After`
/// headers. Retries are transparent to callers.
///
/// Note: the pacing applies to *sequential* calls (it sleeps after each request).
/// It does not serialize concurrent callers — for concurrent multi-host work, route
/// each host through its own single worker holding its own [`RespectfulRetry`].
pub struct RateLimitedHttpClient {
    inner: Box<dyn HttpClient>,
    limiter: Mutex<RespectfulRetry>,
    max_retries: u32,
}

impl RateLimitedHttpClient {
    pub fn new(inner: Box<dyn HttpClient>, config: RetryConfig, max_retries: u32) -> Self {
        Self {
            inner,
            limiter: Mutex::new(RespectfulRetry::new(config)),
            max_retries,
        }
    }
}

/// A request shape the rate limiter can replay across retries.
enum RequestSpec<'a> {
    Get {
        url: &'a str,
        headers: &'a [(&'a str, &'a str)],
    },
    PostForm {
        url: &'a str,
        form: &'a [(&'a str, &'a str)],
        headers: &'a [(&'a str, &'a str)],
    },
}

impl RateLimitedHttpClient {
    async fn perform(&self, spec: RequestSpec<'_>) -> Result<HttpResponse> {
        for attempt in 0..=self.max_retries {
            let response = match &spec {
                RequestSpec::Get { url, headers } => {
                    self.inner.get_with_headers(url, headers).await?
                }
                RequestSpec::PostForm { url, form, headers } => {
                    self.inner.post_form(url, form, headers).await?
                }
            };

            if response.status != 429 && response.status != 403 {
                let delay = self.limiter.lock().unwrap().update(RequestOutcome::Success);
                tokio::time::sleep(delay).await;
                return Ok(response);
            }

            if attempt == self.max_retries {
                return Err(Error::RateLimited {
                    status: response.status,
                    retries: self.max_retries,
                });
            }

            let outcome = parse_retry_after(&response)
                .map(RequestOutcome::ThrottledWithRetryAfter)
                .unwrap_or(RequestOutcome::Throttled);
            let delay = self.limiter.lock().unwrap().update(outcome);
            tokio::time::sleep(delay).await;
        }
        unreachable!()
    }
}

#[async_trait]
impl HttpClient for RateLimitedHttpClient {
    async fn get(&self, url: &str) -> Result<HttpResponse> {
        self.get_with_headers(url, &[]).await
    }

    async fn get_with_headers(&self, url: &str, headers: &[(&str, &str)]) -> Result<HttpResponse> {
        self.perform(RequestSpec::Get { url, headers }).await
    }

    async fn post_form(
        &self,
        url: &str,
        form: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> Result<HttpResponse> {
        self.perform(RequestSpec::PostForm { url, form, headers })
            .await
    }
}

/// Parse the `Retry-After` header as a duration in seconds.
fn parse_retry_after(response: &HttpResponse) -> Option<Duration> {
    let seconds = response
        .headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .parse::<f64>()
        .ok()?;
    Some(Duration::from_secs_f64(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;
    use std::sync::Mutex as StdMutex;

    struct MockHttpClient {
        responses: StdMutex<Vec<HttpResponse>>,
        /// Records the headers seen on the most recent request, so tests can
        /// assert that decorators forward them to the inner client. Shared via
        /// `Arc` so the test can read it after the client is boxed.
        last_headers: std::sync::Arc<StdMutex<Vec<(String, String)>>>,
        /// Likewise for the most recent POST form pairs.
        last_form: std::sync::Arc<StdMutex<Vec<(String, String)>>>,
    }

    impl MockHttpClient {
        fn new(responses: Vec<HttpResponse>) -> Self {
            Self {
                responses: StdMutex::new(responses),
                last_headers: std::sync::Arc::new(StdMutex::new(Vec::new())),
                last_form: std::sync::Arc::new(StdMutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl HttpClient for MockHttpClient {
        async fn get(&self, url: &str) -> Result<HttpResponse> {
            self.get_with_headers(url, &[]).await
        }

        async fn get_with_headers(
            &self,
            _url: &str,
            headers: &[(&str, &str)],
        ) -> Result<HttpResponse> {
            *self.last_headers.lock().unwrap() = headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let response = self.responses.lock().unwrap().remove(0);
            Ok(response)
        }

        async fn post_form(
            &self,
            _url: &str,
            form: &[(&str, &str)],
            headers: &[(&str, &str)],
        ) -> Result<HttpResponse> {
            *self.last_form.lock().unwrap() = form
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            *self.last_headers.lock().unwrap() = headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let response = self.responses.lock().unwrap().remove(0);
            Ok(response)
        }
    }

    /// A client that only implements `get`, exercising the trait's
    /// default `post_form`.
    struct GetOnlyClient;

    #[async_trait]
    impl HttpClient for GetOnlyClient {
        async fn get(&self, _url: &str) -> Result<HttpResponse> {
            Ok(response(200))
        }
    }

    fn test_config() -> RetryConfig {
        RetryConfig {
            safe_interval: Duration::from_millis(1),
            recovery_step: Duration::from_millis(1),
            backoff_multiplier: 2.0,
        }
    }

    fn response(status: u16) -> HttpResponse {
        HttpResponse {
            status,
            headers: HeaderMap::new(),
            body: Vec::new(),
        }
    }

    fn response_with_retry_after(seconds: &str) -> HttpResponse {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_str(seconds).unwrap());
        HttpResponse {
            status: 429,
            headers,
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_headers_forwarded_through_rate_limiter() {
        let inner = MockHttpClient::new(vec![response(200)]);
        let seen = inner.last_headers.clone();
        let client = RateLimitedHttpClient::new(Box::new(inner), test_config(), 3);
        let resp = client
            .get_with_headers("http://example.com", &[("accept", "application/ld+json")])
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![("accept".to_string(), "application/ld+json".to_string())]
        );
    }

    #[tokio::test]
    async fn test_post_form_forwarded_through_rate_limiter() {
        let inner = MockHttpClient::new(vec![response(200)]);
        let form_seen = inner.last_form.clone();
        let headers_seen = inner.last_headers.clone();
        let client = RateLimitedHttpClient::new(Box::new(inner), test_config(), 3);
        let resp = client
            .post_form(
                "http://example.com",
                &[("query", "SELECT 1"), ("format", "json")],
                &[("accept", "application/sparql-results+json")],
            )
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(
            *form_seen.lock().unwrap(),
            vec![
                ("query".to_string(), "SELECT 1".to_string()),
                ("format".to_string(), "json".to_string()),
            ]
        );
        assert_eq!(
            *headers_seen.lock().unwrap(),
            vec![(
                "accept".to_string(),
                "application/sparql-results+json".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn test_post_form_retry_on_429_then_success() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![response(429), response(200)])),
            test_config(),
            3,
        );
        let resp = client
            .post_form("http://example.com", &[("a", "b")], &[])
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
    }

    #[tokio::test]
    async fn test_post_form_default_impl_refuses() {
        let err = GetOnlyClient
            .post_form("http://example.com", &[], &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("POST not supported"));
    }

    #[tokio::test]
    async fn test_no_retry_on_success() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![response(200)])),
            test_config(),
            3,
        );
        let resp = client.get("http://example.com").await.unwrap();
        assert_eq!(resp.status, 200);
    }

    #[tokio::test]
    async fn test_retry_on_429_then_success() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![response(429), response(200)])),
            test_config(),
            3,
        );
        let resp = client.get("http://example.com").await.unwrap();
        assert_eq!(resp.status, 200);
    }

    #[tokio::test]
    async fn test_retry_on_403_then_success() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![response(403), response(200)])),
            test_config(),
            3,
        );
        let resp = client.get("http://example.com").await.unwrap();
        assert_eq!(resp.status, 200);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![
                response(429),
                response(429),
                response(429),
            ])),
            test_config(),
            2,
        );
        let err = client.get("http://example.com").await.unwrap_err();
        assert!(err.to_string().contains("429"));
        assert!(err.to_string().contains("2 retries"));
    }

    #[tokio::test]
    async fn test_no_retry_on_server_error() {
        // 500 is not retried — only 429/403.
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![response(500)])),
            test_config(),
            3,
        );
        let resp = client.get("http://example.com").await.unwrap();
        assert_eq!(resp.status, 500);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_after_header() {
        let client = RateLimitedHttpClient::new(
            Box::new(MockHttpClient::new(vec![
                response_with_retry_after("1"),
                response(200),
            ])),
            test_config(),
            3,
        );
        let resp = client.get("http://example.com").await.unwrap();
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_parse_retry_after() {
        let resp = response_with_retry_after("25");
        assert_eq!(parse_retry_after(&resp), Some(Duration::from_secs(25)));
    }

    #[test]
    fn test_parse_retry_after_missing() {
        let resp = response(429);
        assert_eq!(parse_retry_after(&resp), None);
    }

    #[test]
    fn test_parse_retry_after_fractional() {
        let resp = response_with_retry_after("1.5");
        assert_eq!(parse_retry_after(&resp), Some(Duration::from_secs_f64(1.5)));
    }
}
