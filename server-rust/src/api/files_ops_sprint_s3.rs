//! S3-compatible private object transport. Construction is explicit: the
//! integrator supplies credentials and a private bucket, never a public proxy.
use super::{ObjectStore, safe_filename};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method, StatusCode, Url, header};
use sha2::{Digest, Sha256};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct S3Objects {
    client: Client,
    endpoint: Url,
    object_base: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn sha256(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

impl S3Objects {
    /// `endpoint` includes the bucket path (path-style) or bucket host
    /// (virtual-hosted style). `object_base` is the canonical ledger URL;
    /// it may point at a CDN, but that CDN MUST deny unauthenticated reads.
    /// Plain HTTP is only permitted for an explicitly selected loopback test.
    pub fn new(
        endpoint: &str,
        object_base: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        allow_loopback_http: bool,
    ) -> Result<Self, String> {
        let endpoint = Self::base_url(endpoint, allow_loopback_http)?;
        let object_base = Self::base_url(object_base, allow_loopback_http)?;
        if region.is_empty()
            || !region
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            || access_key.is_empty()
            || !access_key
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
            || secret_key.is_empty()
        {
            return Err("invalid S3 credentials or region".into());
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            endpoint,
            object_base: object_base.to_string().trim_end_matches('/').to_owned(),
            region: region.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
            session_token: None,
        })
    }

    /// Temporary S3 credentials require the session token on every request.
    /// Supply it through the integrator's secret provider, never an object URL.
    pub fn with_session_token(mut self, token: &str) -> Result<Self, String> {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err("invalid S3 session token".into());
        }
        self.session_token = Some(token.to_owned());
        Ok(self)
    }

    fn base_url(raw: &str, loopback: bool) -> Result<Url, String> {
        let url = Url::parse(raw).map_err(|_| "invalid S3 URL")?;
        let local_http = loopback
            && url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "[::1]"));
        if !(url.scheme() == "https" || local_http)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || (raw.ends_with('/') && url.path() != "/")
            || (url.path() != "/"
                && url
                    .path()
                    .strip_prefix('/')
                    .unwrap_or("")
                    .split('/')
                    .any(|segment| {
                        segment.is_empty()
                            || !segment.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                            })
                    }))
            || url.as_str() != raw
        {
            return Err("invalid S3 URL".into());
        }
        Ok(url)
    }

    fn valid_key(key: &str) -> bool {
        !key.is_empty()
            && key.split('/').all(|segment| {
                !segment.is_empty()
                    && segment != "."
                    && segment != ".."
                    && segment
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
            })
    }

    fn url(&self, key: &str) -> Result<Url, String> {
        if !Self::valid_key(key) {
            return Err("invalid S3 object key".into());
        }
        let mut url = self.endpoint.clone();
        let path = format!("{}/{}", url.path().trim_end_matches('/'), key);
        url.set_path(&path);
        Ok(url)
    }

    async fn request(
        &self,
        method: Method,
        key: &str,
        body: Option<&[u8]>,
        extra: &[(&str, String)],
    ) -> Result<reqwest::Response, String> {
        let url = self.url(key)?;
        let date = Utc::now();
        let timestamp = date.format("%Y%m%dT%H%M%SZ").to_string();
        let day = date.format("%Y%m%d").to_string();
        let payload_hash = sha256(body.unwrap_or_default());
        let host = url.host_str().ok_or("missing S3 host")?;
        let host = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        let mut signed = vec![
            ("host".to_owned(), host),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), timestamp.clone()),
        ];
        // Range bytes and the ledger size describe the stored representation,
        // not a response transparently decoded by a gateway or HTTP client.
        if matches!(method, Method::GET | Method::HEAD) {
            signed.push(("accept-encoding".to_owned(), "identity".to_owned()));
        }
        if let Some(token) = &self.session_token {
            signed.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        signed.extend(
            extra
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone())),
        );
        signed.sort_by(|a, b| a.0.cmp(&b.0));
        if signed.iter().any(|(name, value)| {
            value.contains(['\r', '\n'])
                || name
                    .bytes()
                    .any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != b'-')
        }) {
            return Err("invalid S3 header".into());
        }
        let signed_names = signed
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let scope = format!("{day}/{}/s3/aws4_request", self.region);
        let canonical = format!(
            "{}\n{}\n\n{canonical_headers}\n{signed_names}\n{payload_hash}",
            method.as_str(),
            url.path()
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            sha256(canonical.as_bytes())
        );
        let date_key = hmac(format!("AWS4{}", self.secret_key).as_bytes(), &day);
        let region_key = hmac(&date_key, &self.region);
        let service_key = hmac(&region_key, "s3");
        let signing_key = hmac(&service_key, "aws4_request");
        let sig = hex::encode(hmac(&signing_key, &string_to_sign));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={sig}",
            self.access_key
        );
        let mut request = self
            .client
            .request(method, url)
            .header(header::AUTHORIZATION, auth);
        for (name, value) in signed {
            request = request.header(name, value);
        }
        if let Some(bytes) = body {
            request = request.body(bytes.to_vec());
        }
        request.send().await.map_err(|e| e.to_string())
    }

    fn read_range(
        &self,
        key: &str,
        start: u64,
        limit: usize,
        expected_total: Option<u64>,
    ) -> Result<Vec<u8>, String> {
        self.url(key)?;
        if limit == 0 {
            if expected_total.is_some_and(|size| size != 0) {
                return Err("S3 object size differs from ledger".into());
            }
            if start != 0 {
                return Err("invalid empty object range".into());
            }
            return self.execute(async {
                let response = self.request(Method::HEAD, key, None, &[]).await?;
                // HEAD has no response body: reqwest's body length hint is not
                // the size of the stored object. Inspect exactly one provider
                // Content-Length field instead, including on verified reads.
                let mut lengths = response.headers().get_all(header::CONTENT_LENGTH).iter();
                let zero_length = matches!(lengths.next(), Some(value) if value.to_str().ok().and_then(|v| v.parse::<u64>().ok()) == Some(0))
                    && lengths.next().is_none();
                if response.status() == StatusCode::OK
                    && zero_length
                    && Self::identity_encoding(response.headers())
                {
                    Ok(Vec::new())
                } else {
                    Err("missing or nonempty S3 object".into())
                }
            });
        }
        let end = start
            .checked_add(limit as u64 - 1)
            .ok_or("invalid object range")?;
        self.execute(async {
            let mut response = self
                .request(
                    Method::GET,
                    key,
                    None,
                    &[("range", format!("bytes={start}-{end}"))],
                )
                .await?;
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err("S3 range read failed".into());
            }
            if !Self::identity_encoding(response.headers())
                || response
                    .headers()
                    .get_all(header::CONTENT_TYPE)
                    .iter()
                    .any(|value| {
                        value.to_str().is_ok_and(|v| {
                            v.to_ascii_lowercase().starts_with("multipart/byteranges")
                        })
                    })
            {
                return Err("S3 response is not an identity byte range".into());
            }
            let mut ranges = response.headers().get_all(header::CONTENT_RANGE).iter();
            let content_range = ranges
                .next()
                .and_then(|value| value.to_str().ok())
                .filter(|_| ranges.next().is_none())
                .ok_or("S3 response has no unique content range")?;
            let (actual, total) = content_range
                .strip_prefix("bytes ")
                .and_then(|value| value.split_once('/'))
                .ok_or("invalid S3 content range")?;
            let total = total.parse::<u64>().map_err(|_| "invalid S3 object size")?;
            if expected_total.is_some_and(|size| size != total) {
                return Err("S3 object size differs from ledger".into());
            }
            // Readers ask for up to `limit` bytes. Avatar reads deliberately
            // ask for size + 1 to detect a stale ledger, so EOF may shorten it.
            let actual_end = total
                .checked_sub(1)
                .ok_or("empty S3 content range")?
                .min(end);
            if actual_end < start || actual != format!("{start}-{actual_end}") {
                return Err("S3 response range mismatch".into());
            }
            let expected = (actual_end - start + 1) as usize;
            let mut lengths = response.headers().get_all(header::CONTENT_LENGTH).iter();
            if let Some(length) = lengths.next()
                && (length.to_str().ok().and_then(|v| v.parse::<usize>().ok()) != Some(expected)
                    || lengths.next().is_some())
            {
                return Err("S3 response length differs from content range".into());
            }
            let mut data = Vec::with_capacity(expected.min(1 << 20));
            while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
                if chunk.len() > expected - data.len() {
                    return Err("S3 object exceeds requested range".into());
                }
                data.extend_from_slice(&chunk);
            }
            if data.len() != expected {
                return Err("incomplete S3 object range".into());
            }
            Ok(data)
        })
    }

    fn identity_encoding(headers: &header::HeaderMap) -> bool {
        let mut encodings = headers.get_all(header::CONTENT_ENCODING).iter();
        match encodings.next() {
            None => true,
            Some(value) => value.as_bytes() == b"identity" && encodings.next().is_none(),
        }
    }

    fn execute<T>(&self, work: impl Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(work)
    }
}

impl ObjectStore for S3Objects {
    fn object_url(&self, key: &str) -> Result<String, String> {
        self.url(key)?;
        Ok(format!("{}/{key}", self.object_base))
    }

    fn key_from_url(&self, url: &str) -> Result<String, String> {
        let key = url
            .strip_prefix(&format!("{}/", self.object_base))
            .ok_or("object URL is outside S3 storage")?;
        (self.object_url(key)?.as_str() == url)
            .then(|| key.to_owned())
            .ok_or("noncanonical S3 URL".into())
    }

    fn read(&self, key: &str, start: u64, limit: usize) -> Result<Vec<u8>, String> {
        self.read_range(key, start, limit, None)
    }

    fn read_verified(
        &self,
        key: &str,
        start: u64,
        limit: usize,
        expected_size: u64,
    ) -> Result<Vec<u8>, String> {
        self.read_range(key, start, limit, Some(expected_size))
    }

    fn write(&self, key: &str, data: &[u8], kind: &str, filename: &str) -> Result<(), String> {
        let disposition = format!("attachment; filename=\"{}\"", safe_filename(filename));
        let response = self.execute(self.request(
            Method::PUT,
            key,
            Some(data),
            &[
                ("content-type", kind.to_owned()),
                ("if-none-match", "*".to_owned()),
                ("content-disposition", disposition),
            ],
        ))?;
        if !response.status().is_success() {
            return Err(format!("S3 write failed: {}", response.status()));
        }
        Ok(())
    }

    fn remove(&self, key: &str) -> Result<(), String> {
        let response = self.execute(self.request(Method::DELETE, key, None, &[]))?;
        if !response.status().is_success() {
            return Err(format!("S3 delete failed: {}", response.status()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    fn mock(response: &'static str) -> (S3Objects, thread::JoinHandle<String>) {
        mock_at("/private", response)
    }

    fn mock_at(path: &str, response: &'static str) -> (S3Objects, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}{}", listener.local_addr().unwrap(), path);
        let store =
            S3Objects::new(&base, &base, "us-west-2", "synthetic", "fake-only", true).unwrap();
        let thread = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "mock S3 received no request"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("mock S3 accept failed: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 2048];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let count = stream.read(&mut buf).unwrap();
                assert!(count != 0 && request.len() < 16384);
                request.extend_from_slice(&buf[..count]);
            }
            let boundary = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let headers = String::from_utf8(request[..boundary].to_vec()).unwrap();
            let size = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .and_then(|v| v.parse::<usize>().ok())
                })
                .unwrap_or(0);
            while request.len() - boundary < size {
                let count = stream.read(&mut buf).unwrap();
                assert!(count != 0);
                request.extend_from_slice(&buf[..count]);
            }
            stream.write_all(response.as_bytes()).unwrap();
            String::from_utf8(request).unwrap()
        });
        (store, thread)
    }

    // Reconstruct the signature from bytes received by the fake provider,
    // rather than reusing the request's canonical string or signed headers.
    fn signature_matches_wire(request: &str) -> bool {
        use std::collections::HashMap;

        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap();
        let mut parts = request_line.split(' ');
        let method = parts.next().unwrap();
        let path = parts.next().unwrap();
        assert!(parts.next().is_some_and(|version| version == "HTTP/1.1"));
        assert!(parts.next().is_none());
        let headers: HashMap<String, &str> = lines
            .map(|line| {
                let (name, value) = line.split_once(':').unwrap();
                (name.to_ascii_lowercase(), value.trim())
            })
            .collect();
        let auth = headers["authorization"];
        let credential = auth
            .strip_prefix("AWS4-HMAC-SHA256 Credential=synthetic/")
            .unwrap();
        let (scope, rest) = credential.split_once(", SignedHeaders=").unwrap();
        let (names, actual_signature) = rest.split_once(", Signature=").unwrap();
        let signed_names: Vec<&str> = names.split(';').collect();
        assert!(signed_names.windows(2).all(|pair| pair[0] < pair[1]));
        let canonical_headers = signed_names
            .iter()
            .map(|name| format!("{name}:{}\n", headers[*name]))
            .collect::<String>();
        let payload_hash = hex::encode(Sha256::digest(body.as_bytes()));
        if headers["x-amz-content-sha256"] != payload_hash {
            return false;
        }
        let canonical = format!("{method}\n{path}\n\n{canonical_headers}\n{names}\n{payload_hash}");
        let timestamp = headers["x-amz-date"];
        let (day, region_and_service) = scope.split_once('/').unwrap();
        assert_eq!(&timestamp[..8], day);
        assert_eq!(region_and_service, "us-west-2/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let mut key = b"AWS4fake-only".to_vec();
        for value in [day, "us-west-2", "s3", "aws4_request"] {
            let mut mac = HmacSha256::new_from_slice(&key).unwrap();
            mac.update(value.as_bytes());
            key = mac.finalize().into_bytes().to_vec();
        }
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(string_to_sign.as_bytes());
        hex::encode(mac.finalize().into_bytes()) == actual_signature
    }

    #[test]
    fn canonical_private_keys_and_config() {
        let store = S3Objects::new(
            "https://s3.example.invalid/private",
            "https://cdn.example.invalid/uploads",
            "us-west-2",
            "fake",
            "fake",
            false,
        )
        .unwrap();
        let key = "users/11111111-1111-4111-8111-111111111111/file.png";
        let url = store.object_url(key).unwrap();
        assert_eq!(store.key_from_url(&url).unwrap(), key);
        for invalid in [
            "../other",
            "users//file",
            "/etc/passwd",
            "users/%2e%2e/file",
            "users/one?x=1",
        ] {
            assert!(store.object_url(invalid).is_err());
        }
        assert!(
            store
                .key_from_url("https://cdn.example.invalid/uploads2/users/file")
                .is_err()
        );
        assert!(store.key_from_url(&format!("{url}?token=1")).is_err());
        for path in [
            "//private",
            "/private//uploads",
            "/private%2Fuploads",
            "/bücket",
        ] {
            assert!(
                S3Objects::new(
                    &format!("https://s3.example.invalid{path}"),
                    "https://cdn.example.invalid/uploads",
                    "us-west-2",
                    "fake",
                    "fake",
                    false
                )
                .is_err()
            );
            assert!(
                S3Objects::new(
                    "https://s3.example.invalid/private",
                    &format!("https://cdn.example.invalid{path}"),
                    "us-west-2",
                    "fake",
                    "fake",
                    false
                )
                .is_err()
            );
        }
        assert!(
            S3Objects::new(
                "http://other.invalid/bucket",
                "https://cdn.example.invalid",
                "us-west-2",
                "fake",
                "fake",
                true
            )
            .is_err()
        );
        assert!(
            S3Objects::new(
                "https://user:pass@s3.example.invalid/bucket",
                "https://cdn.example.invalid",
                "us-west-2",
                "fake",
                "fake",
                false
            )
            .is_err()
        );
    }

    #[test]
    fn virtual_hosted_bucket_root_is_canonical_and_signed() {
        let store = S3Objects::new(
            "https://bucket.s3.example.invalid/",
            "https://private.example.invalid/",
            "us-west-2",
            "synthetic",
            "fake-only",
            false,
        )
        .unwrap();
        assert_eq!(
            store.object_url("users/owner/photo.png").unwrap(),
            "https://private.example.invalid/users/owner/photo.png"
        );
        assert_eq!(
            store.url("users/owner/photo.png").unwrap().as_str(),
            "https://bucket.s3.example.invalid/users/owner/photo.png"
        );
        assert!(
            S3Objects::new(
                "https://bucket.s3.example.invalid",
                "https://private.example.invalid/",
                "us-west-2",
                "synthetic",
                "fake-only",
                false
            )
            .is_err()
        );
        assert!(
            S3Objects::new(
                "https://bucket.s3.example.invalid/path/",
                "https://private.example.invalid/",
                "us-west-2",
                "synthetic",
                "fake-only",
                false
            )
            .is_err()
        );

        let (store, worker) = mock_at(
            "/",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert_eq!(store.read("users/owner/photo.png", 0, 3).unwrap(), b"abc");
        assert!(
            worker
                .join()
                .unwrap()
                .starts_with("GET /users/owner/photo.png HTTP/1.1")
        );
    }

    #[test]
    fn signed_range_and_bounded_payload() {
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/5\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert_eq!(store.read("users/owner/image.png", 2, 3).unwrap(), b"abc");
        let request = worker.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /private/users/owner/image.png http/1.1"));
        assert!(request.contains("range: bytes=2-4"));
        assert!(request.contains("authorization: aws4-hmac-sha256 credential=synthetic/"));
        assert!(
            request.contains(
                "signedheaders=accept-encoding;host;range;x-amz-content-sha256;x-amz-date"
            )
        );
        let (store, worker) =
            mock("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc");
        assert!(store.read("users/owner/image.png", 2, 3).is_err());
        worker.join().unwrap();
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 1-3/5\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert!(store.read("users/owner/image.png", 2, 3).is_err());
        worker.join().unwrap();
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/5\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabcd",
        );
        assert!(store.read("users/owner/image.png", 2, 3).is_err());
        worker.join().unwrap();
    }

    #[test]
    fn verified_reads_reject_stale_ledger_size_for_full_and_partial_ranges() {
        let key = "users/owner/image.png";
        // A bounded full download of three bytes must not silently serve a
        // truncated prefix when the provider reports a four-byte object.
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/4\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert!(store.read_verified(key, 0, 3, 3).is_err());
        assert!(signature_matches_wire(&worker.join().unwrap()));

        // A middle range is also checked against the ledger's total, even
        // though its length and content-range positions are otherwise valid.
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 1-2/4\r\nContent-Length: 2\r\nConnection: close\r\n\r\nbc",
        );
        assert!(store.read_verified(key, 1, 2, 3).is_err());
        worker.join().unwrap();

        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert_eq!(store.read_verified(key, 0, 3, 3).unwrap(), b"abc");
        worker.join().unwrap();
        let (store, worker) =
            mock("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        assert_eq!(store.read_verified(key, 0, 0, 0).unwrap(), b"");
        worker.join().unwrap();
    }

    #[test]
    fn temporary_credentials_sign_the_session_token_for_every_method() {
        const TOKEN: &str = "synthetic+/session==";
        for method in ["GET", "PUT", "DELETE"] {
            let response = if method == "GET" {
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-0/1\r\nContent-Length: 1\r\nConnection: close\r\n\r\na"
            } else {
                "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            };
            let (store, worker) = mock(response);
            let store = store.with_session_token(TOKEN).unwrap();
            match method {
                "GET" => assert_eq!(store.read("users/owner/photo.png", 0, 1).unwrap(), b"a"),
                "PUT" => store
                    .write("users/owner/photo.png", b"a", "image/png", "photo.png")
                    .unwrap(),
                _ => store.remove("users/owner/photo.png").unwrap(),
            }
            let request = worker.join().unwrap();
            assert!(request.starts_with(&format!("{method} /private/users/owner/photo.png")));
            assert!(request.contains("x-amz-security-token: synthetic+/session=="));
            assert!(request.contains(";x-amz-security-token"));
            assert!(signature_matches_wire(&request));
            assert!(!signature_matches_wire(&request.replacen(
                "x-amz-security-token: synthetic+/session==",
                "x-amz-security-token: substituted",
                1
            )));
            assert!(!signature_matches_wire(&request.replacen(
                "/private/users/owner/photo.png",
                "/private/users/owner/other.png",
                1
            )));
            if method == "PUT" {
                assert!(!signature_matches_wire(&request.replacen(
                    "\r\n\r\na",
                    "\r\n\r\nb",
                    1
                )));
            }
        }
        let (store, worker) =
            mock("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        assert_eq!(
            store.read("users/owner/empty", 0, 0).unwrap(),
            Vec::<u8>::new()
        );
        let request = worker.join().unwrap();
        assert!(!request.contains("x-amz-security-token"));
        assert!(signature_matches_wire(&request));
        let store = S3Objects::new(
            "https://bucket.s3.example.invalid/",
            "https://private.example.invalid/",
            "us-west-2",
            "synthetic",
            "fake-only",
            false,
        )
        .unwrap();
        for invalid in ["", "has space", "has\nnewline", "has\rreturn", "é"] {
            assert!(store.clone().with_session_token(invalid).is_err());
        }
    }

    #[test]
    fn empty_object_requires_real_zero_length_object() {
        let (store, worker) =
            mock("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        assert_eq!(
            store.read("users/owner/empty", 0, 0).unwrap(),
            Vec::<u8>::new()
        );
        let request = worker.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with("head /private/users/owner/empty http/1.1"));
        let (store, worker) =
            mock("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        assert!(store.read("users/owner/empty", 0, 0).is_err());
        worker.join().unwrap();
        // The stored object is nonempty even though a HEAD response has no
        // body. A body-size hint must never stand in for Content-Length.
        for response in [
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: invalid\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ] {
            let (store, worker) = mock(response);
            assert!(store.read_verified("users/owner/empty", 0, 0, 0).is_err());
            worker.join().unwrap();
        }
    }

    #[test]
    fn duplicate_content_range_is_not_an_authoritative_size() {
        let (store, worker) = mock(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Range: bytes 0-2/4\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        );
        assert!(store.read_verified("users/owner/item", 0, 3, 3).is_err());
        worker.join().unwrap();
    }

    #[test]
    fn verified_range_rejects_transformed_or_inconsistent_provider_bytes() {
        let key = "users/owner/image.png";
        let valid = "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Encoding: identity\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc";
        let (store, worker) = mock(valid);
        assert_eq!(store.read_verified(key, 0, 3, 3).unwrap(), b"abc");
        let request = worker.join().unwrap();
        assert!(request.contains("accept-encoding: identity"));
        assert!(signature_matches_wire(&request));
        for response in [
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Encoding: identity, gzip\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Encoding: identity\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Type: multipart/byteranges; boundary=foo\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: 2\r\nConnection: close\r\n\r\nab",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: invalid\r\nConnection: close\r\n\r\nabc",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: 3\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
        ] {
            let (store, worker) = mock(response);
            assert!(store.read_verified(key, 0, 3, 3).is_err(), "{response}");
            worker.join().unwrap();
        }
        let (store, worker) = mock(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: gzip\r\nConnection: close\r\n\r\n",
        );
        assert!(store.read_verified(key, 0, 0, 0).is_err());
        let request = worker.join().unwrap();
        assert!(request.starts_with("HEAD "));
        assert!(request.contains("accept-encoding: identity"));
        assert!(signature_matches_wire(&request));
    }

    #[test]
    fn immutable_write_and_failing_delete_keep_cleanup_intent() {
        let (store, worker) = mock(
            "HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        assert!(
            store
                .write("workspaces/w/file.txt", b"hello", "text/plain", "file.txt")
                .is_err()
        );
        let request = worker.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with("put /private/workspaces/w/file.txt http/1.1"));
        assert!(request.contains("if-none-match: *"));
        assert!(request.contains(
            "x-amz-content-sha256: 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        ));
        let (store, worker) = mock(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        assert!(store.remove("workspaces/w/file.txt").is_err());
        worker.join().unwrap();
    }
}
