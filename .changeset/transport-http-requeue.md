---
'@smooai/observability': patch
---

TypeScript: re-queue ingest batches when native `fetch` resolves a non-2xx response.

Native `fetch` resolves normally for HTTP 4xx and 5xx responses. The transport only handled rejected promises, so a non-2xx ingest response removed the batch from the queue permanently instead of retrying it. The transport now treats `response.ok === false` as a failure and pushes the batch back onto the queue for the existing retry path.
