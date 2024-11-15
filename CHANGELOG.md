# Changelog

## 1.0.4
  * Adds retry to `ChunkedEncodingError` on requests [#21] (https://github.com/singer-io/tap-dixa/pull/21)

## 1.0.3
  * Moves urllib3 to a version that is not affected by GHSA-g4mx-q9vg-27p4
  * Moves requests to a version that is not affected by CVE-2023-32681

## 1.0.2
  * Updated document links in README.md [#5] (https://github.com/singer-io/tap-dixa/pull/5)
  * Added custom_fields object in `conversations` stream's schema [#17] (https://github.com/singer-io/tap-dixa/pull/17)

## 1.0.1
  * Extended `activity_logs` schema [#14] (https://github.com/singer-io/tap-dixa/pull/14)

## 1.0.0
  *  Updated schema of messages & conversations streams
  *  Depricated `updated_at_datestring` replication key [https://github.com/singer-io/tap-dixa/pull/10]
  *  Using epoch time for replication & bookmarking [https://github.com/singer-io/tap-dixa/pull/10]
  *  Added `page_size` property to config for pagination [https://github.com/singer-io/tap-dixa/pull/8]
  *  Added backoff & retry for api timeout issue [https://github.com/singer-io/tap-dixa/pull/11]
  *  Code Refactoring [https://github.com/singer-io/tap-dixa/pull/6]
  *  Bug fixes [https://github.com/singer-io/tap-dixa/pull/8] [https://github.com/singer-io/tap-dixa/pull/9]

  ## 0.2.1
  * Fix Pagination and backoff time [#3] (https://github.com/singer-io/tap-dixa/pull/3)
