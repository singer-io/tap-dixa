# Changelog

## 1.0.8
  * Bump dependency versions for dependabot compliance [#32](https://github.com/singer-io/tap-dixa/pull/32)

## 1.0.7
  * Bump dependency versions for dependabot compliance [#29](https://github.com/singer-io/tap-dixa/pull/29)

## 1.0.6
  * Bump dependency versions for twistlock compliance
    [#27](https://github.com/singer-io/tap-dixa/pull/27) [#28](https://github.com/singer-io/tap-dixa/pull/28)

## 1.0.5
  *  Bump urllib3 from 1.26.18 to 1.26.19 [#22] (https://github.com/singer-io/tap-dixa/pull/22)
  *  Bump requests from 2.31.0 to 2.32.2 [#23] (https://github.com/singer-io/tap-dixa/pull/23)
  *  Bump idna from 3.2 to 3.7 [#24] (https://github.com/singer-io/tap-dixa/pull/24)
  *  Bump certifi from 2023.7.22 to 2024.7.4 [#25] (https://github.com/singer-io/tap-dixa/pull/25)

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
