# Changelog

## 0.2.1
  * Fix Pagination and backoff time [#3] (https://github.com/singer-io/tap-dixa/pull/3) 

## 1.0.0
  *  Updated schema of messages & conversations streams
  *  Depricated `updated_at_datestring` replication key (https://github.com/singer-io/tap-dixa/pull/10)
  *  Using epoch time for replication & bookmarking (https://github.com/singer-io/tap-dixa/pull/10)
  *  Added `page_size` property to config for pagination (https://github.com/singer-io/tap-dixa/pull/8)
  *  Added backoff & retry for api timeout issue (https://github.com/singer-io/tap-dixa/pull/11)
  *  Code Refactoring (https://github.com/singer-io/tap-dixa/pull/6)
  *  Bug fixes (https://github.com/singer-io/tap-dixa/pull/8) (https://github.com/singer-io/tap-dixa/pull/9)