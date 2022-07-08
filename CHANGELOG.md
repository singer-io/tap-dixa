# Changelog

## 0.2.1
  * Fix Pagination and backoff time [#3] (https://github.com/singer-io/tap-dixa/pull/3) 

## 1.0.0
  *  Updated schema of messages & conversations streams
  *  Depricated `updated_at_datestring` replication key [#10] (https://github.com/singer-io/tap-dixa/pull/10)
  *  Using epoch time for replication & bookmarking [#10] (https://github.com/singer-io/tap-dixa/pull/10)
  *  Added `page_size` property to config for pagination [#8] (https://github.com/singer-io/tap-dixa/pull/8)
  *  Added backoff & retry for api timeout issue [#11](https://github.com/singer-io/tap-dixa/pull/11)
  *  Code Refactoring [#6] (https://github.com/singer-io/tap-dixa/pull/6)
  *  Bug fixes [#8] (https://github.com/singer-io/tap-dixa/pull/8) [#9] (https://github.com/singer-io/tap-dixa/pull/9)