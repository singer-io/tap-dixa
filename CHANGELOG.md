# Changelog

## 0.2.1
  * Fix Pagination and backoff time [#3] (https://github.com/singer-io/tap-dixa/pull/3) 

## 1.0.0
  *  Updated schema of messages & conversations streams
  *  Depricated `updated_at_datestring` replication key
  *  Using epoch time for replication & bookmarking
  *  Added `page_size` property to config for pagination
  *  Added backoff & retry for api timeout issue
  *  Code Refactoring
  *  Bug fixes