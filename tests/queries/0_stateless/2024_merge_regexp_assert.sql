SELECT a FROM merge(REGEXP('.'), 'query_log'); -- { serverError 47 }
