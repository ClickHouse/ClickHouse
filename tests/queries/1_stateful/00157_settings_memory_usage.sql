select max(CounterID + UserID) from test.hits settings max_memory_usage = 1; -- { serverError 241 }
