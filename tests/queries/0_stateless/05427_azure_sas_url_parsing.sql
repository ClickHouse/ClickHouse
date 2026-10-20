-- Tags: no-fasttest
-- no-fasttest: needs Azure Blob Storage support, which is not built in the fast test.

-- The (url, sas_token) form of the Azure table functions rejects a URL that ends right after the host or a malformed abfss URL.
SELECT * FROM azureBlobStorage('abfss://foo.net', 'sv=2022'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM azureBlobStorage('abfss://foo.net/c/x.csv', 'sv=2022'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM azureBlobStorage('abfss://c.x@acc.dfs.core.windows.net/p/x.csv', 'sv=2022'); -- { serverError BAD_ARGUMENTS }
