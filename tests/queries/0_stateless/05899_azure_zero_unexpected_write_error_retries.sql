-- Tags: no-fasttest
-- Tag no-fasttest: needs Azurite

-- With zero retries allowed every Azure write still makes one attempt, so the data is written.

SET azure_truncate_on_insert = 1;
SET azure_max_unexpected_write_error_retries = 0;

-- single-part upload
INSERT INTO FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_single.csv'), 'CSV', 'auto', 'x UInt64') SELECT number + 1 FROM numbers(3);
SELECT count(), sum(x) FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_single.csv'), 'CSV', 'auto', 'x UInt64');

-- multi-part upload (StageBlock + CommitBlockList)
INSERT INTO FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_multi.tsv'), 'TSV', 'auto', 's String')
SELECT randomPrintableASCII(1000) FROM numbers(3000)
SETTINGS azure_strict_upload_part_size = 1000000, azure_max_single_part_upload_size = 1000000;
SELECT count() FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_multi.tsv'), 'TSV', 'auto', 's String');

-- empty blob
INSERT INTO FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_empty.tsv'), 'TSV', 'auto', 's String') SELECT 'x' WHERE 0;
SELECT count() FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05899', concat(currentDatabase(), '_empty.tsv'), 'TSV', 'auto', 's String');
