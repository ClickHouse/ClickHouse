-- Window sizes larger than the 64-bit hash width make Buzhash terms 64 bytes apart cancel each other,
-- e.g. with window 128 any input with a period dividing 64 hashes to zero in every window.
SELECT contentDefinedChunks('abc', 65, 1000); -- { serverError BAD_ARGUMENTS }
SELECT contentDefinedChunksUTF8('abc', 128, 1000); -- { serverError BAD_ARGUMENTS }
SELECT contentDefinedChunkOffsets('abc', 256, 1000); -- { serverError BAD_ARGUMENTS }
SELECT contentDefinedChunkOffsetsUTF8('abc', 0, 1000); -- { serverError BAD_ARGUMENTS }

-- Chunks of the unchanged suffix are reused after prepending content.
WITH arrayStringConcat(arrayMap(x -> hex(cityHash64(x)), range(2000))) AS s
SELECT 8, length(contentDefinedChunks(s, 8, 1000)) AS total,
    length(arrayIntersect(contentDefinedChunks(s, 8, 1000), contentDefinedChunks(concat('b', s), 8, 1000))) >= total - 2;
WITH arrayStringConcat(arrayMap(x -> hex(cityHash64(x)), range(2000))) AS s
SELECT 16, length(contentDefinedChunks(s, 16, 1000)) AS total,
    length(arrayIntersect(contentDefinedChunks(s, 16, 1000), contentDefinedChunks(concat('b', s), 16, 1000))) >= total - 2;
WITH arrayStringConcat(arrayMap(x -> hex(cityHash64(x)), range(2000))) AS s
SELECT 48, length(contentDefinedChunks(s, 48, 1000)) AS total,
    length(arrayIntersect(contentDefinedChunks(s, 48, 1000), contentDefinedChunks(concat('b', s), 48, 1000))) >= total - 2;
WITH arrayStringConcat(arrayMap(x -> hex(cityHash64(x)), range(2000))) AS s
SELECT 64, length(contentDefinedChunks(s, 64, 1000)) AS total,
    length(arrayIntersect(contentDefinedChunks(s, 64, 1000), contentDefinedChunks(concat('b', s), 64, 1000))) >= total - 2;
