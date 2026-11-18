-- A shard must encode the types of its `ProfileTraces` blocks as text, like its other packets,
-- because the initiator reads them with the inter-server settings, regardless of
-- `output_format_native_encode_types_in_binary_format`. Every allocation is sampled on the shard,
-- so it is guaranteed to send a trace block.
SELECT sum(length(toString(number)))
FROM remote('127.0.0.{1,2}', numbers(100000))
SETTINGS send_profile_traces = 1, prefer_localhost_replica = 0,
    memory_profiler_sample_probability = 1, max_untracked_memory = 0,
    output_format_native_encode_types_in_binary_format = 1, input_format_native_decode_types_in_binary_format = 1;
