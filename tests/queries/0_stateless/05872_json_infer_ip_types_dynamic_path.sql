-- IPv4/IPv6 inference for strings in dynamic paths of JSON, including the reuse of an existing IPv4/IPv6 variant.

SET input_format_try_infer_ipv4 = 1, input_format_try_infer_ipv6 = 1;

SELECT 'Without date and datetime inference';
SET input_format_try_infer_dates = 0, input_format_try_infer_datetimes = 0;
SELECT dynamicType(json.a), json.a
FROM (SELECT arrayJoin(['{"a":"1.2.3.4"}', '{"a":"5.6.7.8"}', '{"a":"::1"}', '{"a":"x"}', '{"a":"9.9.9.9"}', '{"a":"::2"}'])::JSON AS json);

SELECT 'With date and datetime inference';
SET input_format_try_infer_dates = 1, input_format_try_infer_datetimes = 1;
SELECT dynamicType(json.a), json.a
FROM (SELECT arrayJoin(['{"a":"1.2.3.4"}', '{"a":"2020-01-01"}', '{"a":"::1"}', '{"a":"x"}', '{"a":"9.9.9.9"}', '{"a":"::2"}'])::JSON AS json);

SELECT 'IP inference disabled';
SET input_format_try_infer_ipv4 = 0, input_format_try_infer_ipv6 = 0;
SELECT dynamicType(json.a), json.a
FROM (SELECT arrayJoin(['{"a":"1.2.3.4"}', '{"a":"::1"}'])::JSON AS json);
