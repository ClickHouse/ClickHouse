SELECT positionCaseInsensitiveUTF8('Hello', materialize('%\xF0%'));
SELECT positionCaseInsensitiveUTF8(materialize('Hello'), '%\xF0%') FROM numbers(1000);
