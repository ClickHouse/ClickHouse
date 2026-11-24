-- `ATTACH MASKING POLICY` is the stored form of a masking policy, a client cannot send it.
ATTACH MASKING POLICY p ON default.t UPDATE x = 1 TO ALL; -- { clientError SYNTAX_ERROR }
