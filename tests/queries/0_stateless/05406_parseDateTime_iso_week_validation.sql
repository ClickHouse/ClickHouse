SELECT parseDateTime('2015-53-1', '%G-%V-%u', 'UTC');
SELECT parseDateTime('2020-53-1', '%G-%V-%u', 'UTC');
SELECT parseDateTime('2000-53-1', '%G-%V-%u', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime('2021-53-1', '%G-%V-%u', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
