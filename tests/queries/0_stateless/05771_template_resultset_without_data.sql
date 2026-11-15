-- The format string for the result set of the `Template` input format must contain `${data}`.

SELECT * FROM format(Template, 'x UInt64', '1') SETTINGS format_template_resultset_format = '${data}', format_template_row_format = '${x:CSV}';
SELECT * FROM format(Template, 'x UInt64', '1') SETTINGS format_template_resultset_format = 'prefix', format_template_row_format = '${x:CSV}'; -- { serverError INVALID_TEMPLATE_FORMAT }
SELECT * FROM format(Template, 'x UInt64', '') SETTINGS format_template_resultset_format = '', format_template_row_format = '${x:CSV}'; -- { serverError INVALID_TEMPLATE_FORMAT }
