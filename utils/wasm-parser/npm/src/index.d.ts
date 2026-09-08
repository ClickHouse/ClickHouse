export interface Highlight
{
    /** UTF-8 byte offset, not a JavaScript string index. */
    begin: number;
    /** UTF-8 byte offset, exclusive, not a JavaScript string index. */
    end: number;
    type: string;
}

export interface ParseError
{
    message: string;
    /** UTF-8 byte offset, not a JavaScript string index. */
    begin?: number;
    /** UTF-8 byte offset, exclusive, not a JavaScript string index. */
    end?: number;
    line?: number;
    column?: number;
    expected?: string[];
}

export interface ParseResult
{
    ast?: unknown;
    ast_error?: string;
    highlights?: Highlight[];
    error?: ParseError;
}

export interface FormatResult
{
    sql?: string;
    error?: ParseError;
}

export interface InitOptions
{
    url?: string | URL;
    bytes?: BufferSource;
}

export interface FormatOptions
{
    oneLine?: boolean;
}

export const FEATURE_FORMAT = 1;
export const FEATURE_DCL = 2;
export const FEATURE_AST_JSON = 4;

export const Parser: {
    init(options?: InitOptions): Promise<void>;
    readonly features: number;
    parse(sql: string): ParseResult;
    format(sql: string, options?: FormatOptions): FormatResult;
    formatJson(ast: unknown, options?: FormatOptions): FormatResult;
};
