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

export interface Features
{
    readonly format: boolean;
    readonly dcl: boolean;
    readonly astJson: boolean;
}

export const Parser: {
    init(options?: InitOptions): Promise<void>;
    readonly features: Features;
    parse(sql: string): ParseResult;
    format(sql: string, options?: FormatOptions): FormatResult;
    formatJson(ast: unknown, options?: FormatOptions): FormatResult;
};
