export interface Highlight
{
    begin: number;
    end: number;
    type: string;
}

export interface ParseError
{
    message: string;
    begin?: number;
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

export const Parser: {
    init(options?: InitOptions): Promise<void>;
    readonly features: number;
    parse(sql: string): ParseResult;
    format(sql: string, options?: FormatOptions): FormatResult;
    formatJson(ast: unknown, options?: FormatOptions): FormatResult;
};
