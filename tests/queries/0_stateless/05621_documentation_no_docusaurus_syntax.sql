-- The website reference pages are generated from these descriptions as is: no `:::` admonitions, `@theme/` imports,
-- `highlight-*` magic comments or links to `.md` files (use `<Note>`, `/snippets/components/...`, `highlight={...}`, routes).
SELECT type, name FROM system.documentation
WHERE match(description, '(^|\n)[ \t]*:::') OR position(description, '@theme/') > 0
    OR match(description, 'highlight-(next-line|start|end)')
    OR match(description, '\\]\\([^)\\s:]*\\.mdx?(/?#[^)\\s]*)?\\)')
ORDER BY type, name;
