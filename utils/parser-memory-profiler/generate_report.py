#!/usr/bin/env python3
"""
Generate self-contained HTML report from parser memory profiler results.
"""

import argparse
import json
import html
import base64
import re
import sys
from pathlib import Path


def fix_svg_zoom(svg: str) -> str:
    """
    Set initial zoom to 50% so more of the graph is visible.
    Modify the viewport's initial transform to scale(0.5).
    """
    if not svg or '<svg' not in svg:
        return svg
    
    # Find the viewport group and modify its initial transform to include scale(0.5)
    # Original: <g id="viewport" transform="translate(0,0)">
    # New: <g id="viewport" transform="scale(0.5) translate(0,0)">
    svg = re.sub(
        r'<g id="viewport" transform="translate\(([^)]+)\)">',
        r'<g id="viewport" transform="scale(0.5) translate(\1)">',
        svg
    )
    
    return svg


def generate_html_report(results: dict) -> str:
    """Generate a self-contained HTML report from profiling results."""
    
    queries = results.get("queries", [])
    
    # Build query list items
    query_list_items = []
    for q in queries:
        diff_class = "diff-high" if q["allocated_diff"] > 10000 else "diff-medium" if q["allocated_diff"] > 1000 else "diff-low"
        query_preview = html.escape(q["query"][:50] + "..." if len(q["query"]) > 50 else q["query"])
        query_list_items.append(f'''
            <div class="query-item" data-id="{q['id']}" onclick="selectQuery({q['id']})">
                <span class="query-id">#{q['id']}</span>
                <span class="query-preview">{query_preview}</span>
                <div class="query-stats">
                    <span class="stat-len">{q['query_length']}b</span>
                    <span class="stat-diff {diff_class}">+{q['allocated_diff']:,}</span>
                </div>
            </div>''')
    
    query_list_html = "\n".join(query_list_items)
    
    # Set initial zoom to 50% for call graphs
    for q in queries:
        if q.get('graph_svg'):
            q['graph_svg'] = fix_svg_zoom(q['graph_svg'])
    
    # Encode the JSON data as base64 to avoid any escaping issues
    json_str = json.dumps(results, ensure_ascii=True)
    json_b64 = base64.b64encode(json_str.encode('utf-8')).decode('ascii')
    
    html_template = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parser Memory Profiler Report</title>
    <style>
        :root {{
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --bg-hover: #30363d;
            --border-color: #30363d;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --text-muted: #6e7681;
            --accent-blue: #58a6ff;
            --accent-green: #3fb950;
            --accent-yellow: #d29922;
            --accent-red: #f85149;
            --accent-purple: #a371f7;
            --font-mono: 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace;
            --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: var(--font-sans);
            background: var(--bg-primary);
            color: var(--text-primary);
            display: flex;
            height: 100vh;
            overflow: hidden;
        }}
        
        /* Sidebar */
        .sidebar {{
            width: 320px;
            min-width: 320px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            height: 100vh;
        }}
        
        .sidebar-header {{
            padding: 16px;
            border-bottom: 1px solid var(--border-color);
            background: var(--bg-tertiary);
        }}
        
        .sidebar-header h1 {{
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 8px;
        }}
        
        .sidebar-stats {{
            display: flex;
            gap: 12px;
            font-size: 11px;
            color: var(--text-secondary);
        }}
        
        .sidebar-stats span {{
            display: flex;
            align-items: center;
            gap: 4px;
        }}
        
        .search-box {{
            padding: 12px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .search-box input {{
            width: 100%;
            padding: 8px 12px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 13px;
            outline: none;
            transition: border-color 0.2s;
        }}
        
        .search-box input:focus {{
            border-color: var(--accent-blue);
        }}
        
        .search-box input::placeholder {{
            color: var(--text-muted);
        }}
        
        .sort-controls {{
            padding: 8px 12px;
            display: flex;
            gap: 8px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .sort-btn {{
            padding: 4px 10px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-secondary);
            font-size: 11px;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .sort-btn:hover {{
            background: var(--bg-hover);
            color: var(--text-primary);
        }}
        
        .sort-btn.active {{
            background: var(--accent-blue);
            border-color: var(--accent-blue);
            color: white;
        }}
        
        .query-list {{
            flex: 1;
            overflow-y: auto;
            padding: 8px;
        }}
        
        .query-item {{
            padding: 10px 12px;
            margin-bottom: 4px;
            background: var(--bg-tertiary);
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            border: 1px solid transparent;
        }}
        
        .query-item:hover {{
            background: var(--bg-hover);
        }}
        
        .query-item.selected {{
            border-color: var(--accent-blue);
            background: rgba(88, 166, 255, 0.1);
        }}
        
        .query-id {{
            font-family: var(--font-mono);
            font-size: 11px;
            color: var(--accent-purple);
            font-weight: 600;
        }}
        
        .query-preview {{
            display: block;
            font-family: var(--font-mono);
            font-size: 11px;
            color: var(--text-secondary);
            margin: 4px 0;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .query-stats {{
            display: flex;
            justify-content: space-between;
            font-size: 11px;
        }}
        
        .stat-len {{
            color: var(--text-muted);
        }}
        
        .stat-diff {{
            font-family: var(--font-mono);
            font-weight: 600;
        }}
        
        .diff-low {{ color: var(--accent-green); }}
        .diff-medium {{ color: var(--accent-yellow); }}
        .diff-high {{ color: var(--accent-red); }}
        
        /* Main content */
        .main {{
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}
        
        .main-header {{
            padding: 16px 24px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
        }}
        
        .query-display {{
            font-family: var(--font-mono);
            font-size: 13px;
            line-height: 1.5;
            padding: 12px 16px;
            background: var(--bg-tertiary);
            border-radius: 8px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-break: break-word;
        }}
        
        /* SQL syntax highlighting */
        .sql-keyword {{ color: var(--accent-purple); font-weight: 600; }}
        .sql-function {{ color: var(--accent-blue); }}
        .sql-string {{ color: var(--accent-green); }}
        .sql-number {{ color: var(--accent-yellow); }}
        .sql-operator {{ color: var(--accent-red); }}
        .sql-comment {{ color: var(--text-muted); font-style: italic; }}
        .sql-type {{ color: #79c0ff; }}
        
        .stats-bar {{
            display: flex;
            gap: 24px;
            margin-top: 12px;
            padding: 12px 16px;
            background: var(--bg-tertiary);
            border-radius: 8px;
        }}
        
        .stat-item {{
            display: flex;
            flex-direction: column;
            gap: 2px;
        }}
        
        .stat-label {{
            font-size: 10px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .stat-value {{
            font-family: var(--font-mono);
            font-size: 16px;
            font-weight: 600;
        }}
        
        .stat-value.positive {{ color: var(--accent-red); }}
        
        /* Tabs */
        .tabs {{
            display: flex;
            padding: 0 24px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
        }}
        
        .tab {{
            padding: 12px 20px;
            background: none;
            border: none;
            color: var(--text-secondary);
            font-size: 13px;
            cursor: pointer;
            position: relative;
            transition: color 0.2s;
        }}
        
        .tab:hover {{
            color: var(--text-primary);
        }}
        
        .tab.active {{
            color: var(--accent-blue);
        }}
        
        .tab.active::after {{
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: var(--accent-blue);
        }}
        
        /* Content area */
        .content {{
            flex: 1;
            overflow: auto;
            padding: 24px;
        }}
        
        .tab-content {{
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        .text-report {{
            font-family: var(--font-mono);
            font-size: 12px;
            line-height: 1.6;
            white-space: pre-wrap;
            overflow-x: auto;
            background: var(--bg-tertiary);
            padding: 16px;
            border-radius: 8px;
            color: var(--text-primary);
        }}
        
        .svg-container {{
            background: white;
            border-radius: 8px;
            overflow: hidden;
            min-height: 620px;
        }}
        
        .svg-container iframe {{
            display: block;
            width: 100%;
            height: 600px;
            border: none;
            background: white;
        }}
        
        .placeholder {{
            text-align: center;
            color: var(--text-muted);
            padding: 48px;
        }}
        
        /* Scrollbar styling */
        ::-webkit-scrollbar {{
            width: 8px;
            height: 8px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: var(--bg-primary);
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: var(--bg-hover);
            border-radius: 4px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: var(--text-muted);
        }}
        
        @media (max-width: 900px) {{
            .sidebar {{
                width: 260px;
                min-width: 260px;
            }}
        }}
    </style>
</head>
<body>
    <div class="sidebar">
        <div class="sidebar-header">
            <h1>Parser Memory Profiler</h1>
            <div class="sidebar-stats">
                <span>{len(queries)} queries</span>
                <span>Total: {sum(q['allocated_diff'] for q in queries):,} bytes</span>
            </div>
        </div>
        <div class="search-box">
            <input type="text" id="search" placeholder="Filter queries..." oninput="filterQueries(this.value)">
        </div>
        <div class="sort-controls">
            <button class="sort-btn active" data-sort="id" onclick="sortBy('id')">ID</button>
            <button class="sort-btn" data-sort="length" onclick="sortBy('length')">Length</button>
            <button class="sort-btn" data-sort="diff" onclick="sortBy('diff')">Memory</button>
        </div>
        <div class="query-list" id="queryList">
            {query_list_html}
        </div>
    </div>
    
    <div class="main">
        <div class="main-header">
            <div class="query-display" id="queryDisplay">
                <span class="placeholder">Select a query from the list</span>
            </div>
            <div class="stats-bar" id="statsBar" style="display: none;">
                <div class="stat-item">
                    <span class="stat-label">Query Length</span>
                    <span class="stat-value" id="statLength">-</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Before Parsing</span>
                    <span class="stat-value" id="statBefore">-</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">After Parsing</span>
                    <span class="stat-value" id="statAfter">-</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Allocated</span>
                    <span class="stat-value positive" id="statDiff">-</span>
                </div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab active" data-tab="text" onclick="showTab('text')">Text (Bytes)</button>
            <button class="tab" data-tab="objects" onclick="showTab('objects')">Objects</button>
            <button class="tab" data-tab="graph" onclick="showTab('graph')">Call Graph</button>
            <button class="tab" data-tab="flame" onclick="showTab('flame')">Flame Graph</button>
        </div>
        
        <div class="content">
            <div id="tab-text" class="tab-content active">
                <pre class="text-report" id="textReport">Select a query to view profiling results</pre>
            </div>
            <div id="tab-objects" class="tab-content">
                <pre class="text-report" id="objectsReport">Select a query to view profiling results</pre>
            </div>
            <div id="tab-graph" class="tab-content">
                <div class="svg-container" id="graphContainer">
                    <div class="placeholder">Select a query to view call graph</div>
                </div>
            </div>
            <div id="tab-flame" class="tab-content">
                <div class="svg-container" id="flameContainer">
                    <div class="placeholder">Select a query to view flame graph</div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Decode base64 JSON data
        const DATA = JSON.parse(atob("{json_b64}"));
        
        // SQL syntax highlighting
        function highlightSQL(sql) {{
            const keywords = /\\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|EXISTS|CASE|WHEN|THEN|ELSE|END|AS|DISTINCT|ALL|ANY|UNION|INTERSECT|EXCEPT|ORDER|BY|GROUP|HAVING|LIMIT|OFFSET|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|VIEW|INDEX|DROP|ALTER|ADD|COLUMN|PRIMARY|KEY|FOREIGN|REFERENCES|UNIQUE|CHECK|DEFAULT|CONSTRAINT|ENGINE|PARTITION|MATERIALIZED|SYSTEM|SHOW|DESCRIBE|ATTACH|DETACH|OPTIMIZE|RENAME|TRUNCATE|WITH|RECURSIVE|OVER|WINDOW|ROWS|RANGE|UNBOUNDED|PRECEDING|FOLLOWING|CURRENT|ROW|GRANT|REVOKE|SETTINGS|FORMAT|PREWHERE|GLOBAL|SAMPLE|FINAL|TOTALS|ARRAY|TUPLE|MAP|EXPLAIN|AST|SYNTAX|PIPELINE|PLAN|IF|TTL|CODEC|COMMENT|TEMPORARY|REPLACE|USING|NATURAL|SEMI|ANTI|ASOF|NULLS|FIRST|LAST|COLLATE|POPULATE|LIVE|DICTIONARY|DATABASE|POLICY|QUOTA|PROFILE|ROLE|USER)\\b/gi;
            const types = /\\b(UInt8|UInt16|UInt32|UInt64|UInt128|UInt256|Int8|Int16|Int32|Int64|Int128|Int256|Float32|Float64|Decimal|String|FixedString|UUID|Date|Date32|DateTime|DateTime64|Enum8|Enum16|Array|Tuple|Map|Nested|Nullable|LowCardinality|AggregateFunction|SimpleAggregateFunction|IPv4|IPv6|Bool|Boolean|Nothing|JSON|Object)\\b/g;
            const functions = /\\b(count|sum|avg|min|max|any|anyLast|groupArray|groupUniqArray|argMin|argMax|uniq|uniqExact|uniqCombined|uniqHLL12|median|quantile|quantiles|topK|topKWeighted|covarSamp|covarPop|corr|now|today|yesterday|toDate|toDateTime|toUInt32|toInt32|toString|toFloat64|length|empty|notEmpty|reverse|concat|substring|replaceOne|replaceAll|lower|upper|trim|splitByChar|splitByString|arrayJoin|range|arrayMap|arrayFilter|arrayElement|has|indexOf|countEqual|arraySum|if|multiIf|nullIf|assumeNotNull|toNullable|coalesce|isNull|isNotNull|cityHash64|sipHash64|MD5|SHA256|rand|randConstant|generateUUIDv4)\\b/gi;
            
            return sql
                .replace(/--.*$/gm, '<span class="sql-comment">$&</span>')
                .replace(/'[^']*'/g, '<span class="sql-string">$&</span>')
                .replace(/\\b\\d+(\\.\\d+)?\\b/g, '<span class="sql-number">$&</span>')
                .replace(keywords, '<span class="sql-keyword">$&</span>')
                .replace(types, '<span class="sql-type">$&</span>')
                .replace(functions, '<span class="sql-function">$&</span>');
        }}
        
        function formatBytes(bytes) {{
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
            return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
        }}
        
        let currentQuery = null;
        let currentSort = 'id';
        let sortAsc = true;
        
        function selectQuery(id) {{
            const query = DATA.queries.find(q => q.id === id);
            if (!query) return;
            
            currentQuery = query;
            
            // Update selection in list
            document.querySelectorAll('.query-item').forEach(el => {{
                el.classList.toggle('selected', parseInt(el.dataset.id) === id);
            }});
            
            // Update query display with syntax highlighting
            document.getElementById('queryDisplay').innerHTML = highlightSQL(escapeHtml(query.query));
            
            // Update stats
            document.getElementById('statsBar').style.display = 'flex';
            document.getElementById('statLength').textContent = query.query_length + ' bytes';
            document.getElementById('statBefore').textContent = formatBytes(query.allocated_before);
            document.getElementById('statAfter').textContent = formatBytes(query.allocated_after);
            document.getElementById('statDiff').textContent = '+' + formatBytes(query.allocated_diff);
            
            // Update text reports - the text is already properly formatted from JSON
            const textContent = query.jeprof_text || 'No profiling data available';
            const objectsContent = query.jeprof_objects || 'No profiling data available';
            
            document.getElementById('textReport').textContent = textContent;
            document.getElementById('objectsReport').textContent = objectsContent;
            
            // Update SVGs
            const graphContainer = document.getElementById('graphContainer');
            const flameContainer = document.getElementById('flameContainer');
            
            if (query.graph_svg && query.graph_svg.trim()) {{
                // Clean up header text from call graph SVG
                let graphSvg = query.graph_svg;
                // Remove header text elements (file path, Total B, Focusing on, Dropped nodes/edges)
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*\\/Users\\/[^<]*<\\/text>/g, '');
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*\\.\\/[^<]*<\\/text>/g, '');
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*Total B:[^<]*<\\/text>/g, '');
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*Focusing on:[^<]*<\\/text>/g, '');
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*Dropped nodes[^<]*<\\/text>/g, '');
                graphSvg = graphSvg.replace(/<text[^>]*>[^<]*Dropped edges[^<]*<\\/text>/g, '');
                
                // Use iframe for call graph - graphviz SVG has built-in zoom/pan
                const graphBlob = new Blob([graphSvg], {{type: 'image/svg+xml'}});
                const graphUrl = URL.createObjectURL(graphBlob);
                graphContainer.innerHTML = '<iframe src="' + graphUrl + '" style="width:100%;height:600px;border:none;background:white;"></iframe>';
            }} else {{
                graphContainer.innerHTML = '<div class="placeholder">No call graph available</div>';
            }}
            
            if (query.flame_svg && query.flame_svg.trim()) {{
                // Use iframe for flame graph to enable interactive features (search, zoom)
                const flameBlob = new Blob([query.flame_svg], {{type: 'image/svg+xml'}});
                const flameUrl = URL.createObjectURL(flameBlob);
                flameContainer.innerHTML = `<iframe src="${{flameUrl}}" style="width:100%;height:600px;border:none;background:white;"></iframe>`;
            }} else {{
                flameContainer.innerHTML = '<div class="placeholder">No flame graph available</div>';
            }}
        }}
        
        function escapeHtml(text) {{
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }}
        
        function showTab(tabName) {{
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            
            document.querySelector(`.tab[data-tab="${{tabName}}"]`).classList.add('active');
            document.getElementById(`tab-${{tabName}}`).classList.add('active');
        }}
        
        function sortBy(field) {{
            if (currentSort === field) {{
                sortAsc = !sortAsc;
            }} else {{
                currentSort = field;
                sortAsc = field === 'id';
            }}
            
            document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
            document.querySelector(`.sort-btn[data-sort="${{field}}"]`).classList.add('active');
            
            const list = document.getElementById('queryList');
            const items = Array.from(list.querySelectorAll('.query-item'));
            
            items.sort((a, b) => {{
                const qa = DATA.queries.find(q => q.id === parseInt(a.dataset.id));
                const qb = DATA.queries.find(q => q.id === parseInt(b.dataset.id));
                
                let va, vb;
                if (field === 'id') {{
                    va = qa.id;
                    vb = qb.id;
                }} else if (field === 'length') {{
                    va = qa.query_length;
                    vb = qb.query_length;
                }} else {{
                    va = qa.allocated_diff;
                    vb = qb.allocated_diff;
                }}
                
                return sortAsc ? va - vb : vb - va;
            }});
            
            items.forEach(item => list.appendChild(item));
        }}
        
        function filterQueries(searchText) {{
            const search = searchText.toLowerCase();
            document.querySelectorAll('.query-item').forEach(item => {{
                const query = DATA.queries.find(q => q.id === parseInt(item.dataset.id));
                const matches = query.query.toLowerCase().includes(search) || 
                               query.id.toString().includes(search);
                item.style.display = matches ? '' : 'none';
            }});
        }}
        
        // Select first query on load
        if (DATA.queries.length > 0) {{
            selectQuery(DATA.queries[0].id);
        }}
    </script>
</body>
</html>'''
    
    return html_template


def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from profiler results')
    parser.add_argument('-i', '--input', required=True, help='Input JSON results file')
    parser.add_argument('-o', '--output', required=True, help='Output HTML file')
    
    args = parser.parse_args()
    
    # Read results
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    with open(input_path, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # Generate HTML
    html_content = generate_html_report(results)
    
    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Report generated: {output_path}")
    print(f"Queries: {len(results.get('queries', []))}")


if __name__ == '__main__':
    main()
