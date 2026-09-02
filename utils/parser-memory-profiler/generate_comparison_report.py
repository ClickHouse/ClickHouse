#!/usr/bin/env python3
"""
Generate comparison HTML report between two parser memory profiler runs.
Shows side-by-side memory allocation differences.
"""

import argparse
import json
import html
import base64
import re
import sys
from pathlib import Path


def extract_jeprof_total(text):
    """Extract total bytes from jeprof text output like 'Total: 1648 B'"""
    if not text:
        return 0
    match = re.search(r'Total:\s*([\d,]+)\s*B', text)
    if match:
        return int(match.group(1).replace(',', ''))
    return 0


def format_bytes(b):
    """Format bytes to human readable."""
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b / (1024 * 1024):.2f} MB"


def format_diff(before_val, after_val):
    """Format the difference with color indicator."""
    if before_val == 0:
        if after_val == 0:
            return "0", "neutral"
        return f"+{format_bytes(after_val)}", "worse"
    
    diff = after_val - before_val
    pct = (diff / before_val) * 100
    
    if diff < 0:
        return f"{format_bytes(diff)} ({pct:.1f}%)", "better"
    elif diff > 0:
        return f"+{format_bytes(diff)} (+{pct:.1f}%)", "worse"
    else:
        return "0", "neutral"


def generate_comparison_report(results_before: dict, results_after: dict, 
                               label_before: str = "Before", 
                               label_after: str = "After") -> str:
    """Generate comparison HTML report."""
    
    queries_before = {q["id"]: q for q in results_before.get("queries", [])}
    queries_after = {q["id"]: q for q in results_after.get("queries", [])}
    
    # Get all query IDs
    all_ids = sorted(set(queries_before.keys()) | set(queries_after.keys()))
    
    # Calculate totals using jeprof profile-based metrics
    total_before = sum(extract_jeprof_total(q.get("jeprof_text", "")) for q in queries_before.values())
    total_after = sum(extract_jeprof_total(q.get("jeprof_text", "")) for q in queries_after.values())
    total_diff, total_class = format_diff(total_before, total_after)
    
    # Build comparison rows
    rows = []
    for qid in all_ids:
        q_before = queries_before.get(qid, {})
        q_after = queries_after.get(qid, {})
        
        query_text = q_before.get("query", q_after.get("query", "N/A"))
        query_preview = html.escape(query_text[:60] + "..." if len(query_text) > 60 else query_text)
        
        # Use jeprof profile-based totals instead of allocated_diff for more accurate comparison
        alloc_before = extract_jeprof_total(q_before.get("jeprof_text", ""))
        alloc_after = extract_jeprof_total(q_after.get("jeprof_text", ""))
        
        diff_str, diff_class = format_diff(alloc_before, alloc_after)
        
        rows.append({
            "id": qid,
            "query": query_text,
            "query_preview": query_preview,
            "before": alloc_before,
            "after": alloc_after,
            "diff_str": diff_str,
            "diff_class": diff_class,
            "q_before": q_before,
            "q_after": q_after,
        })
    
    # Build row HTML with expandable details
    row_html_items = []
    for r in rows:
        row_html_items.append(f'''
            <tr class="query-row" data-id="{r['id']}" onclick="toggleQuery({r['id']})">
                <td class="col-id">#{r['id']}</td>
                <td class="col-query">{r['query_preview']}</td>
                <td class="col-before">{format_bytes(r['before'])}</td>
                <td class="col-after">{format_bytes(r['after'])}</td>
                <td class="col-diff {r['diff_class']}">{r['diff_str']}</td>
            </tr>
            <tr class="detail-row" id="detail-{r['id']}" style="display: none;">
                <td colspan="5">
                    <div class="detail-content" id="detail-content-{r['id']}"></div>
                </td>
            </tr>''')
    
    rows_html = "\n".join(row_html_items)
    
    # Encode data as JSON for JavaScript
    comparison_data = {
        "label_before": label_before,
        "label_after": label_after,
        "queries_before": queries_before,
        "queries_after": queries_after,
        "rows": [{k: v for k, v in r.items() if k not in ("q_before", "q_after")} for r in rows]
    }
    json_str = json.dumps(comparison_data, ensure_ascii=True)
    json_b64 = base64.b64encode(json_str.encode('utf-8')).decode('ascii')
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parser Memory Comparison: {label_before} vs {label_after}</title>
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
            --accent-red: #f85149;
            --accent-yellow: #d29922;
            --accent-purple: #a371f7;
            --font-mono: 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace;
            --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }}
        
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: var(--font-sans);
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
        }}
        
        .header {{
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border-color);
            padding: 24px 32px;
        }}
        
        .header h1 {{
            font-size: 24px;
            font-weight: 600;
            margin-bottom: 8px;
        }}
        
        .header-subtitle {{
            color: var(--text-secondary);
            font-size: 14px;
            margin-bottom: 16px;
        }}
        
        .summary-cards {{
            display: flex;
            gap: 24px;
            flex-wrap: wrap;
        }}
        
        .summary-card {{
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 16px 24px;
            min-width: 180px;
        }}
        
        .summary-card .label {{
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }}
        
        .summary-card .value {{
            font-size: 24px;
            font-weight: 600;
            font-family: var(--font-mono);
        }}
        
        .summary-card .value.better {{ color: var(--accent-green); }}
        .summary-card .value.worse {{ color: var(--accent-red); }}
        .summary-card .value.neutral {{ color: var(--text-secondary); }}
        
        .main-content {{
            height: calc(100vh - 160px);
            overflow: auto;
            padding: 24px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            table-layout: fixed;
        }}
        
        th {{
            text-align: left;
            padding: 12px 16px;
            background: var(--bg-tertiary);
            color: var(--text-secondary);
            font-weight: 500;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            position: sticky;
            top: 0;
            cursor: pointer;
            user-select: none;
        }}
        
        th:hover {{
            background: var(--bg-hover);
        }}
        
        th.sorted::after {{
            content: ' ▼';
            font-size: 10px;
        }}
        
        th.sorted.asc::after {{
            content: ' ▲';
        }}
        
        td {{
            padding: 10px 16px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .query-row {{
            cursor: pointer;
            transition: background 0.15s;
        }}
        
        .query-row:hover {{
            background: var(--bg-tertiary);
        }}
        
        .query-row.selected {{
            background: rgba(88, 166, 255, 0.1);
        }}
        
        .col-id {{
            font-family: var(--font-mono);
            color: var(--accent-purple);
            font-weight: 600;
            width: 50px;
        }}
        
        .col-query {{
            font-family: var(--font-mono);
            font-size: 12px;
            color: var(--text-secondary);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .col-before, .col-after {{
            font-family: var(--font-mono);
            text-align: right;
            width: 90px;
        }}
        
        .col-diff {{
            font-family: var(--font-mono);
            font-weight: 600;
            text-align: right;
            width: 130px;
        }}
        
        .col-diff.better {{ color: var(--accent-green); }}
        .col-diff.worse {{ color: var(--accent-red); }}
        .col-diff.neutral {{ color: var(--text-muted); }}
        
        .detail-row td {{
            padding: 0 !important;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .detail-content {{
            background: var(--bg-secondary);
            padding: 20px 24px;
            border-top: 1px solid var(--accent-blue);
        }}
        
        .detail-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 16px;
            gap: 24px;
        }}
        
        .detail-query {{
            font-family: var(--font-mono);
            font-size: 12px;
            background: var(--bg-tertiary);
            padding: 12px 16px;
            border-radius: 6px;
            white-space: pre-wrap;
            word-break: break-word;
            flex: 1;
            max-height: 100px;
            overflow: auto;
        }}
        
        .detail-stats {{
            display: flex;
            gap: 16px;
            flex-shrink: 0;
        }}
        
        .stat-box {{
            background: var(--bg-tertiary);
            padding: 10px 16px;
            border-radius: 6px;
            text-align: center;
            min-width: 100px;
        }}
        
        .stat-box .label {{
            font-size: 9px;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-bottom: 4px;
        }}
        
        .stat-box .value {{
            font-family: var(--font-mono);
            font-size: 16px;
            font-weight: 600;
        }}
        
        .stat-box.diff .value.better {{ color: var(--accent-green); }}
        .stat-box.diff .value.worse {{ color: var(--accent-red); }}
        
        .placeholder {{
            color: var(--text-muted);
            text-align: center;
            padding: 48px;
        }}
        
        .collapsible {{
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            margin-bottom: 12px;
            overflow: hidden;
        }}
        
        .collapsible-header {{
            padding: 12px 16px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            user-select: none;
            transition: background 0.15s;
        }}
        
        .collapsible-header:hover {{
            background: var(--bg-hover);
        }}
        
        .collapsible-header h3 {{
            font-size: 13px;
            font-weight: 500;
            color: var(--text-primary);
        }}
        
        .collapsible-header .toggle {{
            font-size: 12px;
            color: var(--text-muted);
            transition: transform 0.2s;
        }}
        
        .collapsible.open .collapsible-header .toggle {{
            transform: rotate(180deg);
        }}
        
        .collapsible-content {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
        }}
        
        .collapsible.open .collapsible-content {{
            max-height: 2000px;
        }}
        
        .collapsible-inner {{
            padding: 12px 16px;
            border-top: 1px solid var(--border-color);
        }}
        
        .report-text {{
            font-family: var(--font-mono);
            font-size: 11px;
            line-height: 1.5;
            white-space: pre-wrap;
            color: var(--text-secondary);
            background: var(--bg-primary);
            padding: 12px;
            border-radius: 4px;
            max-height: 300px;
            overflow: auto;
        }}
        
        .side-by-side {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }}
        
        .side-label {{
            font-size: 10px;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-bottom: 6px;
        }}
        
        .filter-box {{
            margin-bottom: 16px;
        }}
        
        .filter-box input {{
            width: 100%;
            padding: 10px 14px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            font-size: 13px;
            outline: none;
        }}
        
        .filter-box input:focus {{
            border-color: var(--accent-blue);
        }}
        
        .filter-box input::placeholder {{
            color: var(--text-muted);
        }}
        
        ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        ::-webkit-scrollbar-track {{ background: var(--bg-primary); }}
        ::-webkit-scrollbar-thumb {{ background: var(--bg-hover); border-radius: 4px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Parser Memory Comparison</h1>
        <div class="header-subtitle">{label_before} → {label_after}</div>
        <div class="summary-cards">
            <div class="summary-card">
                <div class="label">Queries</div>
                <div class="value">{len(all_ids)}</div>
            </div>
            <div class="summary-card">
                <div class="label">{label_before} Total</div>
                <div class="value">{format_bytes(total_before)}</div>
            </div>
            <div class="summary-card">
                <div class="label">{label_after} Total</div>
                <div class="value">{format_bytes(total_after)}</div>
            </div>
            <div class="summary-card">
                <div class="label">Difference</div>
                <div class="value {total_class}">{total_diff}</div>
            </div>
        </div>
    </div>
    
    <div class="main-content">
        <div class="filter-box">
            <input type="text" id="filterInput" placeholder="Filter queries..." oninput="filterTable(this.value)">
        </div>
        <table>
            <thead>
                <tr>
                    <th data-sort="id" class="sorted">ID</th>
                    <th data-sort="query">Query</th>
                    <th data-sort="before">{label_before}</th>
                    <th data-sort="after">{label_after}</th>
                    <th data-sort="diff">Difference</th>
                </tr>
            </thead>
            <tbody id="tableBody">
                {rows_html}
            </tbody>
        </table>
    </div>
    
    <script>
        const DATA = JSON.parse(atob("{json_b64}"));
        
        let currentSort = 'id';
        let sortAsc = true;
        
        function formatBytes(b) {{
            if (b < 1024) return b + ' B';
            if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
            return (b / (1024 * 1024)).toFixed(2) + ' MB';
        }}
        
        function extractJeprofTotal(text) {{
            if (!text) return 0;
            const match = text.match(/Total:\\s*([\\d,]+)\\s*B/);
            return match ? parseInt(match[1].replace(/,/g, '')) : 0;
        }}
        
        let openedId = null;
        
        function toggleQuery(id) {{
            const detailRow = document.getElementById('detail-' + id);
            const queryRow = document.querySelector(`.query-row[data-id="${{id}}"]`);
            
            // Close previously opened row
            if (openedId && openedId !== id) {{
                const prevDetail = document.getElementById('detail-' + openedId);
                const prevRow = document.querySelector(`.query-row[data-id="${{openedId}}"]`);
                if (prevDetail) prevDetail.style.display = 'none';
                if (prevRow) prevRow.classList.remove('selected');
            }}
            
            // Toggle current row
            if (detailRow.style.display === 'none') {{
                detailRow.style.display = 'table-row';
                queryRow.classList.add('selected');
                openedId = id;
                
                // Populate detail content
                const contentDiv = document.getElementById('detail-content-' + id);
                if (!contentDiv.innerHTML) {{
                    populateDetail(id, contentDiv);
                }}
            }} else {{
                detailRow.style.display = 'none';
                queryRow.classList.remove('selected');
                openedId = null;
            }}
        }}
        
        function populateDetail(id, container) {{
            const qBefore = DATA.queries_before[id] || {{}};
            const qAfter = DATA.queries_after[id] || {{}};
            const query = qBefore.query || qAfter.query || 'N/A';
            
            const allocBefore = extractJeprofTotal(qBefore.jeprof_text);
            const allocAfter = extractJeprofTotal(qAfter.jeprof_text);
            const diff = allocAfter - allocBefore;
            const diffClass = diff < 0 ? 'better' : diff > 0 ? 'worse' : 'neutral';
            const pctChange = allocBefore > 0 ? ((diff / allocBefore) * 100).toFixed(1) : '0';
            
            container.innerHTML = `
                <div class="detail-header">
                    <div class="detail-query">${{escapeHtml(query)}}</div>
                    <div class="detail-stats">
                        <div class="stat-box">
                            <div class="label">${{DATA.label_before}}</div>
                            <div class="value">${{formatBytes(allocBefore)}}</div>
                        </div>
                        <div class="stat-box">
                            <div class="label">${{DATA.label_after}}</div>
                            <div class="value">${{formatBytes(allocAfter)}}</div>
                        </div>
                        <div class="stat-box diff">
                            <div class="label">Change</div>
                            <div class="value ${{diffClass}}">${{diff >= 0 ? '+' : ''}}${{pctChange}}%</div>
                        </div>
                    </div>
                </div>
                
                <div class="collapsible" onclick="event.stopPropagation(); this.classList.toggle('open')">
                    <div class="collapsible-header">
                        <h3>📊 Profile Report (Bytes)</h3>
                        <span class="toggle">▼</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="collapsible-inner">
                            <div class="side-by-side">
                                <div>
                                    <div class="side-label">${{DATA.label_before}}</div>
                                    <pre class="report-text">${{escapeHtml(qBefore.jeprof_text || 'No data')}}</pre>
                                </div>
                                <div>
                                    <div class="side-label">${{DATA.label_after}}</div>
                                    <pre class="report-text">${{escapeHtml(qAfter.jeprof_text || 'No data')}}</pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="collapsible" onclick="event.stopPropagation(); this.classList.toggle('open')">
                    <div class="collapsible-header">
                        <h3>🔢 Profile Report (Objects)</h3>
                        <span class="toggle">▼</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="collapsible-inner">
                            <div class="side-by-side">
                                <div>
                                    <div class="side-label">${{DATA.label_before}}</div>
                                    <pre class="report-text">${{escapeHtml(qBefore.jeprof_objects || 'No data')}}</pre>
                                </div>
                                <div>
                                    <div class="side-label">${{DATA.label_after}}</div>
                                    <pre class="report-text">${{escapeHtml(qAfter.jeprof_objects || 'No data')}}</pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="collapsible" onclick="event.stopPropagation(); this.classList.toggle('open')">
                    <div class="collapsible-header">
                        <h3>📈 Raw Stats (jemalloc)</h3>
                        <span class="toggle">▼</span>
                    </div>
                    <div class="collapsible-content">
                        <div class="collapsible-inner">
                            <div class="side-by-side">
                                <div>
                                    <div class="side-label">${{DATA.label_before}}</div>
                                    <pre class="report-text">Query length: ${{qBefore.query_length || 0}} bytes
Allocated before: ${{formatBytes(qBefore.allocated_before || 0)}}
Allocated after: ${{formatBytes(qBefore.allocated_after || 0)}}
Diff (jemalloc): ${{formatBytes(qBefore.allocated_diff || 0)}}</pre>
                                </div>
                                <div>
                                    <div class="side-label">${{DATA.label_after}}</div>
                                    <pre class="report-text">Query length: ${{qAfter.query_length || 0}} bytes
Allocated before: ${{formatBytes(qAfter.allocated_before || 0)}}
Allocated after: ${{formatBytes(qAfter.allocated_after || 0)}}
Diff (jemalloc): ${{formatBytes(qAfter.allocated_diff || 0)}}</pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }}
        
        function escapeHtml(text) {{
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }}
        
        function filterTable(search) {{
            search = search.toLowerCase();
            document.querySelectorAll('.query-row').forEach(row => {{
                const id = row.dataset.id;
                const rowData = DATA.rows.find(r => r.id == id);
                const matches = rowData && (
                    rowData.query.toLowerCase().includes(search) ||
                    id.toString().includes(search)
                );
                row.style.display = matches ? '' : 'none';
                // Also hide detail row when filtering
                const detailRow = document.getElementById('detail-' + id);
                if (detailRow && !matches) {{
                    detailRow.style.display = 'none';
                }}
            }});
        }}
        
        document.querySelectorAll('th[data-sort]').forEach(th => {{
            th.addEventListener('click', () => {{
                const field = th.dataset.sort;
                
                if (currentSort === field) {{
                    sortAsc = !sortAsc;
                }} else {{
                    currentSort = field;
                    sortAsc = field === 'id';
                }}
                
                document.querySelectorAll('th').forEach(t => t.classList.remove('sorted', 'asc'));
                th.classList.add('sorted');
                if (sortAsc) th.classList.add('asc');
                
                const tbody = document.getElementById('tableBody');
                const queryRows = Array.from(tbody.querySelectorAll('.query-row'));
                
                queryRows.sort((a, b) => {{
                    const ra = DATA.rows.find(r => r.id == a.dataset.id);
                    const rb = DATA.rows.find(r => r.id == b.dataset.id);
                    
                    let va, vb;
                    if (field === 'id') {{
                        va = ra.id; vb = rb.id;
                    }} else if (field === 'query') {{
                        va = ra.query; vb = rb.query;
                        return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
                    }} else if (field === 'before') {{
                        va = ra.before; vb = rb.before;
                    }} else if (field === 'after') {{
                        va = ra.after; vb = rb.after;
                    }} else {{
                        va = ra.after - ra.before;
                        vb = rb.after - rb.before;
                    }}
                    
                    return sortAsc ? va - vb : vb - va;
                }});
                
                // Reorder rows along with their detail rows
                queryRows.forEach(row => {{
                    const id = row.dataset.id;
                    const detailRow = document.getElementById('detail-' + id);
                    tbody.appendChild(row);
                    if (detailRow) tbody.appendChild(detailRow);
                }});
            }});
        }});
        
        // No auto-selection - user clicks to expand
    </script>
</body>
</html>'''
    
    return html_content


def main():
    parser = argparse.ArgumentParser(description='Generate comparison report between two profiler runs')
    parser.add_argument('--before', '-b', required=True, help='Results JSON from "before" run')
    parser.add_argument('--after', '-a', required=True, help='Results JSON from "after" run')
    parser.add_argument('--output', '-o', required=True, help='Output HTML file')
    parser.add_argument('--label-before', default='Before', help='Label for "before" version')
    parser.add_argument('--label-after', default='After', help='Label for "after" version')
    
    args = parser.parse_args()
    
    # Read both result files
    before_path = Path(args.before)
    after_path = Path(args.after)
    
    if not before_path.exists():
        print(f"Error: Before file not found: {args.before}", file=sys.stderr)
        sys.exit(1)
    
    if not after_path.exists():
        print(f"Error: After file not found: {args.after}", file=sys.stderr)
        sys.exit(1)
    
    with open(before_path, 'r', encoding='utf-8') as f:
        results_before = json.load(f)
    
    with open(after_path, 'r', encoding='utf-8') as f:
        results_after = json.load(f)
    
    # Generate comparison report
    html_content = generate_comparison_report(
        results_before, 
        results_after,
        args.label_before,
        args.label_after
    )
    
    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Comparison report generated: {output_path}")
    print(f"Comparing {len(results_before.get('queries', []))} vs {len(results_after.get('queries', []))} queries")


if __name__ == '__main__':
    main()

