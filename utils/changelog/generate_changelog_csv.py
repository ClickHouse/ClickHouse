import glob
import re
import csv
import os

def parse_changelogs():
    records = []
    files = sorted(glob.glob('docs/resources/changelogs/oss/*.mdx'))
    
    # RegEx patterns
    version_pat = re.compile(r'###\s+(?:<a\s+id=.*?></a>\s*)?ClickHouse release\s+(?:v)?([0-9\.]+)\s*(?:LTS)?[,\s]+([0-9]{4}-[0-9]{2}-[0-9]{2})', re.IGNORECASE)
    category_pat = re.compile(r'^####\s+([^#{\n]+)')
    pr_pat = re.compile(r'\[#([0-9]+)\]\(https://github\.com/ClickHouse/ClickHouse/(?:pull|issues)/[0-9]+\)')
    author_pat = re.compile(r'\(\[([^\]]+)\]\(https://github\.com/[^)]+\)\)')
    
    setting_pat = re.compile(r'(?:setting|parameter)\s+`([a-zA-Z0-9_]+)`|`([a-zA-Z0-9_]*(?:allow|enable|optimize|use|max|min|query|join|session|http|format|output|input)_[a-zA-Z0-9_]+)`')
    
    for f in files:
        current_version = ''
        current_date = ''
        current_category = ''
        
        with open(f, 'r', encoding='utf-8') as fp:
            for line in fp:
                line_str = line.strip()
                v_match = version_pat.search(line_str)
                if v_match:
                    current_version = v_match.group(1).strip()
                    current_date = v_match.group(2).strip()
                    continue
                
                c_match = category_pat.match(line_str)
                if c_match:
                    current_category = c_match.group(1).strip().rstrip(':')
                    continue
                
                if line_str.startswith('* ') and current_version:
                    desc = line_str[2:].strip()
                    
                    prs = [int(p) for p in pr_pat.findall(desc)]
                    primary_pr = prs[0] if prs else None
                    
                    authors = author_pat.findall(desc)
                    author = ', '.join(authors) if authors else None
                    
                    is_experimental = 1 if ('experimental' in current_category.lower() or 'experimental' in desc.lower() or '`allow_experimental_' in desc) else 0
                    is_breaking = 1 if ('backward incompatible' in current_category.lower() or 'breaking' in desc.lower() or 'incompatible' in desc.lower()) else 0
                    is_security_fix = 1 if ('security' in current_category.lower() or 'cve-' in desc.lower() or 'security fix' in desc.lower()) else 0
                    
                    default_enabled = ""
                    if re.search(r'(enabled by default|default:\s*`?(?:true|1)`?|is now the default|now enabled by default)', desc, re.IGNORECASE):
                        default_enabled = 1
                    elif re.search(r'(disabled by default|default:\s*`?(?:false|0)`?|can be enabled by|disabled until)', desc, re.IGNORECASE):
                        default_enabled = 0
                        
                    action = ""
                    lower_desc = desc.lower()
                    if lower_desc.startswith('add') or 'now supports' in lower_desc or 'implement' in lower_desc:
                        action = 'Added'
                    elif lower_desc.startswith('fix') or 'fixed' in lower_desc:
                        action = 'Fixed'
                    elif 'enable' in lower_desc:
                        action = 'Enabled'
                    elif 'disable' in lower_desc:
                        action = 'Disabled'
                    elif 'deprecat' in lower_desc:
                        action = 'Deprecated'
                    elif 'remov' in lower_desc:
                        action = 'Removed'
                    elif 'optimi' in lower_desc or 'improve' in lower_desc or 'performance' in lower_desc:
                        action = 'Optimized'
                        
                    setting_matches = setting_pat.findall(desc)
                    setting_name = ""
                    if setting_matches:
                        for s1, s2 in setting_matches:
                            s = s1 or s2
                            if s and len(s) > 3:
                                setting_name = s
                                break
                                
                    records.append({
                        'version': current_version,
                        'date': current_date,
                        'type': current_category,
                        'action': action,
                        'is_experimental': is_experimental,
                        'is_breaking': is_breaking,
                        'is_security_fix': is_security_fix,
                        'setting_name': setting_name,
                        'default_enabled': default_enabled,
                        'pull_request': primary_pr if primary_pr is not None else "",
                        'author': author if author is not None else "",
                        'description': desc
                    })
    return records

if __name__ == '__main__':
    recs = parse_changelogs()
    print(f"Total parsed records: {len(recs)}")
    out_dir = 'docs/resources/changelogs'
    out_csv = os.path.join(out_dir, 'changelog.csv')
    out_zst = os.path.join(out_dir, 'changelog.csv.zst')
    fieldnames = [
        'version', 'date', 'type', 'action', 'is_experimental', 'is_breaking',
        'is_security_fix', 'setting_name', 'default_enabled', 'pull_request', 'author', 'description'
    ]
    os.makedirs(out_dir, exist_ok=True)
    with open(out_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(recs)
    
    # Compress with zstd
    import subprocess
    subprocess.run(['zstd', '-19', '-f', out_csv, '-o', out_zst], check=True)
    os.remove(out_csv)
    print(f"Successfully generated {out_zst}")
