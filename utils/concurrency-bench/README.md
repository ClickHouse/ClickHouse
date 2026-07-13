# Concurrency Stress Testing Tools

A small toolkit for stress-testing concurrency-related features in ClickHouse. It runs `clickhouse-bench` for each query while increasing the number of concurrent clients from 1 to 32. This helps evaluate scheduling- and concurrency-related changes.

## Prerequisites
- ClickHouse server running and accessible.
- `clickhouse-bench` and `clickhouse-client` available in PATH.
- Bash and Python 3 (for the log-to-JSON converter).
- This directory contains: `run.sh`, `queries.txt`, `logs2json.py`, and `index.html`.

## Testing procedure
1. Start the ClickHouse server and set up any concurrency features you want to test.
2. Prepare `queries.txt`. The file is semicolon-separated with two columns:
   - Column 1: test duration per concurrency level in seconds.
   - Column 2: SQL query to test.

3. Run the benchmark. Running in the background is recommended:
   ```
   nohup ./run.sh > run.log 2>&1 &
   ```

4. After completion, results for each query will be saved into separate `Q*.log` files. Convert them to JSON:
   ```
   ./logs2json.py /path/to/workdir/Q*.log > results.json
   ```

5. Visualize results. Add your result JSON file(s) to `index.html`:
   ```js
   const DATASET_FILES = [
     "./no-concurrency-control.json",
     "./concurrency-control.json"
   ];
   ```
6. Open `index.html` in your browser.

## Notes
- Default concurrency levels are 1..32. To change them, edit `run.sh`.
- Ensure queries in `queries.txt` do not contain `;` since it is used as the separator.
