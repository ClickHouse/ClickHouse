
echo "This will create some tables in database 'bench' and time some queries, and then fit a linear regression to the timings. Clickhouse server must be running locally."
#make sql queries
python make-sql.py
#run sql queries
echo "Creating tables (unless they already exist)."
 ../../../gh2-ClickHouse/build/programs/clickhouse client --database bench --time --multiquery < create_tables.sql

echo "Running SELECT queries. This may take some time (expect 30 minutes or more)."
 ../../../gh2-ClickHouse/build/programs/clickhouse client --reorder_joins 0 --database bench --time --multiquery < queries.sql > output.txt 2> timings.txt

python fit-regression.py

#The coefficients that are found correspond to a very simplistic model of a join, 
#so it's a good idea to further evaluate whether or not they are any good.
#For example, to benchmark against TPC-H:
#    1) Generate the data, using the code from tpc.org
#    2) Pipe in the queries from https://github.com/MonetDB/tpch-clickhouse , passing --time to clickhouse-client
