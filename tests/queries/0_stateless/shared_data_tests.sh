client="/home/avogar/ClickHouse/build-release/programs/clickhouse-client --host iw8004ysau.eu-west-1.aws.clickhouse-staging.com --secure --password NrbrMXK2~utUe"

$client -q "create or replace table test_perf_3 (query String, table String, time_ms UInt64, memory_usage UInt64, with_filesystem_cache Bool) engine=MergeTree order by tuple()"
#$client -q "create or replace table test_perf_2 (query String, table String, time_ms UInt64, memory_usage UInt64, with_filesystem_cache Bool) engine=MergeTree order by tuple()"

query_id_suffix=$(date +%s)

#for with_cache in 0 1; do
#  for num_paths in 10 100 1000; do
#      for part_type in "compact" "wide"; do
##          for bucket in 1 8 32; do
#              for impl in "no_shared_data"; do
#                  table="test_${part_type}_${num_paths}_paths_${impl}"
#                  for query in "select json" "select json.a1" "select json.non_existing_path" "select json.a1, json.a2, json.a3, json.a4, json.a5" "select json.arr" "select json.arr[].b0" "select json.arr[].b0, json.arr[].b1, json.arr[].b2, json.arr[].b3, json.arr[].b4"; do
#                      perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
#                      echo "Run query: $perf_query"
#                      query_id="${perf_query}_${query_id_suffix}"
#                      $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
#                      $client -q "system flush logs on cluster default"
#                      $client -q "insert into test_perf_1 select '$query', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$perf_query%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
#                  done
#              done
##          done
#      done
#  done
#done


for with_cache in 0 1; do
  for part_type in "compact" "wide"; do
      for bucket in 1 8 32; do
          for impl in "old" "new" "new_with_substreams"; do
            table="gharchive_${part_type}_${bucket}_bucket_${impl}"
              for query in "select json" "select json.payload.pull_request.id" "select json.payload.pull_request.id, json.payload.pull_request.number, json.payload.pull_request.state, json.payload.pull_request.url, json.payload.pull_request.user.id" "select json.payload.release.assets[].state" "select json.payload.release.assets[].state, json.payload.release.assets[].size, json.payload.release.assets[].id, json.payload.release.assets[].name, json.payload.release.assets[].label"; do
                  perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
                  echo "Run query: $perf_query"
                  query_id="${perf_query}_${query_id_suffix}"
                  $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
                  $client -q "system flush logs on cluster default"
                  $client -q "insert into test_perf_1 select '$query', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$perf_query%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
              done
          done
      done
  done

  for part_type in "compact" "wide"; do
      table="gharchive_${part_type}_no_shared_data"
      for query in "select json" "select json.payload.pull_request.id" "select json.payload.pull_request.id, json.payload.pull_request.number, json.payload.pull_request.state, json.payload.pull_request.url, json.payload.pull_request.user.id" "select json.payload.release.assets[].state" "select json.payload.release.assets[].state, json.payload.release.assets[].size, json.payload.release.assets[].id, json.payload.release.assets[].name, json.payload.release.assets[].label"; do
          perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
          echo "Run query: $perf_query"
          query_id="${perf_query}_${query_id_suffix}"
          $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
          $client -q "system flush logs on cluster default"
          $client -q "insert into test_perf_1 select '$query', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$perf_query%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
      done
  done
done

escape_single_quotes() {
  local input="$*"
  # Replace each single quote with \'
  local escaped="${input//\'/\\\'}"
  printf "%s\n" "$escaped"
}

#for with_cache in 0 1; do
#  for num_paths in 10 100 1000 10000; do
#      for part_type in "wide" "compact"; do
#          for impl in "json_shared_data_old" "json_shared_data_new"; do
#              table="test_${part_type}_${num_paths}_paths_${impl}"
#              for query in "select json.key0" "select json.non_existing_key" "select json.key0, json.key1, json.key2, json.key3, json.key4"; do
#                  perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
#                  echo "Run query: $perf_query"
#                  query_id="${perf_query}_${query_id_suffix}"
#                  $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
#                  $client -q "system flush logs on cluster default"
#                  $client -q "insert into test_perf_2 select '$query', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$perf_query%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
#              done
#          done
#
#          if [ $num_paths != "10000" ]; then
#              table="test_${part_type}_${num_paths}_paths_json_no_shared_data"
#              for query in "select json.key0" "select json.non_existing_key" "select json.key0, json.key1, json.key2, json.key3, json.key4"; do
#                  perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
#                  echo "Run query: $perf_query"
#
#                  query_id="${perf_query}_${query_id_suffix}"
#                  $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
#                  $client -q "system flush logs on cluster default"
#                  $client -q "insert into test_perf_2 select '$query', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$perf_query%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
#              done
#          fi
#
#          table="test_${part_type}_${num_paths}_paths_map"
#          for query in "select json['key0']" "select json['non_existing_key']" "select json['key0'], json['key1'], json['key2'], json['key3'], json['key4']"; do
#              perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
#              echo "Run query: $perf_query"
#
#              query_id="${perf_query}_${query_id_suffix}"
#              $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
#                  $client -q "system flush logs on cluster default"
#              $client -q "insert into test_perf_2 select '$(escape_single_quotes $query)', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$(escape_single_quotes $perf_query)%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
#
#          done
#
#          table="test_${part_type}_${num_paths}_paths_string"
#          for query in "select JSONExtractInt(json, 'key0')" "select JSONExtractInt(json, 'non_existing_key')" "select JSONExtractInt(json, 'key0'), JSONExtractInt(json, 'key1'), JSONExtractInt(json, 'key2'), JSONExtractInt(json, 'key3'), JSONExtractInt(json, 'key4')"; do
#              perf_query="${query} from $table format Null settings max_parallel_replicas=1, enable_filesystem_cache=${with_cache}"
#              echo "Run query: $perf_query"
#              query_id="${perf_query}_${query_id_suffix}"
#              $client -m -q "$perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query; $perf_query;" --query_id="$query_id"
#              $client -q "system flush logs on cluster default"
#              $client -q "insert into test_perf_2 select '$(escape_single_quotes $query)', '$table', avg(query_duration_ms), avg(memory_usage), $with_cache from (select query_duration_ms, memory_usage from clusterAllReplicas(default, system.query_log) where query_id like '%$(escape_single_quotes $perf_query)%$query_id_suffix%' and type='QueryFinish' order by event_time_microseconds desc limit 5)"
#          done
#      done
#  done
#done
