rm sqlitebench_test*

./build/sqliteBench --journal_mode=wal --benchmarks=fillrand_variable_kv_length --page_size=16384 --key_size=16 --value_size=100
