
rm sqlitebench_test*
./build/sqliteBench --journal_mode=wal --benchmarks=pingpong_mixed --page_size=16384
