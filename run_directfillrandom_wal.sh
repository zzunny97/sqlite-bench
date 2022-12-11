rm sqlitebench_test*

./build/sqliteBench --journal_mode=wal --page_size=16384 --benchmarks=directfillrandom
