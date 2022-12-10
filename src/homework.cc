#include "bench.h"

namespace sqliteBench
{
  int status;
  char *err_msg = NULL;

  // #1. Write a code for setting the journal mode in the SQLite database engine
  // [Requirement]
  // (1) This function set jounral mode using "FLAGS_journal_mode" extern variable (user input)
  // (2) This function check journal mode; if user try to set wrong journal mode then return -2
  // (3) This function returns status (int data type) of sqlite API function
  int Benchmark::benchmark_setJournalMode()
  {
    if (strlen(FLAGS_journal_mode) == 0)
      return 0;

    const char *available_options[] = {
        "delete",
        "persist",
        "truncate",
        "memory",
        "wal",
        "off"};
    bool is_valid = false;
    char fill_stmt[100];

    for (int i = 0; i < 6; i++)
    {
      if (!strcmp(FLAGS_journal_mode, available_options[i]))
      {
        is_valid = true;
        snprintf(fill_stmt, sizeof(fill_stmt), "PRAGMA journal_mode = %s", FLAGS_journal_mode);
        break;
      }
    }

    if (!is_valid)
    {
      fprintf(stderr, "WARNING: %s is not valid journal mode, "
                      "set to the default option(delete)\n",
              FLAGS_journal_mode);
      snprintf(fill_stmt, sizeof(fill_stmt), "PRAGMA journal_mode = delete");
    }
    status = sqlite3_exec(db_, fill_stmt, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);
    return status;
  }

  // #2. Write a code for setting page size in the SQLite database engine
  // [Requirement]
  // (1) This function set page size using "FLAGS_page_size" extern variable (user input)
  // (2) This function return status (int data type) of sqltie API function
  // (3) This function is called at benchmark_open() in bench.cc
  int Benchmark::benchmark_setPageSize()
  {
    char *err_msg = NULL;
    char fill_stmt[100];
    snprintf(fill_stmt, sizeof(fill_stmt), "PRAGMA page_size = %d", FLAGS_page_size);
    status = sqlite3_exec(db_, fill_stmt, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);
    return status;
  }

  // #3. Write a code for insert function (direct SQL execution mode)
  // [Requriement]
  // (1) This function fill random key-value data using direct qurey execution sqlite API function
  //     (please refer to lecture slide project 3)
  // (2) This function has single arguemnt num_ which is total number of operations
  // (3) This function create SQL statement (key-value pair) do not modfiy this code
  // (4) This function execute given SQL statement
  // (5) This function is called at benchmark_open() in bench.cc
  int Benchmark::benchmark_directFillRand(int num_)
  {
    for (int i = 0; i < num_; i++)
    {
      //      DO NOT MODIFY HERE     //
      const char *value = gen_.rand_gen_generate(FLAGS_value_size);
      char key[100];
      const int k = gen_.rand_next() % num_;

      snprintf(key, sizeof(key), "%016d", k);
      char fill_stmt[100];
      snprintf(fill_stmt, sizeof(fill_stmt), "INSERT INTO test values (%s , x'%x')", key, value);
      ////////////////////////////////

      // fprintf(stderr, "%s, %x\n", key, value);
      status = sqlite3_exec(db_, fill_stmt, NULL, NULL, &err_msg);
      finished_single_op();
      exec_error_check(status, err_msg);
    }
    return 0;
  }

  // xxx(homework)
  // write your own benchmark functions
  // you can add multiple functions as you like
  // you can change function name. Here example is literally example.
  int Benchmark::benchmark_fillVariableKeyVariableValue(
      int num_entries,
      int max_key_size,
      int max_value_size,
      int entries_per_batch)
  {
    fprintf(stderr, "example functions works!\n");
    /*
      variable key length and variable value length fill benchmark
    */
  }

}; // namespace sqliteBench
