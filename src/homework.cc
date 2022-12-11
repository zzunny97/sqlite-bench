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
        char uppered_journal_mode[100];
        for (int i = 0; i < strlen(FLAGS_journal_mode); i++)
        {
          uppered_journal_mode[i] = toupper(FLAGS_journal_mode[i]);
        }
        snprintf(fill_stmt, sizeof(fill_stmt), "PRAGMA journal_mode = %s", uppered_journal_mode);
        break;
      }
    }

    if (!is_valid)
    {
      fprintf(stderr, "WARNING: %s is not valid journal mode, "
                      "set to the default option (delete)\n",
              FLAGS_journal_mode);
      snprintf(fill_stmt, sizeof(fill_stmt), "PRAGMA journal_mode = DELETE");
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
    if (FLAGS_page_size <= 0)
    {
      fprintf(stderr, "ERROR: Page size should be more than 0\n");
      exit(1);
    }
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

  void Benchmark::benchmark_pingpong_mixed(
      bool write_sync,
      int order,
      int state,
      int num_entries,
      int value_size,
      int entries_per_batch)
  {
    /* Create new database if state == FRESH */
    if (state == FRESH)
    {
      if (FLAGS_use_existing_db)
      {
        message_ = static_cast<char *>(malloc(sizeof(char) * 100));
        strcpy(message_, "skipping (--use_existing_db is true)");
        return;
      }
      sqlite3_close(db_);
      db_ = NULL;
      benchmark_open();
      start();
    }

    if (num_entries != num_)
    {
      char *msg = static_cast<char *>(malloc(sizeof(char) * 100));
      snprintf(msg, 100, "(%d ops)", num_entries);
      message_ = msg;
    }

    char *err_msg = NULL;
    int status;

    sqlite3_stmt *read_stmt, *replace_stmt, *begin_trans_stmt, *end_trans_stmt;
    const char *read_str = "SELECT * FROM test WHERE key = ?";
    const char *replace_str = "REPLACE INTO test (key, value) VALUES (?, ?)";
    const char *begin_trans_str = "BEGIN TRANSACTION";
    const char *end_trans_str = "END TRANSACTION";

    /* Check for synchronous flag in options */
    const char *sync_stmt = (write_sync) ? "PRAGMA synchronous = FULL" : "PRAGMA synchronous = OFF";
    status = sqlite3_exec(db_, sync_stmt, NULL, NULL, &err_msg);
    exec_error_check(status, err_msg);

    /* Preparing sqlite3 statements */
    status = sqlite3_prepare_v2(db_, read_str, -1,
                                &read_stmt, NULL);
    error_check(status);
    status = sqlite3_prepare_v2(db_, replace_str, -1,
                                &replace_stmt, NULL);
    error_check(status);
    status = sqlite3_prepare_v2(db_, begin_trans_str, -1,
                                &begin_trans_stmt, NULL);
    error_check(status);
    status = sqlite3_prepare_v2(db_, end_trans_str, -1,
                                &end_trans_stmt, NULL);
    error_check(status);

    bool transaction = (entries_per_batch > 1);
    for (int i = 0; i < num_entries; i += entries_per_batch)
    {
      /* Begin write transaction */
      if (FLAGS_transaction && transaction)
      {
        status = sqlite3_step(begin_trans_stmt);
        step_error_check(status);
        status = sqlite3_reset(begin_trans_stmt);
        error_check(status);
      }

      /* Create and execute SQL statements */
      for (int j = 0; j < entries_per_batch; j++)
      {
        // fprintf(stderr, "ping\n");

        /* Bind KV values into replace_stmt */
        if ((i + j) % 2 == 0)
        {
          const char *value = gen_.rand_gen_generate(value_size);

          /* Create values for key-value pair */
          const int k = (order == SEQUENTIAL) ? i + j : (gen_.rand_next() % num_entries);
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);

          status = sqlite3_bind_blob(replace_stmt, 1, key, 16, SQLITE_STATIC);
          error_check(status);
          status = sqlite3_bind_blob(replace_stmt, 2, value,
                                     value_size, SQLITE_STATIC);
          error_check(status);

          /* Execute replace_stmt */
          bytes_ += value_size + strlen(key);
          status = sqlite3_step(replace_stmt);
          step_error_check(status);

          /* Reset SQLite statement for another use */
          status = sqlite3_clear_bindings(replace_stmt);
          error_check(status);
          status = sqlite3_reset(replace_stmt);
          error_check(status);

          finished_single_op();
        }
        else
        {
          // fprintf(stderr, "pong\n");
          char key[100];
          int k = (order == SEQUENTIAL) ? i + j : (gen_.rand_next() % reads_);
          snprintf(key, sizeof(key), "%016d", k);

          /* Bind key value into read_stmt */
          status = sqlite3_bind_blob(read_stmt, 1, key, 16, SQLITE_STATIC);
          error_check(status);

          /* Execute read statement */
          while ((status = sqlite3_step(read_stmt)) == SQLITE_ROW)
          {
          }
          step_error_check(status);

          /* Reset SQLite statement for another use */
          status = sqlite3_clear_bindings(read_stmt);
          error_check(status);
          status = sqlite3_reset(read_stmt);
          error_check(status);
          finished_single_op();
        }
      }

      /* End write transaction */
      if (FLAGS_transaction && transaction)
      {
        status = sqlite3_step(end_trans_stmt);
        step_error_check(status);
        status = sqlite3_reset(end_trans_stmt);
        error_check(status);
      }
    }

    status = sqlite3_finalize(read_stmt);
    error_check(status);
    status = sqlite3_finalize(replace_stmt);
    error_check(status);
    status = sqlite3_finalize(begin_trans_stmt);
    error_check(status);
    status = sqlite3_finalize(end_trans_stmt);
    error_check(status);
  }

  // fillVariableKeyVariableValue
  // This function randomly generate variable length KV pairs,
  // whose key size is (0, max_key_size) and
  // whose value size is (0, max_value_size)
  // Also it supports batch option as is benchmark_write function
  void
  Benchmark::benchmark_fillVariableKeyValueLength(
      int num_entries,
      int max_key_size,
      int max_value_size,
      int entries_per_batch)
  {
    /*
      variable key length and variable value length fill benchmark
    */
    if (num_entries != num_)
    {
      char *msg = static_cast<char *>(malloc(sizeof(char) * 100));
      snprintf(msg, 100, "(%d ops)", num_entries);
      message_ = msg;
    }

    char *err_msg = NULL;
    int status;

    sqlite3_stmt *replace_stmt, *begin_trans_stmt, *end_trans_stmt;
    const char *replace_str = "REPLACE INTO test (key, value) VALUES (?, ?)";
    const char *begin_trans_str = "BEGIN TRANSACTION";
    const char *end_trans_str = "END TRANSACTION";

    /* Preparing sqlite3 statements */
    status = sqlite3_prepare_v2(db_, replace_str, -1,
                                &replace_stmt, NULL);
    error_check(status);
    status = sqlite3_prepare_v2(db_, begin_trans_str, -1,
                                &begin_trans_stmt, NULL);
    error_check(status);
    status = sqlite3_prepare_v2(db_, end_trans_str, -1,
                                &end_trans_stmt, NULL);
    error_check(status);

    bool transaction = (entries_per_batch > 1);
    for (int i = 0; i < num_entries; i += entries_per_batch)
    {
      /* Begin write transaction */
      if (FLAGS_transaction && transaction)
      {
        status = sqlite3_step(begin_trans_stmt);
        step_error_check(status);
        status = sqlite3_reset(begin_trans_stmt);
        error_check(status);
      }

      /* Create and execute SQL statements */
      for (int j = 0; j < entries_per_batch; j++)
      {
        const int key_size = gen_.rand_next() % max_key_size;
        const int value_size = gen_.rand_next() % max_value_size;

        const char *key = gen_.rand_gen_generate(key_size);
        const char *value = gen_.rand_gen_generate(value_size);

        /* Bind KV values into replace_stmt */
        status = sqlite3_bind_blob(replace_stmt, 1, key, 16, SQLITE_STATIC);
        error_check(status);
        status = sqlite3_bind_blob(replace_stmt, 2, value,
                                   value_size, SQLITE_STATIC);
        error_check(status);

        /* Execute replace_stmt */
        bytes_ += value_size + strlen(key);
        status = sqlite3_step(replace_stmt);
        step_error_check(status);

        /* Reset SQLite statement for another use */
        status = sqlite3_clear_bindings(replace_stmt);
        error_check(status);
        status = sqlite3_reset(replace_stmt);
        error_check(status);

        free((void *)key);
        free((void *)value);
        finished_single_op();
      }

      /* End write transaction */
      if (FLAGS_transaction && transaction)
      {
        status = sqlite3_step(end_trans_stmt);
        step_error_check(status);
        status = sqlite3_reset(end_trans_stmt);
        error_check(status);
      }
    }

    status = sqlite3_finalize(replace_stmt);
    error_check(status);
    status = sqlite3_finalize(begin_trans_stmt);
    error_check(status);
    status = sqlite3_finalize(end_trans_stmt);
    error_check(status);
  }

}; // namespace sqliteBench
