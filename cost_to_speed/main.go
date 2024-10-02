// Test for joingen

package main

import (
    "math/rand"
    "fmt"
    "log"
    "os"
    jg "joinopttest/joingen"
    "joinopttest/dbuser"
)

func main() {
    rand.Seed(123)
    fmt.Println("# Hello, World!")

    table_sizes := []int{ 10, 100, 1000, 5000, 10000, 15000, 20000, 40000, 80000}
    N_QUERIES := 100

    jg.CreateTables(table_sizes)
    // Create domains
    jg.CreateJoinCols()
    jg.CollectTableStatistics()
    jg.DumpDatabaseGraph()
    jg.DumpFillCommands()

    dbuser.Setup("root@tcp(127.0.0.1:3319)/test",
                 "root@tcp(127.0.0.1:3319)/test")
    dbuser.Connect()

    fill_commands := jg.GetFillCommands()
    fmt.Println("# Filling database")
    for _, sql := range fill_commands {
      dbuser.RunQuery(sql)
    }
    fmt.Println("# Done filling the database")

    /*
      Generate and run queries
    */
    filename :="queries.sql"
    qlog, err := os.Create(filename)
    if err != nil {
      log.Fatal(err)
    }

    analyze_filename := "analyze_outputs.json";
    qplans, err := os.Create(analyze_filename)
    if err != nil {
      log.Fatal(err)
    }
    qtimes_filename := "query_times.csv"
    out_csv, err := os.Create(qtimes_filename)
    if err != nil {
      log.Fatal(err)
    }

    fmt.Fprintf(out_csv, "Query, Cost, Query_time_ms\n")
    fmt.Printf(          "Query, Cost, Query_time_ms\n")
    for i := 1; i <= N_QUERIES; i++ {
      q:= jg.GenerateQuery()
      cost, ms, js := dbuser.RunTestAnalyzeQuery("analyze format=json " + q)
      fmt.Fprintf(out_csv, "Q%d,  %f,  %f\n", i, cost, ms)
      fmt.Printf(          "Q%d,  %f,  %f\n", i, cost, ms)

      fmt.Fprintf(qlog,   "# Q%d : cost=%f time=%f\nanalyze format=json\n%s\n",   i, cost, ms, q);
      fmt.Fprintf(qplans, "# Q%d : cost=%f time=%f\nanalyze format=json\n%s\n%s\n\n", i, cost, ms, q, js);
    }


    fmt.Println("# Queries: " + filename)
    fmt.Println("# Query times: " + qtimes_filename)
    fmt.Println("# ANALYZE outputs: " + analyze_filename)
    err = qlog.Close()
    if err != nil {
      log.Fatal(err)
    }
    err = qplans.Close()
    if err != nil {
      log.Fatal(err)
    }
    out_csv.Close()
    dbuser.Close()
}


