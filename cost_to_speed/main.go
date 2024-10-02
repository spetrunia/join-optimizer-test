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
    rand.Seed(12664)
    fmt.Println("# Hello, World!")

    table_sizes := []int{ 10, 100, 1000, 5000, 10000, 15000, 20000}
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
    qplans, err := os.Create("analyze_outputs.json")
    if err != nil {
      log.Fatal(err)
    }
    out_csv, err := os.Create("query_times.csv")
    if err != nil {
      log.Fatal(err)
    }

    fmt.Fprintf(out_csv, "Query, Cost, Query_time_ms\n")
    fmt.Printf(          "Query, Cost, Query_time_ms\n")
    for i := 1; i <= N_QUERIES; i++ {
      q:= jg.GenerateQuery()
      cost, micros, js := dbuser.RunTestAnalyzeQuery("analyze format=json " + q)
      fmt.Fprintf(out_csv, "Q%f,  %f,  %f\n", i, cost, micros/1000.0)
      fmt.Printf(          "Q%f,  %f,  %f\n", i, cost, micros)

      fmt.Fprintf(qlog, "# Q%d : %d %f\nanalyze format=json\n%s\n", i, micros, cost, q);
      fmt.Fprintf(qplans, "# Q%d : %d %f\nanalyze format=json\n%s\n%s", i, micros, cost, q, js);
    }


    fmt.Println("# Queries written to " + filename)
    err = qlog.Close()
    if err != nil {
      log.Fatal(err)
    }
    err = qplans.Close()
    if err != nil {
      log.Fatal(err)
    }
    dbuser.Close()
}


