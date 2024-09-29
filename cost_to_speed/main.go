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

    jg.CreateTables()
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

    for i := 1; i <=10; i++ {
      q:= jg.GenerateQuery()
      micros, cost, js := dbuser.RunTestAnalyzeQuery("analyze format=json " + q)
      fmt.Printf("Q%d,  %d,  %f\n", i, micros, cost)

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


