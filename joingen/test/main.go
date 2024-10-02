// Test for joingen

package main

import (
    "math/rand"
    "fmt"
    jg "joinopttest/joingen"
)

func main() {
    rand.Seed(12664)
    fmt.Println("# Hello, World!")

    table_sizes := []int{ 10, 100, 1000, 10000}
    jg.CreateTables(table_sizes)
    // Create domains
    jg.CreateJoinCols()
    jg.DumpDatabaseGraph()
    jg.DumpFillCommands()
    //for i := 1; i <=10; i++ {
    //  fmt.Printf(" rand=%d\n", rand.Intn(10))
    //}
    for i := 1; i <=10; i++ {
      fmt.Printf("Q: %s\n", jg.GenerateQuery())
    }
   // connect()
}


