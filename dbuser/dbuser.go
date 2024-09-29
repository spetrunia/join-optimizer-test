
package dbuser

import (
    "fmt"
    "math"
    "time"
    "log"
    "database/sql"
  _  "github.com/go-sql-driver/mysql"
    "github.com/buger/jsonparser"
)

var N_ATTEMPTS=3;

var conn_str [2]string;
var init_str [2]string;
var db [2]*sql.DB

func Setup(server1 string, server2 string) {
   conn_str[0]= server1
   conn_str[1]= server2
}

func SetupInitString(server1 string, server2 string) {
   init_str[0]= server1
   init_str[1]= server2
}

func Connect() {
  for i:= 0; i < 2; i++ {
    db1, err := sql.Open("mysql", conn_str[i] + "?multiStatements=true")
    if err != nil {
        panic(err.Error())
    }
    db[i]= db1;
    //defer db.Close()
    fmt.Printf("# Connected to server %d\n", i)
  }
}

func Close() {
  for i:= 0; i < 2; i++ {
    if (db[i] != nil) {
      db[i].Close()
    }
  }
}

/* 
  Run a query once, without retries. 
  In one-server mode, run once, otherwise run on each server.
*/
func RunQuery(query string) {
  _, err := db[0].Exec(query)
  if err !=nil {
    panic("Query: " + query + ": error: " + err.Error())
  }
}

/*
  This runs a test query.
  The query is run multiple times and the best one is taken.
  
  @return
    (query_speed, query_result)
*/
func RunTestQuery(query string) (int64, *sql.Rows) {
  var mintime int64
  var best_rows *sql.Rows
  mintime= math.MaxInt64

  for i:=0; i < N_ATTEMPTS; i++ {
    a := time.Now()
    rows, err := db[0].Query(query)
    b := time.Now()
    if err !=nil {
      panic(err.Error())
    }
    ns:= (b.Sub(a)).Nanoseconds()
    //fmt.Println(b.Sub(a))
    if (ns < mintime) {
      mintime= ns
      best_rows= rows
    } else {
      rows.Close()
    }
  }
  return mintime/1000, best_rows
}

/*
 go get -u github.com/buger/jsonparser
*/
func RunTestAnalyzeQuery(query string) (int64, float64, string) {

  min_time, best_rows := RunTestQuery(query)

  if !best_rows.Next() {
    if err:= best_rows.Err(); err!=nil {
      log.Fatal(err);
    } else {
      log.Fatal("RunTestQuery got empty result set")
    }
  }

  var value []byte
  if err := best_rows.Scan(&value); err != nil {
    log.Fatal(err)
  }
  // Parse the JSON
  cost, err :=jsonparser.GetFloat(value, "query_block", "cost")
  if (err != nil) {
    log.Fatal(err);
  }
  return min_time, cost, string(value);
}


