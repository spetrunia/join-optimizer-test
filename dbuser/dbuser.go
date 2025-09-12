package dbuser

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/PaesslerAG/jsonpath"

	_ "github.com/go-sql-driver/mysql"
)

var N_ATTEMPTS = 3

var conn_str [2]string

var db [2]*sql.DB

func Setup(server1 string, server2 string) {
	conn_str[0] = server1
	conn_str[1] = server2
}

func Connect_servers() {
	for i := 0; i < 2; i++ {
		db1, err := sql.Open("mysql", conn_str[i])
		if err != nil {
			panic(err.Error())
		}
		db[i] = db1
		//defer db.Close()
		fmt.Printf("# Connected to server %d\n", i)
	}
}

func SetSettings(conn int, sql string) {
	_, err := db[conn].Exec(sql)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("# conn[%d]: %s\n", conn, sql)
	/*
	   var value int;
	   err := db[conn].QueryRow("select connection_id()").Scan(&value);

	   	if err != nil {
	   	    panic(err.Error())
	   	}

	   fmt.Printf("# connection_id()=%d\n", value);
	*/
}

/*
Fill the database.
  - Do not run the same fill commands on the same database
  - TODO: do not fill the database if it's already filled
    (cache it)
*/
func RunFillCommands(commands []string) {
	n_servers := 2
	fmt.Println("# Running fill commands")
	if conn_str[0] == conn_str[1] {
		fmt.Println("# Just once as we have one server")
		n_servers = 1
	}

	for i := 0; i < n_servers; i++ {
		start_time := time.Now()
		// Feed the fill commands to the server
		for _, sql := range commands {
			fmt.Printf("\n%s\n", sql)
			//_, err := db[i].Query(sql)
			_, err := db[i].Exec(sql)
			if err != nil {
				panic(err.Error())
			}
		}
		end_time := time.Now()
		fmt.Printf("# Load Time: ")
		fmt.Println(end_time.Sub(start_time))
	}
}

func Perform_query(query string) {
	var mintime int64
	mintime = 0

	for i := 0; i < N_ATTEMPTS; i++ {
		a := time.Now()
		_, err := db[i].Query(query)
		b := time.Now()
		if err != nil {
			panic(err.Error())
		}
		ns := (b.Sub(a)).Nanoseconds()
		fmt.Println(b.Sub(a))
		if ns < mintime {
			mintime = ns
		}
	}
}

func Close_connections() {
	for i := 0; i < 2; i++ {
		if db[i] != nil {
			db[i].Close()
		}
	}
}

func GetExplainCosts(query string) (float64, float64) {
	var cost [2]float64
	var ok bool
	for i := 0; i < 2; i++ {
		row := db[i].QueryRow("explain format=json " + query)
		var json_str string
		err := row.Scan(&json_str)
		if err != nil {
			log.Fatal(err)
		}

		v := interface{}(nil)
		json.Unmarshal([]byte(json_str), &v)

		res, err := jsonpath.Get("$.query_block.cost", v)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		cost[i], ok = res.(float64)
		if !ok {
			fmt.Printf("Unexpected type for cost: %T\n", res)
			os.Exit(1)
		}
	}
	return cost[0], cost[1]
}

// TODO: do we need to accumulate/print report?
