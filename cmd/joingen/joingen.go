package main

import (
	//    "database/sql"
	"fmt"
	"log"
	"os"

	//    "log"
	//    "math/rand/v2"
	"math/rand"
	"speedcmp/dbuser"
	sg "speedcmp/schemagen"
)

func main() {
	rand.Seed(12664)
	fmt.Println("# Hello, World!")

	dbuser.Setup("root@tcp(127.0.0.1:3319)/test",
		"root@tcp(127.0.0.1:3319)/test")
	dbuser.Connect_servers()
	//dbuser.SetSettings(1, "set optimizer_prune_level=0")
	dbuser.SetSettings(1, "set optimizer_search_depth=1")

	sg.CreateTables()
	// Create domains
	sg.CreateJoinCols()
	sg.DumpDatabaseGraph()
	sg.DumpFillCommands()
	dbuser.RunFillCommands(sg.GetDbFillCommands())

	query_log, err := os.Create("queries.txt")
	if err != nil {
		log.Fatal(err)
	}
	run_log, err := os.Create("run.txt")
	if err != nil {
		log.Fatal(err)
	}

	COUNT := 50
	diff1 := 0
	diff2 := 0
	for i := 1; i <= COUNT; i++ {
		//fmt.Println("#")
		query := sg.GenerateQuery()
		fmt.Printf("# query%d\n%s\n", i, query)
		fmt.Fprintf(query_log, "# %s\n", query)

		cost1, cost2 := dbuser.GetExplainCosts(query)
		fmt.Fprintf(run_log,
			"query%d %g %g %.5g\n", i, cost1, cost2, cost1*100/cost2)
		fmt.Printf("query%d %g %g %.5g\n", i, cost1, cost2, cost1*100/cost2)
		if cost1-cost2 > 1e-5 {
			diff1 += 1
		}
		if cost2-cost1 > 1e-5 {
			diff2 += 1
		}
	}
	fmt.Printf("TOTAL:   %d\n", COUNT)
	fmt.Printf("smaller: %d\n", diff1)
	fmt.Printf("bigger:  %d\n", diff2)

	dbuser.Close_connections()
}
