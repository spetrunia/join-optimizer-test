/*
  go get -u github.com/go-sql-driver/mysql
*/
package main
import (
    "fmt"
     "joinopttest/dbuser"
)


func main() {

    dbuser.Setup("root@tcp(127.0.0.1:3319)/test",
                 "root@tcp(127.0.0.1:3319)/test")
    dbuser.Connect()
    query:= "analyze format=json select avg(seq) from seq_1_to_10000"
    ns, _ := dbuser.RunTestQuery(query)
    fmt.Printf("Query %s, time=%d microsec\n", query, ns/1000)

    ns, cost, js := dbuser.RunTestAnalyzeQuery(query)
    fmt.Printf("Query %s, time=%d microsec, cost=%f\n", query, ns/1000, cost)
    fmt.Printf("analyze=%s\n", js)

    fmt.Println("# Done")
    dbuser.Close()
}
