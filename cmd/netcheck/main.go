/*
  go get -u github.com/go-sql-driver/mysql
*/
package main
import (
    "fmt"
     "speedcmp/dbuser"
)


func main() {

    dbuser.Setup("root@tcp(127.0.0.1:3319)/test",
                 "root@tcp(127.0.0.1:3319)/test")
    dbuser.Connect_servers()

    //dbuser.Perform_query("select avg(seq) from seq_1_to_10000")

    //defer insert.Close()
    fmt.Println("# Done")
    
}
