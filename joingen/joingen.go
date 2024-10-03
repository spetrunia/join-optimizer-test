package joingen

import (
    "database/sql"
    "fmt"
    "os"
    "os/exec"
    "log"
//    "math/rand/v2"
    "math/rand"
)

var db *sql.DB
var db_fill_commands []string

var verbose= false
/*********************************************************************/

type Domain struct {
  min_val int
  max_val int
  name string
}

type Column struct {
  //name string : use name()
  col_type *Domain
  /* 
    A list of table names. Each of the tables has a column with the same name
    as this one.
  */
  join_cols []string
  create_sql string
  fill_sql string
}

/*
  A table in the database
*/
type Table struct {
  name string
  size int
  // Columns other than PK.
  columns []Column
}


func (this *Column) name() string {
  return this.col_type.name
}

func (this *Column) sql_definition() string {
  return "INT default NULL"
}

func (this *Column) sql_rand_value() string {
  return fmt.Sprintf("FLOOR(%d + RAND() * (%d - %d))",
                     this.col_type.min_val, this.col_type.max_val, this.col_type.min_val);
}

/*
  Fill the table column with random values from this domain.

  @note
    We just pick random value each time so we have no idea about n_distinct we will get!

*/
func (this *Column) set_fill_sql_base(this_table *Table) {
  this.fill_sql=
    "update " + this_table.name + "\n" +
    " set " + this.col_type.name + " = " + this.sql_rand_value() +
    ";\n"
  db_fill_commands= append(db_fill_commands, this.fill_sql)
}


/*
  Fill the column with subset of values from big_table.

  N_DISTINCT(this) will be lower than N_DISTINCT(big_table.column).
  
  We don't control how much lower.
*/

func (this *Column) set_fill_sql_smaller(this_table *Table, big_table string) {
  sql1 :=
    "create temporary table tmp as\n" +
    "select \n" +
    "  " + this.col_type.name + " value,\n" +
    "  row_number() over () as nr\n" +
    "from\n" +
    "  " + big_table + "\n" +
    "order by rand() limit " + fmt.Sprintf("%d", this_table.size) + ";\n"

  sql2:=
   "update tmp, " + this_table.name + " as t_next\n" +
   "set \n" +
   "  t_next." + this.col_type.name + "= tmp.value\n" +
   "where\n" +
   "  t_next.pk = tmp.nr;\n" +
   "drop table tmp;\n"
  db_fill_commands= append(db_fill_commands, sql1)
  db_fill_commands= append(db_fill_commands, sql2)
  this.fill_sql = sql1 + sql2
}

/*********************************************************************/
var domainNumber int

func createDomain(target_size int) Domain {
  domainNumber++
  new_name:= fmt.Sprintf("col%d", domainNumber)
  return Domain{min_val: 1, max_val: rand.Intn(target_size), name: new_name}
}

func (tbl Table) ddl() string {
  s:= "create table " + tbl.name + "(\n"
  s+= "  pk int primary key auto_increment\n"
  s+= ");\n"
  return s
}

func (tbl *Table) addColumn(dom *Domain, other_table string) *Column {
  new_col := Column{col_type: dom}
  new_col.join_cols = append(new_col.join_cols, other_table)
  new_col.create_sql= "alter table " + tbl.name +
                      " add column " + new_col.name() +
                      " " + new_col.sql_definition() + ";\n"
  db_fill_commands= append(db_fill_commands, new_col.create_sql)

  index_sql:= "alter table " + tbl.name +
              " add index(" + new_col.name() + ");\n"
  db_fill_commands= append(db_fill_commands, index_sql)

  tbl.columns = append(tbl.columns, new_col)
  return &tbl.columns[len(tbl.columns) - 1]
}

/*
  Maps table_name -> Table object.
*/
var tableByName map[string]*Table

/*
  Maps table Number -> string.
  Numbering doesn't mean anything.
*/
var tableByNumber []string

func getRandomTable() string {
  return tableByNumber[rand.Intn(len(tableByNumber))];
}



/*
  Create a few tables with only PK.
*/
func CreateTables(table_sizes []int) {
  tableByName = make(map[string]*Table)
  //TABLE_SIZES := []int{ 10, 100, 1000, 10000}
  for _, size := range table_sizes {
    tbl := Table{ name: fmt.Sprintf("t%d", size), size: size }
    tableByName[tbl.name]= &tbl
    tableByNumber= append(tableByNumber, tbl.name)
    fmt.Println("# Created table ", tbl.name)
    //fmt.Println(tbl.ddl());
    db_fill_commands= append(db_fill_commands, "drop table if exists " + tbl.name + ";\n")
    db_fill_commands= append(db_fill_commands, tbl.ddl())
    sql:= "insert into " + tbl.name + "(pk)" +
            " select NULL from seq_1_to_" + fmt.Sprintf("%d", size) + ";\n";
    db_fill_commands= append(db_fill_commands, sql)
  }
}


func getRandomElementIndex(arr []string) int {
  return rand.Intn(len(arr));
}

func getRandomElement(arr []string) string {
  return arr[getRandomElementIndex(arr)];
}

func removeElement(arr []string, str string) []string {
   for idx, elem := range arr {
     if (elem == str ) {
       return append(arr[:idx], arr[idx+1:]...);
     }
   }
   return arr
}

func checkElementExists(arr []string, str string) bool {
   for _, elem := range arr {
     if (elem == str ) {
       return true
     }
   }
   return false
}

func printArray(arr []string) {
  fmt.Print("{")
  for _, elem := range arr {
    fmt.Printf("%s ", elem)
  }
  fmt.Print("}")
}

/*
  Create pairs of join columns.
   We need to build a connected graph.
   The build procedure is:
     connected_set = ();
     while () {
       t1 = <pick a random table not in connected_set>;
       t2 = <pick a random table in connected_set>;
       new_tables= {t1, t2};
       while () {
         if (rand() > 1/ n^2) {
           tN=<pick a random table in connected_set>
           new_tables.add(tN);
       }
       // create a set of columns in new_tables and fill them with agreed domain.
     }
*/

func CreateJoinCols() {
  fmt.Println("# Constructing join graph")
  connected_set := make([]string, 0);
  disconnected_set := make([]string, 0);

  for _, tbl := range tableByNumber {
    disconnected_set= append(disconnected_set, tbl)
  }

  // Get the first element and put it into connected_set
  tbl0 := getRandomElement(disconnected_set)
  disconnected_set= removeElement(disconnected_set, tbl0)
  connected_set= append(connected_set, tbl0)
  fmt.Printf("# Adding table %s\n", tbl0)

  // Continue with joining a random table with random table
  for len(disconnected_set) > 0 {
    tbl:= getRandomElement(disconnected_set)
    disconnected_set= removeElement(disconnected_set, tbl)
    join_tbl := getRandomElement(connected_set)

    // Create a pair of columns in both tables
    connected_set= append(connected_set, tbl)
    //printArray(connected_set)


    ptbl := tableByName[tbl];
    pjoin_tbl := tableByName[join_tbl];

    max_size:= ptbl.size
    if max_size < pjoin_tbl.size {
      max_size= pjoin_tbl.size
    }

    new_domain := createDomain(max_size)
    tbl_col := ptbl.addColumn(&new_domain, join_tbl)
    join_tbl_col := pjoin_tbl.addColumn(&new_domain, tbl)

    // Set SQL fill commands.
    if (ptbl.size > pjoin_tbl.size) {
      tbl_col.set_fill_sql_base(ptbl);
      join_tbl_col.set_fill_sql_smaller(pjoin_tbl, ptbl.name)
    } else {
      join_tbl_col.set_fill_sql_base(pjoin_tbl);
      tbl_col.set_fill_sql_smaller(ptbl, pjoin_tbl.name)
    }

    fmt.Printf("# Adding table %s ( <-> %s, %s)\n", tbl, join_tbl, new_domain.name)
  }
}

func CollectTableStatistics() {
  for _, tbl := range tableByNumber {
    db_fill_commands= append(db_fill_commands, "analyze table " + tbl + ";\n")
  }
}

func DumpDatabaseGraph() {
  filename :="database.dot"
  filename_pdf :="database.pdf"
  f, err := os.Create(filename)
  if err != nil {
    fmt.Println(err)
    return
  }

  // print the tables 
  fmt.Fprintln(f, "graph {");
  for _, tbl := range tableByNumber {
    for _, col := range tableByName[tbl].columns {
      for _, join_table := range col.join_cols {
        //if (tbl >= join_table) {
          fmt.Fprintf(f, " %s -- %s;\n", tbl, join_table);
        //}
      }
    }
  }
  // print the connections
  fmt.Fprintln(f, "}");
  err = f.Close()
  if err != nil {
    fmt.Println(err)
    return
  }
  fmt.Printf("# Wrote database structure to %s\n", filename);
  cmd := exec.Command("dot", "-Tpdf", "-o" + filename_pdf,  filename);
  if err := cmd.Run(); err != nil {
    log.Fatal(err)
  }
  fmt.Printf("# Dumped it to %s\n", filename_pdf);
}

func DumpFillCommands() {
  filename :="fill.sql"
  f, err := os.Create(filename)
  if err != nil {
    log.Fatal(err)
  }
  for _, sql := range db_fill_commands {
    fmt.Fprintf(f, "%s", sql);
  }
  err = f.Close()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Printf("# Wrote fill commands to %s\n", filename);
}

func GetFillCommands() []string {
  return db_fill_commands;
}


/*
   Generating a query: 
   tables.
*/
func GenerateQuery() string {
  if (verbose) { fmt.Println("# Generating a query\n") }

  join_tables := make([]string, 0);
  query := "select * \nfrom\n";

  tbl0 := getRandomElement(tableByNumber)
  join_tables= append(join_tables, tbl0)
  query= query + "  " + tbl0 + "\n"
  //fmt.Printf("# First table %s\n", tbl0)

  //n_tables := 3 //rand.Intn(len(tableByNumber))
  n_tables := 2 + rand.Intn(min(len(tableByNumber) - 2, 6))
  for len(join_tables) < n_tables {
    // Take a random table that's in the join already
    old_tbl_name:= getRandomElement(join_tables)
    old_tbl := tableByName[old_tbl_name]

    // Find the new tables it is connected to (we don't yet consider self-joins)
    join_candidates := make([]string, 0)
    join_cand_columns := make([]string ,0)
    for _, pcol := range(old_tbl.columns) {
      if !checkElementExists(join_tables, pcol.join_cols[0]) {
        join_candidates= append(join_candidates, pcol.join_cols[0])
        join_cand_columns = append(join_cand_columns, pcol.name())
      }
    }
    if (len(join_candidates) == 0) {
      continue
    }

    // Ok got a new table
    i := getRandomElementIndex(join_candidates)
    new_tbl_name:= join_candidates[i]
    join_col_name:= join_cand_columns[i]
    query = query + fmt.Sprintf("  join %s on %s.%s=%s.%s\n", new_tbl_name,
                                new_tbl_name, join_col_name,
                                old_tbl_name, join_col_name)

    //not_used_tables= removeElement(not_used_tables, tbl)
    join_tables= append(join_tables, new_tbl_name)
    if (verbose) { fmt.Printf("# Adding table %s\n", new_tbl_name) }
  }
  if (verbose) { fmt.Printf("%s\n", query); }
  return query;
}

