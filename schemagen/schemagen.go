package schemagen

import (
	"os"
	"os/exec"

	//  "os/exec"
	"fmt"
	"log"
	"math/rand"
)

var db_fill_commands []string

func GetDbFillCommands() []string {
	return db_fill_commands
}

/*********************************************************************
*  Tables in the database and their data
*********************************************************************/
type Domain struct {
	min_val int
	max_val int
	name    string
}

type Column struct {
	//name string : use name()
	col_type *Domain
	/*
	   A list of table names. Each of the tables has a column with the same name
	   as this one.
	*/
	join_cols  []string
	create_sql string
	fill_sql   string
}

/*
A table in the database
*/
type Table struct {
	name string
	size int
	// Columns other than PK.
	fill_columns []FillColumn

	join_columns []Column
}

type FillColumn struct {
	name string
	size int
}

func (fc FillColumn) getDefinition() string {
	return fc.name + " varchar(" + fmt.Sprintf("%d", fc.size) + ")"
}

func (fc FillColumn) getRandomValue() string {
	return "repeat('X'," + fmt.Sprintf("%d", fc.size) + ")"
}

func (tbl Table) getCreateTable() string {
	s := "create or replace table " + tbl.name + "(\n"
	s += "  pk int primary key auto_increment"

	for _, fcol := range tbl.fill_columns {
		s += ",\n  "
		s += fcol.getDefinition()
	}
	s += "\n"
	s += ");\n"
	return s
}

func (col *Column) join_col_name() string {
	return col.col_type.name
}

func (col *Column) sql_definition() string {
	return "INT default NULL"
}

func (col *Column) sql_rand_value() string {
	return fmt.Sprintf("FLOOR(%d + RAND() * (%d - %d))",
		col.col_type.min_val, col.col_type.max_val, col.col_type.min_val)
}

func (col *Column) set_fill_sql_base(this_table *Table) {
	col.fill_sql =
		"update " + this_table.name + "\n" +
			" set " + col.col_type.name + " = " + col.sql_rand_value() +
			";\n"
	db_fill_commands = append(db_fill_commands, col.fill_sql)
}

func (col *Column) set_fill_sql_smaller(this_table *Table, prev_table string) {
	sql1 :=
		"create or replace table tmp as\n" +
			"select \n" +
			"  " + col.col_type.name + " value,\n" +
			"  row_number() over () as nr\n" +
			"from\n" +
			"  " + prev_table + "\n" +
			"order by rand() limit " + fmt.Sprintf("%d", this_table.size) + ";\n"

	sql2 :=
		"update tmp, " + this_table.name + " as t_next\n" +
			"set \n" +
			"  t_next." + col.col_type.name + "= tmp.value\n" +
			"where\n" +
			"  t_next.pk = tmp.nr;\n"
	sql3 := "drop table tmp;\n"

	db_fill_commands = append(db_fill_commands, sql1)
	db_fill_commands = append(db_fill_commands, sql2)
	db_fill_commands = append(db_fill_commands, sql3)
	col.fill_sql = sql1 + sql2 + sql3
}

func (tbl Table) getFillCommands() []string {
	var res []string
	sql := "insert into " + tbl.name + "\n" +
		" select NULL "

	for _, fcol := range tbl.fill_columns {
		sql += ", "
		sql += fcol.getRandomValue()
	}

	sql += "\nfrom seq_1_to_" + fmt.Sprintf("%d", tbl.size) + ";\n"
	res = append(res, sql)

	sql = "analyze table " + tbl.name + ";"
	res = append(res, sql)

	return res
}

/*********************************************************************/

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
	return tableByNumber[rand.Intn(len(tableByNumber))]
}

/*
Create a few tables
*/
func CreateTables() {
	TABLE_SIZES := []int{10, 100, 500, 1000, 10000, 20000, 40000}
	tableByName = make(map[string]*Table)
	for _, size := range TABLE_SIZES {
		tbl := Table{name: fmt.Sprintf("t%d", size), size: size}
		tableByName[tbl.name] = &tbl
		tableByNumber = append(tableByNumber, tbl.name)
		fmt.Println("# Created table ", tbl.name)

		// Add fill columns
		MAX_FILL_COL_SIZE := 200
		for i := 1; i < 20; i++ {
			if rand.Float64() < 0.1 {
				break
			}
			// Add a column...
			colsize := rand.Intn(MAX_FILL_COL_SIZE-1) + 1
			col := FillColumn{name: fmt.Sprintf("fill_col%d", i), size: colsize}
			tbl.fill_columns = append(tbl.fill_columns, col)
		}

		db_fill_commands = append(db_fill_commands, tbl.getCreateTable())
		db_fill_commands = append(db_fill_commands, tbl.getFillCommands()...)
	}
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

func DumpFillCommands() {
	filename := "fill.sql"
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	for _, sql := range db_fill_commands {
		fmt.Fprintf(f, "%s", sql)
	}
	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("# Wrote fill commands to %s\n", filename)
}

/*
Generate a cross-join query
*/
func GenerateCrossJoinQuery() string {

	//fmt.Println("# Generating a query\n")

	query := "select * from "

	empty := true
	n_tables := rand.Intn(len(tableByNumber)-1) + 1

	for i := 1; i <= n_tables; i++ {

		// Take a random table that's in the join already
		tbl_name := getRandomTable()

		if empty {
			empty = false
		} else {
			query = query + ", "
		}

		query = query + tbl_name + " as TBL" + fmt.Sprintf("%d", i)
	}
	//fmt.Printf("%s;\n", query)
	return query
}

func getRandomElementIndex(arr []string) int {
	return rand.Intn(len(arr))
}

func getRandomElement(arr []string) string {
	return arr[getRandomElementIndex(arr)]
}

func removeElement(arr []string, str string) []string {
	for idx, elem := range arr {
		if elem == str {
			return append(arr[:idx], arr[idx+1:]...)
		}
	}
	return arr
}

func checkElementExists(arr []string, str string) bool {
	for _, elem := range arr {
		if elem == str {
			return true
		}
	}
	return false
}

func GenerateQuery() string {

	// fmt.Println("# Generating a query")

	join_tables := make([]string, 0)
	query := "select * \nfrom\n"

	tbl0 := getRandomElement(tableByNumber)
	join_tables = append(join_tables, tbl0)
	query = query + "  " + tbl0 + "\n"
	// fmt.Printf("# First table %s\n", tbl0)

	//n_tables := 3
	n_tables := 1 + rand.Intn(len(tableByNumber)-1)
	for len(join_tables) < n_tables {
		// Take a random table that's in the join already
		old_tbl_name := getRandomElement(join_tables)
		old_tbl := tableByName[old_tbl_name]

		// Find the new tables it is connected to (we don't yet consider self-joins)
		join_candidates := make([]string, 0)
		join_cand_columns := make([]string, 0)
		for _, pcol := range old_tbl.join_columns {
			if !checkElementExists(join_tables, pcol.join_cols[0]) {
				join_candidates = append(join_candidates, pcol.join_cols[0])
				join_cand_columns = append(join_cand_columns, pcol.join_col_name())
			}
		}
		if len(join_candidates) == 0 {
			continue
		}

		// Ok got a new table
		i := getRandomElementIndex(join_candidates)
		new_tbl_name := join_candidates[i]
		join_col_name := join_cand_columns[i]
		query = query + fmt.Sprintf("  join %s on %s.%s=%s.%s\n", new_tbl_name,
			new_tbl_name, join_col_name,
			old_tbl_name, join_col_name)

		//not_used_tables= removeElement(not_used_tables, tbl)
		join_tables = append(join_tables, new_tbl_name)
		// fmt.Printf("# Adding table %s\n", new_tbl_name)
	}
	//fmt.Printf("%s\n", query)
	return query
}

func DumpDatabaseGraph() {
	filename := "database.dot"
	filename_pdf := "database.pdf"
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		return
	}

	// print the tables
	fmt.Fprintln(f, "graph {")
	for _, tbl := range tableByNumber {
		for _, col := range tableByName[tbl].join_columns {
			for _, join_table := range col.join_cols {
				//if (tbl >= join_table) {
				fmt.Fprintf(f, " %s -- %s;\n", tbl, join_table)
				//}
			}
		}
	}
	// print the connections
	fmt.Fprintln(f, "}")
	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("# Wrote database structure to %s\n", filename)
	cmd := exec.Command("dot", "-Tpdf", "-o"+filename_pdf, filename)
	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("# Dumped it to %s\n", filename_pdf)
}

func CreateJoinCols() {
	fmt.Println("# Constructing join graph")
	connected_set := make([]string, 0)
	disconnected_set := make([]string, 0)

	disconnected_set = append(disconnected_set, tableByNumber...)

	// Get the first element and put it into connected_set
	tbl0 := getRandomElement(disconnected_set)
	disconnected_set = removeElement(disconnected_set, tbl0)
	connected_set = append(connected_set, tbl0)
	fmt.Printf("# Adding table %s\n", tbl0)

	// Continue with joining a random table with random table
	for len(disconnected_set) > 0 {
		tbl := getRandomElement(disconnected_set)
		disconnected_set = removeElement(disconnected_set, tbl)
		join_tbl := getRandomElement(connected_set)

		// Create a pair of columns in both tables
		connected_set = append(connected_set, tbl)
		//printArray(connected_set)

		ptbl := tableByName[tbl]
		pjoin_tbl := tableByName[join_tbl]

		card := min(ptbl.size, pjoin_tbl.size)
		card = rand.Intn(card-3) + 2

		new_domain := createDomain(card)
		tbl_col := ptbl.addColumn(&new_domain, join_tbl)
		join_tbl_col := pjoin_tbl.addColumn(&new_domain, tbl)

		// Set SQL fill commands.
		if ptbl.size > pjoin_tbl.size {
			tbl_col.set_fill_sql_base(ptbl)
			join_tbl_col.set_fill_sql_smaller(pjoin_tbl, ptbl.name)
		} else {
			join_tbl_col.set_fill_sql_base(pjoin_tbl)
			tbl_col.set_fill_sql_smaller(ptbl, pjoin_tbl.name)
		}

		fmt.Printf("# Adding table %s ( <-> %s, %s)\n", tbl, join_tbl, new_domain.name)
	}
}

/*********************************************************************/
var domainNumber int

func createDomain(cardinality int) Domain {
	domainNumber++
	new_name := fmt.Sprintf("col%d", domainNumber)
	return Domain{min_val: 1, max_val: cardinality, name: new_name}
}

func (tbl Table) ddl() string {
	s := "create or replace table " + tbl.name + "(\n"
	s += "  pk int primary key auto_increment\n"
	s += ");\n"
	return s
}

func (tbl *Table) addColumn(dom *Domain, other_table string) *Column {
	new_col := Column{col_type: dom}
	new_col.join_cols = append(new_col.join_cols, other_table)
	new_col.create_sql = "alter table " + tbl.name +
		" add column " + new_col.join_col_name() +
		" " + new_col.sql_definition() + ";\n"
	db_fill_commands = append(db_fill_commands, new_col.create_sql)

	index_sql := "alter table " + tbl.name +
		" add index(" + new_col.join_col_name() + ");\n"
	db_fill_commands = append(db_fill_commands, index_sql)

	tbl.join_columns = append(tbl.join_columns, new_col)
	return &tbl.join_columns[len(tbl.join_columns)-1]
}
