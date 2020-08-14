package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
)

// Date struct
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

func (d *Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

func graphiteIndexFilter(pathGlobs []string, date *Date) string {
	var pathFilter strings.Builder
	appended := false
	if len(pathGlobs) > 0 || date != nil {
		pathFilter.WriteString("WHERE ")
	} else {
		return ""
	}
	if len(pathGlobs) > 0 {
		appended = true
		pathFilter.WriteString("(")
		for i := range pathGlobs {
			if i > 0 {
				pathFilter.WriteString(" OR ")
			}
			pathFilter.WriteString(fmt.Sprintf("Path like '%s'", pathGlobs[i]))
		}
		pathFilter.WriteString(")")
	}
	if date != nil {
		if appended {
			pathFilter.WriteString(" AND ")
		}
		pathFilter.WriteString(fmt.Sprintf("Date='%s'", date.String()))
	}
	return pathFilter.String()
}

func graphiteIndexDateQuery(graphiteIndex string, pathGlobs []string, dateFilter string) string {
	var dates string
	if len(dateFilter) > 0 {
		dates = " AND (" + dateFilter + ") "
	}
	return fmt.Sprintf("SELECT Date FROM %s %s%s GROUP BY Date", graphiteIndex, graphiteIndexFilter(pathGlobs, nil), dates)
}

func graphiteIndexPathQuery(graphiteIndex string, pathGlobs []string, dateFilter string) string {
	var dates string
	if len(dateFilter) > 0 {
		dates = " AND (" + dateFilter + ") "
	}
	return fmt.Sprintf("SELECT Path FROM %s %s%s GROUP BY Path", graphiteIndex, graphiteIndexFilter(pathGlobs, nil), dates)
}

func graphiteIndexDeleteQuery(graphiteIndex string, pathGlobs []string, date *Date) string {
	return fmt.Sprintf("ALTER TABLE %s  DELETE %s", graphiteIndex, graphiteIndexFilter(pathGlobs, date))
}

func graphiteIndexList(con *sql.DB, graphiteIndex string, pathGlobs []string, dateFilter string) ([]Date, []string, error) {
	dates := make([]Date, 0, 2)
	paths := make([]string, 0, 100)

	if len(pathGlobs) == 0 {
		return dates, paths, fmt.Errorf("empthy path glob list")
	}

	query := graphiteIndexDateQuery(graphiteIndex, pathGlobs, dateFilter)
	rows, err := con.Query(query)
	if err != nil {
		return dates, paths, err
	}
	for rows.Next() {
		var date time.Time
		if err := rows.Scan(&date); err != nil {
			rows.Close()
			return dates, paths, nil
		}
		dates = append(dates, Date{Year: date.Year(), Month: date.Month(), Day: date.Day()})
	}
	rows.Close()

	query = graphiteIndexPathQuery(graphiteIndex, pathGlobs, dateFilter)
	rows, err = con.Query(query)
	if err != nil {
		return dates, paths, err
	}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			rows.Close()
			return dates, paths, nil
		}
		paths = append(paths, path)
	}
	rows.Close()

	return dates, paths, nil
}

type ClickhouseMutation struct {
	ID         string    `db:"mutation_id"`
	CreateTime time.Time `db:"create_time"`
	PartsToDo  int64     `db:"parts_to_do"`
	Done       uint8     `db:"is_done"`
	Command    string    `db:"command"`
}

func clickhouseMutations(con *sql.DB, database string, table string, isDone uint8) ([]ClickhouseMutation, error) {
	var items []ClickhouseMutation

	query := fmt.Sprintf("select mutation_id, create_time, parts_to_do, is_done, command from system.mutations where is_done=%d and database='%s' and table='%s'",
		isDone, database, table)
	rows, err := con.Query(query)
	if err != nil {
		return items, err
	}
	defer rows.Close()
	for rows.Next() {
		var mutation ClickhouseMutation
		if err := rows.Scan(&mutation.ID, &mutation.CreateTime, &mutation.PartsToDo,
			&mutation.Done, &mutation.Command); err != nil {
			return items, err
		}
		items = append(items, mutation)
	}
	return items, err
}

func reversePath(path string) string {
	a := strings.Split(path, ".")

	l := len(a)
	for i := 0; i < l/2; i++ {
		a[i], a[l-i-1] = a[l-i-1], a[i]
	}

	return strings.Join(a, ".")
}

func readGlobs(path string) ([]string, []string, error) {
	var globs = make([]string, 0, 10)
	var globsWithReverse = make([]string, 0, 20)
	if len(path) == 0 {
		return globs, globsWithReverse, fmt.Errorf("glob file path not set")
	}

	file, err := os.Open(path)
	if err != nil {
		return globs, globsWithReverse, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		glob := strings.Trim(scanner.Text(), " ")
		if len(glob) == 0 {
			continue
		} else if glob == "%" || glob == "?" {
			return nil, globs, fmt.Errorf("check glob in %s", glob)
		} else if strings.IndexAny(glob, "~!@#$^&*() '\"") != -1 {
			return globs, globsWithReverse, fmt.Errorf("invalid symbol in glob %s", glob)
		}
		globs = append(globs, glob)
		globsWithReverse = append(globsWithReverse, glob)
		globsWithReverse = append(globsWithReverse, reversePath(glob))
	}
	if len(globs) == 0 {
		return globs, globsWithReverse, fmt.Errorf("empthy glob list")
	}
	return globs, globsWithReverse, nil
}

func main() {
	var globsFile = flag.String("globs", "", "graphite index globs file")
	var index = flag.String("index", "graphite_index", "graphite index table")
	var query = flag.Bool("query", false, "show graphite index table query (don't execute)")
	var reverse = flag.Bool("reverse", false, "Add reverse paths to cleanup check (if forward paths already deleted)")
	var delete = flag.Bool("delete", false, "run delete commands")
	var showPath = flag.Bool("show", false, "print paths before run delete command")
	var askDelete = flag.Bool("ask", false, "ask before run delete commands")
	var maxMerges = flag.Int("merges", 1, "allowed merges")
	var dateFilter = flag.String("dates", "", "restrict dates with like Date > '2020-02-01' AND Date < '2020-03-01'")
	flag.Parse()

	if len(*dateFilter) > 0 {
		values := strings.Split(*dateFilter, " ")
		if len(values) < 3 {
			log.Fatalf("Invalid date filter")
		}
		i := 0
		pos := 0
		for {
			if i == len(values) {
				break
			}
			switch pos {
			case 0:
				if values[i] != "Date" {
					log.Fatalf("Invalid date filter: use Date instead of %s", values[i])
				}
				pos++
			case 1:
				if values[i] != ">" && values[i] != ">=" && values[i] != "<" && values[i] != "<=" &&
					values[i] != "=" && values[i] != "!=" {
					log.Fatalf("Invalid date filter: use correct comparator instead of %s", values[i])
				}
				pos++
			case 2:
				matched, err := regexp.Match("^'[0-9]{4}-[0-9]{2}-[0-9]{2}'$", []byte(values[i]))
				if err != nil || !matched {
					log.Fatalf("Invalid date filter: use correct date instead of %s", values[i])
				}
				pos++
			case 3:
				if values[i] != "AND" {
					log.Fatalf("Invalid date filter: use AND instead of %s", values[i])
				}
				if i == len(values)-1 {
					log.Fatalf("Invalid date filter: can't use %s at the and", values[i])
				}
				pos = 0
			}
			i++
		}
	}

	if len(*index) > 0 {
		globs, globsWithReverse, err := readGlobs(*globsFile)
		if err != nil {
			log.Fatal(err)
		}

		if *query {
			if *reverse {
				fmt.Printf("%s\n", graphiteIndexPathQuery(*index, globsWithReverse, *dateFilter))
			} else {
				fmt.Printf("%s\n", graphiteIndexPathQuery(*index, globs, *dateFilter))
			}
		} else {
			con, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000")
			if err != nil {
				log.Fatal(err)
			}

			var dates []Date
			var paths []string
			fmt.Println("Check index")
			if *reverse {
				dates, paths, err = graphiteIndexList(con, *index, globsWithReverse, *dateFilter)
			} else {
				dates, paths, err = graphiteIndexList(con, *index, globs, *dateFilter)
			}
			if err != nil {
				log.Fatal(err)
			}
			if *showPath {
				for _, path := range paths {
					fmt.Printf("%s\n", path)
				}
			}
			if len(paths) == 0 {
				log.Fatalln("No path found")
			} else {
				fmt.Printf("Read %d paths in %d days:", len(paths), len(dates))
				i := 0
				if dates[0].Year == 1970 && dates[0].Month == 02 && dates[0].Day == 12 {
					fmt.Printf(" %s", dates[0].String())
					i++
				}
				if i < len(dates) {
					fmt.Printf(" %s", dates[i].String())
				}
				if i != len(dates)-1 {
					fmt.Printf(" - %s", dates[len(dates)-1].String())
				}
				fmt.Println("")
				if *askDelete {
					sc := bufio.NewScanner(os.Stdin)
					fmt.Print("Enter Y for start delete: ")
					if !sc.Scan() || sc.Text() != "Y" {
						fmt.Fprintln(os.Stderr, "Canceled")
						os.Exit(1)
					}
					*delete = true
				}
				for i := range dates {
					query := graphiteIndexDeleteQuery(*index, globsWithReverse, &dates[i])
					if *delete {
						for {
							wait := 10
							merges, err := clickhouseMutations(con, "default", *index, 0)
							if err != nil {
								log.Fatal(err)
							}
							n := 0
							for _, merge := range merges {
								if merge.Done == 0 {
									n++
								}
							}
							if n >= *maxMerges {
								fmt.Printf("\nWait for merges complete before delete %s (%d of %d)\n",
									dates[i].String(), i, len(dates))
								for _, merge := range merges {
									time := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
										merge.CreateTime.Year(), merge.CreateTime.Month(), merge.CreateTime.Day(),
										merge.CreateTime.Hour(), merge.CreateTime.Minute(), merge.CreateTime.Second())
									s := strings.LastIndex(merge.Command, "Date = '")
									var mergeDate string
									if s > 0 {
										e := strings.IndexByte(merge.Command[s+8:], '\'')
										if e >= 0 {
											mergeDate = merge.Command[s : s+8+e+1]
										}
									}
									fmt.Printf("%-10s  %s %s parts %d\n", merge.ID, time, mergeDate, merge.PartsToDo)
								}
								// wait for merges complete
								time.Sleep(time.Duration(wait) * time.Second)
								if wait < 60 {
									wait += 10
								}
							} else {
								_, err := con.Exec(query)
								if err != nil {
									log.Fatal(err)
								}

								if i < len(dates)-1 {
									time.Sleep(1 * time.Second)
								}
								break
							}
						}
					} else {
						fmt.Printf("%s\n\n", query)
					}
				}
			}
		}
	}
}
