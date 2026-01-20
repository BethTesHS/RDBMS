package engine

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

type Repl struct {
	Db *Database
}

func (r *Repl) Start() {
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("\n=================================")
		fmt.Println("   SQLly REPL READY (Go Version)")
		fmt.Println(`   Try: INSERT INTO users VALUES (1, "Admin", 99)`)
		fmt.Println("=================================\n")

		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("SQLly> ")

		for scanner.Scan() {
			input := scanner.Text()
			if strings.TrimSpace(input) == "" {
				fmt.Print("SQLly> ")
				continue
			}
			if input == "exit" {
				break
			}

			result := r.Db.ExecuteSql(input)
			fmt.Println(result)
			fmt.Print("SQLly> ")
		}
	}()
}