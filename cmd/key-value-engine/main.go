package main

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/engine"
)

func mainMenu(eng *engine.Engine) {
	var key string
	var value []byte

	var expr int
	for true {
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. Scan")
		fmt.Println("5. Exit")
		fmt.Print("> ")
		_, err := fmt.Scan(&expr)
		if err != nil {
			return
		}

		switch expr {
		case 1:
			fmt.Print("Key value: ")
			_, err := fmt.Scanf("%s %s", &key, &value)
			if err != nil {
				fmt.Println(err)
			}
			err = eng.Put(key, value)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Put successful")
		case 2:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Println(err)
			}
			value, err := eng.Get(key)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(value)
		case 3:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Println(err)
			}

			err = eng.Delete(key)
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println("Delete successful")
		case 4:
			scanMenu(eng)
		case 5:
			return
		}
	}
}

func scanMenu(eng *engine.Engine) {
	fmt.Println("1. Prefix Scan")
	fmt.Println("2. Range Scan")
	fmt.Println("3. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		return
	}

	switch expr {
	case 1:
		fmt.Print("Prefix, page number, page size: ")
		var prefix string
		var pageNumber, pageSize int
		_, err := fmt.Scanf("%s %d %d", &prefix, &pageNumber, &pageSize)
		if err != nil {
			fmt.Println(err)
		}

		entries, err := eng.PrefixScan(prefix, pageNumber, pageSize)
		if err != nil {
			fmt.Println(err)
		}

		for _, entry := range entries {
			fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
		}
	case 2:
		fmt.Print("Min, maxKey, page number, page size: ")
		var minKey, maxKey string
		var pageNumber, pageSize int
		_, err := fmt.Scanf("%s %s %d %d", &minKey, &maxKey, &pageNumber, &pageSize)
		if err != nil {
			fmt.Println(err)
		}

		entries, err := eng.RangeScan(minKey, maxKey, pageNumber, pageSize)
		if err != nil {
			fmt.Println(err)
		}

		for _, entry := range entries {
			fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
		}
	}
}

func main() {
	eng, err := engine.NewEngine()
	if err != nil {
		panic(err)
		return
	}

	mainMenu(eng)
}
