package main

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/engine"
	"log"
)

func mainMenu(eng *engine.Engine) {
	var key string
	var value []byte

	var expr int
	for true {
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. Exit")
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
				log.Fatal(err)
			}
			err = eng.Put(key, value)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Put successful")
		case 2:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				log.Fatal(err)
			}
			value, err := eng.Get(key)
			if err != nil {
				if err.Error() == "key not found" {
					fmt.Println(err)
				} else {
					log.Fatal(err)
				}
			}
			fmt.Println(value)
		case 3:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				log.Fatal(err)
			}

			err = eng.Delete(key)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Delete successful")
		case 4:
			return
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
