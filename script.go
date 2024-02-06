package main

import (
	"fmt"
	"github.com/DamjanVincic/key-value-engine/internal/engine"
)

func scriptMenu(eng *engine.Engine) {
	fmt.Println("1. 100 different keys")
	fmt.Println("2. 50000 different keys")
	fmt.Println("3. Exit")

	var expr int

	_, err := fmt.Scan(&expr)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 100_000; i++ {
		var key int
		switch expr {
		case 1:
			key = i % 100
		case 2:
			key = i % 50_000
		case 3:
			return
		}
		err := eng.Put(fmt.Sprintf("key%d", key), []byte(fmt.Sprintf("value%d", key)))
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(i)
	}
}

func main() {
	eng, err := engine.NewEngine()
	if err != nil {
		fmt.Println(err)
		return
	}

	scriptMenu(eng)
}
