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
		fmt.Println("5. Iterate")
		fmt.Println("6. Probabilistic Types")
		fmt.Println("7. Check Merkle")
		fmt.Println("8. Exit")
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
			} else {
				fmt.Println("Put successful")
			}
		case 2:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Println(err)
			}
			value, err := eng.Get(key)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(value)
			}
		case 3:
			fmt.Print("Key: ")
			_, err := fmt.Scanf("%s", &key)
			if err != nil {
				fmt.Println(err)
			}

			err = eng.Delete(key)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Delete successful")
			}
		case 4:
			scanMenu(eng)
		case 5:
			iterateMenu(eng)
		case 6:
			probabilisticTypes(eng)
		case 7:
			fmt.Print("Path: ")
			var path string
			_, err := fmt.Scanf("%s", &path)
			if err != nil {
				fmt.Println(err)
				return
			}

			corrupted, err := eng.CheckMerkle(path)
			if err != nil {
				fmt.Println(err)
				return
			}

			if corrupted != nil {
				fmt.Println(fmt.Sprintf("%d corrupted nodes found", corrupted))
			} else {
				fmt.Println("Merkle is not corrupted")
			}
		case 8:
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
	case 3:
		return
	}
}

func iterateMenu(eng *engine.Engine) {
	fmt.Println("1. Prefix Iterate")
	fmt.Println("2. Range Iterate")
	fmt.Println("3. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		fmt.Println(err)
		return
	}

	switch expr {
	case 1:
		fmt.Print("Prefix: ")
		var prefix string
		_, err := fmt.Scanf("%s", &prefix)
		if err != nil {
			fmt.Println(err)
			return
		}

		iter, entry, err := eng.PrefixIterate(prefix)
		if err != nil {
			fmt.Println(err)
			return
		}
		if entry != nil {
			fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
		} else {
			fmt.Println("No records found")
			return
		}

		for {
			fmt.Println("1. Next")
			fmt.Println("2. Previous")
			fmt.Println("3. Stop")

			_, err := fmt.Scan(&expr)
			if err != nil {
				fmt.Println(err)
				return
			}

			switch expr {
			case 1:
				entry, err = iter.Next()
				if err != nil {
					if err.Error() == "no records left" {
						err := iter.Stop()
						if err != nil {
							fmt.Println(err)
						}
						return
					} else {
						fmt.Println(err)
						return
					}
				}
				if entry != nil {
					fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
				} else {
					fmt.Println("No records found")
					return
				}
			case 2:
				entry, err = iter.Previous()
				if err != nil {
					if err.Error() == "no records left" {
						err := iter.Stop()
						if err != nil {
							fmt.Println(err)
							return
						}
					} else {
						fmt.Println(err)
						return
					}
				}

				if entry != nil {
					fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
				} else {
					fmt.Println("No records found")
					return
				}
			case 3:
				err := iter.Stop()
				if err != nil {
					return
				}
				return
			}
		}
	case 2:
		fmt.Print("Min, maxKey: ")
		var keyMin, keyMax string
		_, err := fmt.Scanf("%s %s", &keyMin, &keyMax)
		if err != nil {
			fmt.Println(err)
			return
		}

		iter, entry, err := eng.RangeIterate(keyMin, keyMax)
		if err != nil {
			fmt.Println(err)
			return
		}
		if entry != nil {
			fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
		} else {
			fmt.Println("No records found")
			return
		}

		for {
			fmt.Println("1. Next")
			fmt.Println("2. Previous")
			fmt.Println("3. Stop")

			_, err := fmt.Scan(&expr)
			if err != nil {
				fmt.Println(err)
				return
			}

			switch expr {
			case 1:
				entry, err = iter.Next()
				if err != nil {
					if err.Error() == "no records left" {
						err := iter.Stop()
						if err != nil {
							fmt.Println(err)
						}
						return
					} else {
						fmt.Println(err)
						return
					}
				}
				if entry != nil {
					fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
				} else {
					fmt.Println("No records found")
					return
				}
			case 2:
				entry, err = iter.Previous()
				if err != nil {
					if err.Error() == "no records left" {
						err := iter.Stop()
						if err != nil {
							fmt.Println(err)
							return
						}
					} else {
						fmt.Println(err)
						return
					}
				}

				if entry != nil {
					fmt.Println(fmt.Sprintf("%s: %s", entry.Key, entry.Value))
				} else {
					fmt.Println("No records found")
					return
				}
			case 3:
				err := iter.Stop()
				if err != nil {
					return
				}
				return
			}
		}
	case 3:
		return
	}
}

func probabilisticTypes(eng *engine.Engine) {
	fmt.Println("1. Bloom Filter")
	fmt.Println("2. CountMinSketch")
	fmt.Println("3. HyperLogLog")
	fmt.Println("4. SimHash")
	fmt.Println("5. Exit")
	fmt.Print("> ")

	var expr int
	_, err := fmt.Scan(&expr)
	if err != nil {
		fmt.Println(err)
		return
	}

	switch expr {
	case 1:
		fmt.Print("Key: ")
		var key string
		_, err := fmt.Scanf("%s", &key)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = eng.BloomFilter(key)
		if err != nil {
			fmt.Println(err)
			return
		}
	case 2:
		fmt.Print("Key: ")
		var key string
		_, err := fmt.Scanf("%s", &key)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = eng.CountMinSketch(key)
		if err != nil {
			fmt.Println(err)
			return
		}
	case 3:
		fmt.Print("Key: ")
		var key string
		_, err := fmt.Scanf("%s", &key)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = eng.HyperLogLog(key)
		if err != nil {
			fmt.Println(err)
			return
		}
	case 4:
		fmt.Print("Key: ")
		var key string
		_, err := fmt.Scanf("%s", &key)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = eng.Simhash(key)
		if err != nil {
			fmt.Println(err)
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
