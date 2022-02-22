package main

import (
	sys "Key-Value-Engine/kv-system"
	"bufio"
	"fmt"
	"os"
)

func menu() {
	fmt.Println("\n======= MENU =======")
	fmt.Println("1. PUT")
	fmt.Println("2. GET")
	fmt.Println("3. DELETE")
	fmt.Println("4. EDIT")
	fmt.Println("--- HyperLogLog  ---")
	fmt.Println("5. ADD TO HLL")
	fmt.Println("6. ESTIMATE HLL")
	fmt.Println("-- CountMinSketch --")
	fmt.Println("7. ADD TO CMS")
	fmt.Println("8. QUERY CMS")
	fmt.Println("--------------------")
	fmt.Println("0. EXIT")
	fmt.Print("\nChose option from menu: ")
}

func scan() string {
	input := bufio.NewScanner(os.Stdin)
	input.Scan()
	return input.Text()
}

func parseChoice(choice string, system *sys.System) bool {
	switch choice {
	case "0":
		fmt.Println("\nGoodbye !")
		return false
	case "1":
		fmt.Println("\n- PUT")
		fmt.Print("Key: ")
		key := scan()
		fmt.Print("Value: ")
		value := scan()
		system.Put(key, []byte(value), false)
		break
	case "2":
		fmt.Println("\n- GET")
		fmt.Print("Key: ")
		key := scan()
		_, val := system.Get(key)
		fmt.Println("Value: ", string(val))
		break
	case "3":
		fmt.Println("\n- DELETE")
		fmt.Print("Key: ")
		key := scan()
		system.Delete(key)
		break
	case "4":
		fmt.Println("\n- PUT")
		fmt.Print("Key: ")
		key := scan()
		fmt.Print("Value: ")
		value := scan()
		system.Edit(key, []byte(value))
		break
	default:
		fmt.Println("\nWrong input ! Please try again. ")
		break
	}

	return true
}

func main() {

	system := new(sys.System)
	system.Init()
	fmt.Println("Welcome !")
	run := true
	for run {
		menu()
		run = parseChoice(scan(), system)
	}
	//for {
	//	fmt.Println(menu)
	//}
}