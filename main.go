package main

import (
	sys "Key-Value-Engine/kv-system"
	"Key-Value-Engine/kv-system/structures"
	"bufio"
	"fmt"
	"os"
)

func menu() {
	fmt.Println("\n======= MENU =======")
	fmt.Println(" 1. PUT")
	fmt.Println(" 2. GET")
	fmt.Println(" 3. DELETE")
	fmt.Println(" 4. EDIT")
	fmt.Println("--- HyperLogLog  ---")
	fmt.Println(" 5. CREATE HLL")
	fmt.Println(" 6. ADD TO HLL")
	fmt.Println(" 7. ESTIMATE HLL")
	fmt.Println("-- CountMinSketch --")
	fmt.Println(" 8. CREATE CMS")
	fmt.Println(" 9. ADD TO CMS")
	fmt.Println("10. QUERY CMS")
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
	case "5":
		fmt.Println("\n- CREATE HLL")
		fmt.Print("HLL's Key: ")
		key := "hll-" + scan()
		value := structures.CreateHLL(uint8(system.Config.HLLParameters.HLLPrecision)).SerializeHLL()
		system.Put(key, value, false)
		break
	case "6":
		fmt.Println("\n- ADD TO HLL")
		fmt.Print("HLL's Key: ")
		key := "hll-" + scan()
		ok, hllData := system.Get(key)
		if !ok {
			fmt.Println("HLL with given key not found !")
			break
		}
		hll := structures.DeserializeHLL(hllData)
		fmt.Print("Value to add: ")
		value := scan()
		hll.Add(value)
		fmt.Println("Value added !")
		system.Put(key, hll.SerializeHLL(), false)
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