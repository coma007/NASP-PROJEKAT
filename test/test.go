package main

import (
	sys "Key-Value-Engine/kv-system"
	"fmt"
)

func main() {
	system := new(sys.System)
	system.Init()
	system.Put("Milica", []byte("Maca"), false)
	system.Put("ad", []byte("Peccca"), false)
	system.Put("aa", []byte("Macca"), false)
	_, value := system.Get("aa")
	fmt.Println("value")
	fmt.Println(value)
	_, value = system.Get("aa")
	fmt.Println("value")
	fmt.Println(value)
	fmt.Println("delete")
	fmt.Println(system.Delete("aa"))
	fmt.Println("opet get")
	fmt.Println(system.Get("aa"))

	// EDIT
	boolean :=  system.Edit("Milica", []byte("tralala..."))
	fmt.Println(boolean)
	_, value = system.Get("Milica")
	fmt.Println("Testiranje edita: ")
	fmt.Println(string(value))
}

