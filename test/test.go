package main

import (
	sys "Key-Value-Engine/kv-system"
	"fmt"
)

func main() {
	system := new(sys.System)
	system.Init()
	// PUT
	system.Put("Milica", []byte("Maca"), false)
	system.Put("ad", []byte("Peccca"), false)
	system.Put("aa", []byte("Macca"), false)
	system.Put("jovana", []byte("Maca"), false)
	system.Put("zz", []byte("Peccca"), false)
	system.Put("marija", []byte("Macca"), false)
	system.Put("milena", []byte("Maca"), false)
	system.Put("jovanina", []byte("Peccca"), false)
	system.Put("a", []byte("Macca"), false)
	system.Put("Krajoliko", []byte("Maca"), false)
	system.Put("mmm", []byte("Peccca"), false)
	system.Put("as", []byte("Macca"), false)
	_, value := system.Get("aa")
	fmt.Println("value")
	fmt.Println(string(value))
	_, value = system.Get("ad")
	fmt.Println("value")
	fmt.Println(string(value))

	// DELETE
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
