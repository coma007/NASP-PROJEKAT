package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type MerkleRoot struct {
	root *MerkleNode
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type MerkleNode struct {
	data [20]byte
	left *MerkleNode
	right *MerkleNode
}

func (n *MerkleNode) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// ...

// StringsToBytes - treba u slucaju da se posalje array stringova
func StringsToBytes(strings []string) [][] byte {
	data := [][] byte {}
	for i:= 0; i < len(strings); i++ {
		key_byte := []byte(strings[i])
		data = append(data, key_byte)
	}
	 return data
}

// CreateMerkleTree - funkcija od koje krece kriranje stabla
func CreateMerkleTree(keys [][]byte) MerkleRoot {
	//                keys []string

	// string to byte
	//data := StringsToBytes(keys)

	// ako je proslijedjen array bajtova
	data := keys

	leaves := Leaves(data)
	all_nodes := CreateAllNodes(leaves)

	root := MerkleRoot{all_nodes[len(all_nodes) - 1][0]}
	return root

}

// Leaves - formira listove stabla
func Leaves(data [][]byte) []*MerkleNode {
	leaves := []*MerkleNode{}

	for i:= 0; i < len(data); i++ {
		node := MerkleNode{Hash(data[i]), nil, nil}
		leaves = append(leaves, &node)
	}

	return leaves
}

// CreateAllNodes - kreira sve nivoe stabla od listova ka korijenu
func CreateAllNodes(leaves []*MerkleNode) [][]*MerkleNode {
	// all_levels - lista nivoa (svaki nivo je lista cvorova)
	all_levels := [][]*MerkleNode{}
	all_levels = append(all_levels, leaves)

	// svi cvorovi jednog nivoa
	level := []*MerkleNode{}

	nodes := leaves

	for len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			if (i + 1) < len(nodes) {
				node1 := nodes[i]
				node2 := nodes[i+1]
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := MerkleNode{Hash(new_node_bytes), node1, node2}
				level = append(level, &new_node)
			} else {  // ako nam fali odgovarajuci cvor
				node1 := nodes[i]
				node2 := MerkleNode{data: [20]byte{}, left: nil, right: nil}
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := MerkleNode{Hash(new_node_bytes), node1, &node2}
				level = append(level, &new_node)
			}
		}
		all_levels = append(all_levels, level)
		nodes = level
		level = []*MerkleNode{} // prelazak na novi nivo
	}
	return all_levels
}

// PrintTree - print stablo po sirini
func PrintTree(root *MerkleNode) {
	queue := make([] *MerkleNode, 0)
	queue = append(queue, root)

	for len(queue) != 0 {
		e := queue[0]
		queue = queue[1:]
		fmt.Println(e.String())

		if e.left != nil {
			queue = append(queue, e.left)
		}
		if e.right != nil {
			queue = append(queue, e.right)
		}
	}
}

func WriteInFile(root *MerkleNode) {
	file, err := os.OpenFile("data/metadata/metadata.txt", os.O_WRONLY, 0444)
	if err != nil {
		log.Fatal(err)
	}

	queue := make([] *MerkleNode, 0)
	queue = append(queue, root)

	for len(queue) != 0 {
		e := queue[0]
		queue = queue[1:]
		bytes := e.data[:]
		_, _ = file.Write(bytes)

		if e.left != nil {
			queue = append(queue, e.left)
		}
		if e.right != nil {
			queue = append(queue, e.right)
		}
	}
}

//func main(){
//	fmt.Println("Pocetak...")
//
//	stringovi := []string{"kljuc1", "key2", "kljuc3", "key", "kljuc5"}
//	bajtovi := StringsToBytes(stringovi)
//	root := CreateMerkleTree(bajtovi)
//	current := root.root
//
//	PrintTree(current)
//
//	WriteInFile(current)
//
//	fmt.Println("Kraj...")
//}