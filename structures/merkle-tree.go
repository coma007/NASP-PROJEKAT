package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// ...

func StringsToBytes(strings []string) [][] byte{
	data := [][] byte {}
	for i:= 0; i < len(strings); i++ {
		key_byte := []byte(strings[i])
		data = append(data, key_byte)
	}
	 return data
}

// CreateMerkleTree - funkcija od koje krece kriranje stabla
func CreateMerkleTree(keys []string) MerkleRoot{

	// string to byte
	data := StringsToBytes(keys)

	leafs := Leafs(data)
	all_nodes := CreateAllNodes(leafs)

	root := MerkleRoot{all_nodes[len(all_nodes) - 1][0]}
	return root

}

// Leafs - formira listove stabla
func Leafs(data [][]byte) []*Node{
	leafs := []*Node{}

	for i:= 0; i < len(data); i++ {
		node := Node{Hash(data[i]), nil, nil}
		leafs = append(leafs, &node)
	}

	return leafs
}

// CreateAllNodes - kreira sve nivoe stabla od listova ka korijenu
func CreateAllNodes(leafs []*Node) [][]*Node{
	// all_levels - lista nivoa (svaki nivo je lista cvorova)
	all_levels := [][]*Node {}
	all_levels = append(all_levels, leafs)

	// svi cvorovi jednog nivoa
	level := []*Node {}

	nodes := leafs

	for len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			if (i + 1) < len(nodes) {
				node1 := nodes[i]
				node2 := nodes[i+1]
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := Node{Hash(new_node_bytes), node1, node2}
				level = append(level, &new_node)
			} else {  // ako nam fali cvor odgovarajuci cvor
				node1 := nodes[i]
				node2 := Node{data: [20]byte{}, left: nil, right: nil}
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := Node{Hash(new_node_bytes), node1, &node2}
				level = append(level, &new_node)
			}
		}
		all_levels = append(all_levels, level)
		nodes = level
		level = []*Node {}  // prelazak na novi nivo
	}
	return all_levels
}

// PrintTree - print stablo po sirini
func PrintTree(root *Node) {
	queue := make([] *Node, 0)
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

func main(){
	fmt.Println("Pocetak...")

	stringovi := []string{"kljuc1", "key2", "kljuc3", "key", "kljuc5"}
	root := CreateMerkleTree(stringovi)
	current := root.root

	PrintTree(current)

	fmt.Println("Kraj...")
}