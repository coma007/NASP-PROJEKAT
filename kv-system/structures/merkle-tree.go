package structures

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
)

type MerkleRoot struct {
	root *MerkleNode
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type MerkleNode struct {
	data  [20]byte
	left  *MerkleNode
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
func StringsToBytes(strings []string) [][]byte {
	data := [][]byte{}
	for i := 0; i < len(strings); i++ {
		key_byte := []byte(strings[i])
		data = append(data, key_byte)
	}
	return data
}

// CreateMerkleTree - funkcija od koje krece kriranje stabla
func CreateMerkleTree(keys [][]byte, path string) *MerkleRoot {

	// ako je proslijedjen array bajtova
	data := keys

	leaves := Leaves(data)
	root_node := CreateAllNodes(leaves)

	root := MerkleRoot{root_node}
	new_path := strings.Replace(path, "Data.db", "Metadata.txt", 1)
	new_path = "./kv-system/data/metadata/" + new_path
	WriteInFile(root_node, new_path)
	return &root
}

// Leaves - formira listove stabla
func Leaves(data [][]byte) []*MerkleNode {
	leaves := []*MerkleNode{}

	for i := 0; i < len(data); i++ {
		node := MerkleNode{Hash(data[i]), nil, nil}
		leaves = append(leaves, &node)
	}

	return leaves
}

// CreateAllNodes - kreira sve nivoe stabla od listova ka korijenu
func CreateAllNodes(leaves []*MerkleNode) *MerkleNode {

	// svi cvorovi jednog nivoa
	level := []*MerkleNode{}

	nodes := leaves

	if len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			if (i + 1) < len(nodes) {
				node1 := nodes[i]
				node2 := nodes[i+1]
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := MerkleNode{Hash(new_node_bytes), node1, node2}
				level = append(level, &new_node)
			} else { // ako nam fali odgovarajuci cvor
				node1 := nodes[i]
				node2 := MerkleNode{data: [20]byte{}, left: nil, right: nil}
				node1_data := node1.data[:]
				node2_data := node2.data[:]
				new_node_bytes := append(node1_data, node2_data...)
				new_node := MerkleNode{Hash(new_node_bytes), node1, &node2}
				level = append(level, &new_node)
			}
		}
		nodes = level

		if len(nodes) == 1 {
			return nodes[0]
		}
	}
	return CreateAllNodes(level)
}

// PrintTree - print stablo po sirini
func PrintTree(root *MerkleNode) {
	queue := make([]*MerkleNode, 0)
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

func WriteInFile(root *MerkleNode, path string) {
	newFile, err := os.Create(path)
	err = newFile.Close()
	if err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0444)
	if err != nil {
		log.Fatal(err)
	}

	queue := make([]*MerkleNode, 0)
	queue = append(queue, root)

	for len(queue) != 0 {
		e := queue[0]
		queue = queue[1:]
		_, _ = file.WriteString(e.String() + "\n")

		if e.left != nil {
			queue = append(queue, e.left)
		}
		if e.right != nil {
			queue = append(queue, e.right)
		}
	}
	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}
}
