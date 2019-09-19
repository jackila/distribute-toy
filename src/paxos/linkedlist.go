package paxos

//ElemType is list type
type ElemType interface{}

//Node is list node
type Node struct {
	State AcceptorState
	Seq   int
	Next  *Node
}

//LinkedList is single list
type LinkedList struct {
	Head *Node
}

//NewNode create a new node
func NewNode(Seq int, State AcceptorState) *Node {
	return &Node{State, Seq, nil}
}

//NewLinkedList create a new list
//the head node data is the length of the linked list
func NewLinkedList() *LinkedList {
	head := &Node{AcceptorState{}, -1, nil}
	return &LinkedList{head}
}

//Append insert an element after
//the list
func (list *LinkedList) Append(node *Node) {
	exist, nNode := list.isNewNode(node)
	if exist {
		nNode.State = node.State
		return
	}
	tmpList := list.Head
	for {
		if tmpList.Next == nil {
			break
		} else {
			tmpList = tmpList.Next
		}
	}
	tmpList.Next = node
}

//DeleteElem delete a designation element from
//the list
func (list *LinkedList) DeleteElem(Seq int) bool {
	var lastList *Node
	lastList = list.Head
	tmpList := list.Head.Next
	for {
		if tmpList.Seq != Seq {
			if tmpList.Next != nil {
				lastList = tmpList
				tmpList = tmpList.Next
			} else {
				break
			}
		} else {
			lastList.Next = tmpList.Next
			tmpList = nil
			return true
		}
	}
	return false
}

//Find searce the element from the list
func (list *LinkedList) Find(Seq int) (*Node, bool) {
	tmpList := list.Head
	for tmpList.Seq != Seq {
		if tmpList.Next != nil {
			tmpList = tmpList.Next
		} else {
			//fmt.Println("Error :this data is not in the list!!!")
			return NewNode(Seq, AcceptorState{}), false
		}
	}
	return tmpList, true
}

//isNewNode determine the node is acceptable
func (list *LinkedList) isNewNode(node *Node) (bool, *Node) {
	tmplist := list.Head
	for tmplist.Next != nil {
		if tmplist.Next.Seq == node.Seq {
			return false, tmplist.Next
		}
		tmplist = tmplist.Next
	}
	return false, &Node{}
}
