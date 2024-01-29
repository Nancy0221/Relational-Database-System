package list

import (
	"fmt"
	"io"
	"strings"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	return &List{head: nil, tail: nil}
	// panic("function not yet implemented");
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
	// panic("function not yet implemented");
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
	// panic("function not yet implemented");
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	var node = &Link{list: list, next: list.head, value: value}
	if list.head != nil {
		// connect node and the head
		list.head.prev = node
	}
	if list.tail == nil {
		// only one node in list, so make the tail points to node
		list.tail = node
	}
	// now, the new head is node
	list.head = node
	// what I going to return is a *Link type
	return node

	// panic("function not yet implemented");
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	var node = &Link{list: list, prev: list.tail, value: value}
	if list.tail != nil {
		// connect tail to node
		list.tail.next = node
	}
	if list.head == nil {
		// only one node in list, so make the head points to node
		list.head = node
	}
	// now,  the new tail is node
	list.tail = node
	// return the *Link type variable
	return node
	// panic("function not yet implemented");
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	var cur = list.head
	for {
		// finish to search all of the list
		if cur == nil {
			break
		}
		// to see if cur is the right element
		if f(cur) {
			return cur
		}
		// move cur pointer to the next element
		cur = cur.next
	}
	// right element isn't exist in list
	return nil
	// panic("function not yet implemented");
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	var cur = list.head
	for {
		if cur == nil {
			break
		}
		f(cur)
		cur = cur.next
		// panic("function not yet implemented");
	}
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
	// panic("function not yet implemented");
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
	// panic("function not yet implemented");
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
	// panic("function not yet implemented");
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
	// panic("function not yet implemented");
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
	// panic("function not yet implemented");
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	var l = link.list

	if l.head == link {
		// node is list head
		l.head = link.next
	}
	if l.tail == link {
		// node is list tail
		l.tail = link.prev
		if link.prev != nil {
			link.prev.next = nil
		}
	}
	var b = l.Find(func(node *Link) bool { return node.GetKey() == link.GetKey() })
	if b != nil {
		// node is in th middle of the list
		link.prev.next = link.next
		link.next.prev = link.prev
	}
	// panic("function not yet implemented");
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	var r = repl.NewRepl()
	// list_print------------------------------------------------------
	var actionListPrint = func(str string, replConfig *repl.REPLConfig) error {
		// action logic
		var cur = list.head
		if list.head == nil {
			io.WriteString(replConfig.GetWriter(), "The list is empty. \n")
		}
		for {
			if cur == nil {
				break
			}
			if cur.next != nil {
				io.WriteString(replConfig.GetWriter(), fmt.Sprint(cur.GetKey(), ", "))
			} else {
				io.WriteString(replConfig.GetWriter(), fmt.Sprint(cur.GetKey(), "\n"))
			}
			cur = cur.next
		}
		return nil
	}
	r.AddCommand("list_print", actionListPrint, "Prints out all of the elements in the list in order, separated by commas. ")

	// list_push_head <elt>--------------------------------------------
	var actionPushHead = func(input string, replConfig *repl.REPLConfig) error {
		var split = strings.Fields(input)
		var ele = split[1]
		list.PushHead(ele)
		return nil
	}
	r.AddCommand("list_push_head", actionPushHead, "Inserts the given element to the List as a string. ")

	// list_push_tail <elt>--------------------------------------------
	var actionPushTail = func(input string, replConfig *repl.REPLConfig) error {
		var split = strings.Fields(input)
		var ele = split[1]
		list.PushTail(ele)
		return nil
	}
	r.AddCommand("list_push_tail", actionPushTail, "Inserts the given element to the end of the List as a string. ")

	// list_remove <elt>--------------------------------------------
	var actionListRemove = func(input string, replConfig *repl.REPLConfig) error {
		var split = strings.Fields(input)
		var ele = split[1]
		var node = list.Find(func(node *Link) bool { return node.GetKey() == ele })
		if node != nil {
			node.PopSelf()
		} else {
			io.WriteString(replConfig.GetWriter(), "The element that you want to delete isn't in the list. \n")
		}
		return nil
	}
	r.AddCommand("list_remove", actionListRemove, "Removes the given element from the list. ")

	// list_contains <elt>--------------------------------------------
	var actionListContain = func(input string, replConfig *repl.REPLConfig) error {
		var split = strings.Fields(input)
		var ele = split[1]
		var node = list.Find(func(node *Link) bool { return node.GetKey() == ele })
		if node != nil {
			io.WriteString(replConfig.GetWriter(), "Found!\n")
		} else {
			io.WriteString(replConfig.GetWriter(), "Not found. \n")
		}
		return nil
	}
	r.AddCommand("list_contains", actionListContain, "To see if the element exist in the list. ")

	return r
	// panic("function not yet implemented")
}
