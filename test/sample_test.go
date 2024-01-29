package test

import (
	"fmt"
	"io"
	"testing"

	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"
	"github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

func TestSample(t *testing.T) {
	l := list.NewList()
	if l.PeekHead() != nil || l.PeekTail() != nil {
		t.Fatal("bad list initialization")
	}
}

func TestSample2(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushTail(2)
	l.PushTail(3)
	if l.PeekHead().GetKey() != 1 {
		t.Fatal("something wrong")
	}
}

func TestSample3(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushTail(2)
	l.PushTail(3)
	if l.PeekTail().GetKey() != 3 {
		t.Fatal("something wrong")
	}
}

func TestPopSelf1(t *testing.T) {
	l := list.NewList()
	// l.PushHead(1)
	// l.PushTail(2)
	l.PushTail(3)
	head := l.PeekHead()
	head.PopSelf()
	if l.PeekHead() != nil {
		t.Fatal("head wrong")
	}

	if l.PeekTail() != nil {
		t.Fatal("tail wrong: " + fmt.Sprint(l.PeekTail().GetKey()))
	}
}

func TestAddNewTailAndPop(t *testing.T) {
	l := list.NewList()
	l.PushHead(1)
	l.PushTail(2)
	l.PushTail(3)
	tail := l.PeekTail()
	tail.PopSelf()
	if l.PeekTail().GetKey() != 2 {
		t.Fatal("tail wrong")
	}
}

func TestAddCommand(t *testing.T) {
	var r = repl.NewRepl()
	var action = func(str string, replConfig *repl.REPLConfig) error {
		// action logic: print the help infomation
		io.WriteString(replConfig.GetWriter(), r.HelpString())
		return nil
	}
	r.AddCommand(".help", action, "get infomation about triggers")
	t.Fatal(fmt.Sprint(r.GetHelp()))
}
