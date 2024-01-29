package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tl.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// create editLog and write it to buffer
	var log = editLog{id: clientId, tablename: table.GetName(), action: action, key: key, oldval: oldval, newval: newval}
	rm.writeToBuffer(log.toString())
	// put it in txStack
	rm.txStack[clientId] = append(rm.txStack[clientId], &log)

	// panic("function not yet implemented")
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// get start log and write it to buffer
	var log = startLog{id: clientId}
	rm.writeToBuffer(log.toString())
	// put it in txStack
	rm.txStack[clientId] = append(rm.txStack[clientId], &log)

	//panic("function not yet implemented")
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// get commit log and write it to buffer
	var log = commitLog{id: clientId}
	// When a transaction commits, you can delete all of its data in the txStack map.
	// delete it from txStack because it is already committed, noting to do with it
	delete(rm.txStack, clientId)
	rm.writeToBuffer(log.toString())
	// put it in txStack
	rm.txStack[clientId] = append(rm.txStack[clientId], &log)

	// panic("function not yet implemented")
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// Prevent all tables from being written to while checkpointing!
	// go through each table in rm.d.GetTables() and run table.GetPager().FlushAllPages(). 
	for _, value := range rm.d.GetTables()  {
		// value is index
		var p = value.GetPager()
		// Remember to lock the respective Pagers using LockAllUpdates() and UnlockAllUpdates().
		p.LockAllUpdates()
		p.FlushAllPages()
		p.UnlockAllUpdates()
	}
	// Write the log AFTER flushing to disk!
	// CHECKPOINT log -- lists the currently running transactions
	// var log = checkpointLog{}
	var log = make([]uuid.UUID, 0)
	for key, _ := range rm.txStack {
		// key is id
		log = append(log, key)
		// log.ids = append(log.ids, key)
	}
	// write to buffer
	var l = checkpointLog{ids: log}
	rm.writeToBuffer(l.toString())

	// panic("function not yet implemented")
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	// Seek backwards through the logs to the most recent checkpoint and note which transactions are currently active.
	var currentActivate = make(map[uuid.UUID]bool)
	// get log and most recent checkpoint position
	var log, ptr, err = rm.readLogs()
	if err != nil {
		return err
	}

	var length = len(log)
	for ptr <= length - 1 {
		// var currentLog = 
		// Replay all actions from the most recent checkpoint to the end of the log, keeping track of which transactions are active.
		switch logType := log[ptr].(type) {
		// only tableLog and editLog call Redo()
		case *tableLog: 
			rm.Redo(logType)
		case *checkpointLog:
			// checkpointLog contains the current running txns
			for _, txn := range logType.ids {
				currentActivate[txn] = true
				rm.tm.Begin(txn)
			}
		case *editLog:
			currentActivate[logType.id] = true
			rm.Redo(logType)
		case *startLog: 
			currentActivate[logType.id] = true
			rm.tm.Begin(logType.id)
		case *commitLog: 
			delete(currentActivate, logType.id)
			rm.tm.Commit(logType.id)
		}
		ptr = ptr + 1
	}
	// in reverse order
	var undoPtr = length - 1
	for undoPtr >= 0 {
		switch logFile := log[undoPtr].(type) {
		case *editLog:
			// Undo all transactions that have failed to commit. 
			// this log belong to the current activate transaction
			var _, find = currentActivate[logFile.id]
			if find {
				var err = rm.Undo(logFile)
				if err != nil {
					return err
				}
			}
		case *startLog:
			var _, find = currentActivate[logFile.id]
			if find {
				delete(currentActivate, logFile.id)
				// Commit those undone transactions to mark them as done.
				// Write a transaction commit log.
				rm.Commit(logFile.id)
				// Commits the given transaction and removes it from the running transactions list.
				var err = rm.tm.Commit(logFile.id)
				if err != nil {
					return err
				}
			}
		}
		undoPtr = undoPtr - 1
	}
	return nil

	// panic("function not yet implemented")
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	// If there are no logs, ...?
	var log, find = rm.txStack[clientId]
	if !find {
		return nil
	}
	// Check that the log is valid from the beginning.
	if len(log) > 0 {
		// make sure first log is startLog
		if _, ok := log[0].(*startLog); !ok {
			return errors.New("incorrect format")
		}
		// Rollback the rest of the logs LIFO.
		for ptr := len(log) - 1; ptr >= 0; ptr-- {
			if _, ok := log[ptr].(*editLog); ok {
				var err = rm.Undo(log[ptr])
				if err != nil {
					return err
				}
			}
		}

		// commit log and txn when done
		rm.Commit(clientId)
		var err =  rm.tm.Commit(clientId)
		if err != nil {
			return err
		}
	} else {
		// commit log and txn when done
		rm.Commit(clientId)
		var err =  rm.tm.Commit(clientId)
		if err != nil {
			return err
		}
	}
	return nil

	// panic("function not yet implemented")
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
