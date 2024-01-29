package concurrency

import (
	"errors"
	"sync"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	uuid "github.com/google/uuid"
)

// Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// Release the write lock on the tx
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// Release the write lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// Get the transaction id.
func (t *Transaction) GetClientID() (clientId uuid.UUID) {
	return t.clientId
}

// Get the transaction's resources.
func (t *Transaction) GetResources() (resources map[Resource]LockType) {
	return t.resources
}

// Transaction Manager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// Get the transactions.
func (tm *TransactionManager) GetLockManager() (lm *LockManager) {
	return tm.lm
}

// Get the transactions.
func (tm *TransactionManager) GetTransactions() (txs map[uuid.UUID]*Transaction) {
	return tm.transactions
}

// Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (tx *Transaction, found bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	tx, found = tm.transactions[clientId]
	return tx, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) (err error) {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Locks the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) (err error) {
	/* SOLUTION {{{ */
	// Get the transaction we want, and construct the resource.
	tm.tmMtx.RLock()
	t, found := tm.GetTransaction(clientId)
	if !found {
		tm.tmMtx.RUnlock()
		return errors.New("transaction not found")
	}
	resource := Resource{tableName: table.GetName(), resourceKey: resourceKey}
	// Check if we already have rights to the resource
	t.RLock()
	if curLockType, ok := t.resources[resource]; ok {
		tm.tmMtx.RUnlock()
		if curLockType == W_LOCK || curLockType == lType {
			t.RUnlock()
			return nil
		}
		t.RUnlock()
		return errors.New("cannot upgrade to write lock in the middle of transaction")
	}
	t.RUnlock()
	// Create a precedence graph, see if we create a cycle by locking this resource.
	for _, tt := range tm.discoverTransactions(resource, lType) {
		if t == tt {
			continue
		}
		tm.pGraph.AddEdge(t, tt)
		defer tm.pGraph.RemoveEdge(t, tt)
	}
	// If a deadlock, unlock and error.
	if tm.pGraph.DetectCycle() {
		tm.tmMtx.RUnlock()
		return errors.New("deadlock detected")
	}
	// Else, lock the resource.
	tm.tmMtx.RUnlock()
	tm.lm.Lock(resource, lType)
	t.WLock()
	defer t.WUnlock()
	t.resources[resource] = lType
	return nil
	/* SOLUTION }}} */
}

// Unlocks the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) (err error) {
	/* SOLUTION {{{ */
	// Get the transaction we want, and construct the resource.
	tm.tmMtx.RLock()
	t, found := tm.GetTransaction(clientId)
	tm.tmMtx.RUnlock()
	if !found {
		return errors.New("transaction not found")
	}
	resource := Resource{tableName: table.GetName(), resourceKey: resourceKey}
	// Iterate through our locks to find the right one and remove it.
	t.WLock()
	defer t.WUnlock()
	removed := false
	for r, storedType := range t.resources {
		if r == resource {
			if storedType != lType {
				return errors.New("incorrect unlock type")
			}
			removed = true
			delete(t.resources, r)
			break
		}
	}
	// Error if no lock found.
	if !removed {
		return errors.New("resource not locked")
	}
	// Unlock the resource.
	err = tm.lm.Unlock(resource, lType)
	if err != nil {
		return err
	}
	return nil
	/* SOLUTION }}} */
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) (err error) {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) (txs []*Transaction) {
	txs = make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				txs = append(txs, t)
				break
			}
		}
		t.RUnlock()
	}
	return txs
}