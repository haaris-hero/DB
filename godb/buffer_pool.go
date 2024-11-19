package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	numPages int                  // Capacity of the buffer pool
	pages    map[interface{}]Page // Map to store pages by key (e.g., DBFile.pageKey)
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		numPages: numPages,
		pages:    make(map[interface{}]Page),
	}, nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() error {
	for _, page := range bp.pages {
		if page.isDirty() {
			f := page.getFile()
			err := f.flushPage(page) // Assuming flushPage writes to disk
			if err != nil {
				return err
			}
			page.setDirty(0, false) // Mark page as not dirty after flushing
		}
	}
	return nil
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {

	pageKey := file.pageKey(pageNo) // Unique key for the page

	// Check if page is already in buffer pool
	if page, exists := bp.pages[pageKey]; exists {
		//fmt.Println("exisrtt")
		return page, nil
	}

	// If the page is not cached, check if we have space to add it
	if len(bp.pages) >= bp.numPages {
		// Handle eviction if buffer pool is full
		evicted := false
		for key, page := range bp.pages {
			if !page.isDirty() {
				delete(bp.pages, key) // Remove a clean page
				evicted = true
				break
			}
		}
		if !evicted {
			return nil, fmt.Errorf("all pages are dirty, cannot evict any page")
		}
	}

	// Load the page from disk using the DBFile's readPage method
	newPage, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}

	// Add the new page to the buffer pool
	bp.pages[pageKey] = newPage
	return newPage, nil
}
