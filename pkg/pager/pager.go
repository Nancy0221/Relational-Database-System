package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	config "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/config"
	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	maxPageNum   int64                // The number of pages used by this database.
	ptMtx        sync.Mutex           // Page table mutex.
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return filepath.Base(pager.file.Name())
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.maxPageNum
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.maxPageNum
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.maxPageNum = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {
	var page *Page = nil
	var page_in_freelist = pager.freeList.PeekHead()
	var page_in_unpinnedlist = pager.unpinnedList.PeekHead()
	// return page from free list
	if page_in_freelist != nil {
		// delete it from free list to prevent get it repeately
		page_in_freelist.PopSelf()
		// assign value to page
		// (*Page) is a type assertion, which asserts that the value retrieved from freeLink.GetKey() 
		// 		is of type *Page. If it's not of that type, it will result in a runtime panic.
		page = page_in_freelist.GetKey().(*Page)
	} else if !pager.HasFile() { 
		//  throw an error if there is no available page from the free list and the pager is not backed by disk
		return page, errors.New("pager is not backed by disk")
	} else if page_in_unpinnedlist != nil {
		// no page in free list, evict a page from unpinned list and return clean page
		page_in_unpinnedlist.PopSelf()
		page = page_in_unpinnedlist.GetKey().(*Page)
		// write page data to disk, and this page might be a dirty page
		// we already removed it from unpinnedList, so we'd better to check whether it is dirty and need to write it to the disk
		pager.FlushPage(page)
		// page dne in unpinnedlist
		delete(pager.pageTable, page.pagenum)
	} else {
		// no page in either list, throw error
		return page, errors.New("no page in either list")
	}
	// update variable's values in page and return it
	page.pagenum = pagenum
	page.pinCount = 1
	page.dirty = false
	return page, nil

	// panic("function not yet implemented")
}

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	page = nil
	// 1. invalid page number -> throw an error
	if pagenum < 0 {
		return page, errors.New("invalid page number")
	}
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	// 2. either in the unpinned or pinned list
	// 		(1) unpinned list: then we note that it is actively being used and return it
	// 				-> remove it from unpinned list and add it to pinned list
	// 				-> update pageTable, map the new page to the old pagenum
	// 		(2) pinned list: we have already noted it's actively being used
	// 				-> just get the page and then do nothing
	var node, ok = pager.pageTable[pagenum]
	if ok {
		var list = node.GetList()
		page = node.GetKey().(*Page)
		// check if the list that contains node is the unpinnedList or not
		if list == pager.unpinnedList {
			// delete node from unpinnedLst
			node.PopSelf()
			// add it to pinnedList
			var new_page_in_pinnedList = pager.pinnedList.PushTail(page)
			// update pageTable
			pager.pageTable[pagenum] = new_page_in_pinnedList
		}
		page.Get()
		return page, nil
	} 
	// 3. if the new page is introduced to the system, you must update them
	page, err = pager.NewPage(pagenum)
	if err != nil {
		return nil, err
	} 
	// 4. in the used range, we read the data from disk into our page
	if pagenum < pager.maxPageNum {
		err = pager.ReadPageFromDisk(page, pagenum)
		if err != nil {
			return nil, err
		}
	} else {
		// deal with 3: not in the system, should update some infomation
		page.dirty = true
		pager.maxPageNum++
	}

	// 5. ensure that the page table is up to date and return the page
	// 		(1) add this page to pinnedList
	// 		(2) map the new page to its pagenum in pageTable
	pager.pageTable[pagenum] = pager.pinnedList.PushTail(page)
	// pinCount = 1 here
	return page, nil

	// panic("function not yet implemented")
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	// We should only do this if the file exists and the page is dirty
	if page.dirty && pager.HasFile() {
		// *page.data: data we want to write
		// page.pagenum * PAGESIZE: offset * page size
		pager.file.WriteAt(*page.data, page.pagenum * PAGESIZE)
		page.dirty = false
	}
	// panic("function not yet implemented")
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	for _, v := range pager.pageTable {
		pager.FlushPage(v.GetKey().(*Page))
	}
	// panic("function not yet implemented")
}

// [RECOVERY] Block all updates.
func (pager *Pager) LockAllUpdates() {
	pager.ptMtx.Lock()
	for _, page := range pager.pageTable {
		page.GetKey().(*Page).LockUpdates()
	}
}

// [RECOVERY] Enable updates.
func (pager *Pager) UnlockAllUpdates() {
	for _, page := range pager.pageTable {
		page.GetKey().(*Page).UnlockUpdates()
	}
	pager.ptMtx.Unlock()
}
