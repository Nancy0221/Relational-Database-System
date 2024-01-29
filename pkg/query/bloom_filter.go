package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	"github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	// hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	return &BloomFilter{size: size, bits: bitset.New(uint(size))}
	// panic("function not yet implemented")
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	// using two different hash function to fill in the bit map
	var xxhashRes = hash.XxHasher(key, filter.size)
	var murmurhashRes = hash.MurmurHasher(key, filter.size)
	filter.bits.Set(xxhashRes)
	filter.bits.Set(murmurhashRes)
	// panic("function not yet implemented")
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	// get hashing result from two hash function
	var xxhashRes = hash.XxHasher(key, filter.size)
	var murmurhashRes = hash.MurmurHasher(key, filter.size)
	// get testing result
	var inXxHash = filter.bits.Test(xxhashRes)
	var inMurmurHash = filter.bits.Test(murmurhashRes)
	return inXxHash && inMurmurHash
	// panic("function not yet implemented")
}
