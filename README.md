# NoSQL Database (Key-Value Engine)

Project for Advanced Algorithms and Data Structures course, 3rd semester of Software Engineering, Faculty of Technical Sciences, Novi Sad, 2024

## Supported Operations
- **Put** - for inserting a new key-value pair
- **Get** - for getting the value of a key
- **Delete** - for deleting a key-value pair

<br/>
Keys are strings, values are bytes.

## Components
- **Write Ahead Log**
- **Memtable**
    - Can use Hash Map, Skip List or B Tree as a data structure
- **SSTable**
    - **Data** - for storing the data itself
    - **Filter** - Bloom Filter for checking if the key exists in the SSTable
    - **Index** - for finding the position of the key in the Data segment
    - **Summary** - for finding the position of the key in the Index segment and for storing min and max keys in the SSTable
    - **Metadata** - Merkle tree of all values from Data segment
- **LSM Tree**
    - SSTables are structured as a leveled LSM Tree
    - Implemented size-tiered and leveled compaction algorithms
- **Cache**
    - LRU Cache for storing the most frequently used values

## System Upgrades
- **External configuration** - a YAML configuration file for setting up most of the system parameters
- **Rate Limiting** - for limiting the number of requests per time period
    - Implemented using token bucket algorithm
- **Operations on probabilistic types**
    - Bloom Filter
    - CountMinSketch
    - HyperLogLog
    - SimHash
- **Compression**
    - Variable encoding for numeric values
    - Global compression dictionary
- **Scanning operations**
    - **Prefix Scan** - for finding keys with a given prefix, paginated
    - **Range Scan** - for finding keys in a given range, paginated
- **Operations with iterators**
    - **Prefix Iterate** - for iterating over keys with a given prefix
    - **Range Iterate** - for iterating over keys in a given range

## How to run
- `go run cmd/key-value-engine/main.go`

## Contributors:
- [Damjan Vinčić](https://github.com/DamjanVincic)
- [Nađa Zorić](https://github.com/zoricnadja)
- [Mijat Krivokapić](https://github.com/mijatkrivokapic)
- [Milica Radić](https://github.com/milicaradicc)