# NoSQL Database

This project is an implementation of a custom NoSQL key-value database engine from scratch.

## Team Members
- [Anastasija Savić]
- [Katarina Vučić]
- [Milica Sladaković]
- [Nemanja Dutina]

[Anastasija Savić]:https://github.com/savic-a
[Katarina Vučić]:https://github.com/kaca01
[Milica Sladaković]:https://github.com/coma007
[Nemanja Dutina]:https://github.com/eXtremeNemanja

## Overview
**NoSQL databases**, or "Not Only SQL" databases, are a category of databases designed to handle unstructured, semi-structured, or rapidly changing data. Unlike traditional relational databases, NoSQL databases offer more flexibility and scalability for handling large volumes of data.

### NoSQL Database Characteristics
NoSQL databases exhibit the following characteristics:

- **Schema Flexibility:** NoSQL databases allow data to be stored without a fixed schema, enabling easy adaptation to evolving data structures.
- **Horizontal Scalability:** Many NoSQL databases are designed to scale out horizontally by distributing data across multiple nodes, enabling efficient handling of massive amounts of data and high traffic loads.
- **High Performance:** NoSQL databases often optimize for specific use cases, providing fast read and write operations.
- **Variety of Data Models:** NoSQL databases support various data models, including key-value, document, column-family, and graph, catering to different data storage and retrieval requirements.

### Types of NoSQL Databases
NoSQL databases are categorized into several types, each serving different purposes:

1. **Key-Value Stores:** These databases store data as key-value pairs, offering simple and efficient data retrieval by using keys.
2. **Document Stores:** Document-oriented databases store data in flexible JSON-like documents, making them suitable for semi-structured or unstructured data.
3. **Column-Family Stores:** These databases organize data into column families, allowing efficient querying of specific columns and scaling horizontally.
4. **Graph Databases:** Graph databases are designed for managing highly interconnected data, making them ideal for applications like social networks or recommendation engines.

### This Project's Scope
The custom NoSQL key-value database implemented in this project focuses on the key-value store model. It consists of two main components: the **write path** and the **read path**. The write path handles data insertion and updates, while the read path focuses on efficient data retrieval.

By building this NoSQL database engine, we gained a deeper understanding of the underlying principles behind NoSQL databases, including data storage, indexing, and optimization strategies.


## Table of Contents
- [Features](#features)
  - [Key-Value Operations](#key-value-operations)
  - [Caching Mechanism](#caching-mechanism)
  - [User Request Limitation](#user-request-limitation)
  - [Configuration](#configuration)
- [Write Path](#write-path)
  - [Write-Ahead Log (WAL) and Memtable](#write-ahead-log-wal-and-memtable)
  - [SSTable Creation](#sstable-creation)
  - [LSM Tree Structure and Manual Compactions](#lsm-tree-structure-and-manual-compactions)
  - [Merkle Tree for Data Validation](#merkle-tree-for-data-validation)
  - [Logical Deletes and In-Place Edits](#logical-deletes-and-in-place-edits)
  - [Data Persistence and Storage Location](#data-persistence-and-storage-location)
- [Read Path](#read-path)
  - [Flow of the Read Path](#flow-of-the-read-path)
  - [In-Memory Structures](#in-memory-structures)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Dependencies](#dependencies)
- [Usage Examples](#usage-examples)

## Features
This project encompasses a range of features that make up the core functionality of the custom NoSQL key-value database:

### Key-Value Operations
The database supports the following key-value operations:

- **PUT:** This operation allows you to insert data into the database. It accepts data in the form of strings and bit arrays and returns a boolean indicating the success of the operation.
- **GET:**  By providing a key in string format, you can retrieve a corresponding bit array from the database.
- **DELETE:** Supply the key in string format, and the operation returns a boolean indicating whether the deletion was successful.

#### Additional Put Operation for Probabilistic Structures
In addition to the standard Put operation, the database offers an extended Put operation specifically designed for integrating probabilistic data structures. These structures, such as [HyperLogLog](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/hll.go), [Count-Min Sketch](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/cms.go), [Bloom Filter](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/bloom-filter.go), and [SimHash](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/sim-hash.go), enable efficient approximate counting, inclusivity testing, and similarity computation.  
By offering this extra Put operation, the database can seamlessly incorporate probabilistic data structures, boosting applications that require rapid and memory-efficient approximate computations.

Those operations are implemented via two separate components: read path and write path.

### Caching Mechanism
Efficiency is enhanced through the incorporation of a caching mechanism. The database intelligently stores frequently accessed data in cache, reducing the need for repeated expensive disk reads. This feature accelerates data retrieval and contributes to improved overall performance.  
The [**Cache**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/cache.go) is implemented using the **Least Recently Used (LRU)** algorithm to store frequently accessed data in memory. Its size can be configured via configuration file.

### User Request Limitation
To ensure optimal resource utilization and fair access, the database implements a user request limitation mechanism using [**Token Bucket**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/tokenBucket.go) algorithm. This mechanism manages the rate at which users can send requests to the database, preventing excessive access and promoting balanced usage. All necessary parameters for the Token Bucket algorithm are stored within the system, and these settings are adjustable through configuration file. 

### Configuration
The project comes with a default configuration that simplifies the setup process. The default configuration includes settings related to storage, caching, and other database parameters. This configuration can be overwritten in [config file](https://github.com/coma007/No-SQL-Database/blob/main/config/config.json). 



## Write Path
The write path is responsible for handling the insertion and update of data into the database. It manages data storage, indexing, and maintaining consistency.

![Write Path](https://github.com/coma007/No-SQL-Database/blob/main/docs/write-path.png)

### Write-Ahead Log (WAL) and Memtable
The write process begins with the [**Write-Ahead Log (WAL)**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/wal.go), also known as the Commit Log. The WAL logs incoming data requests before they are committed to the main data storage. Upon approval from the WAL, the data item is added to the Memtable, which resides in memory. The WAL acts as a safeguard, ensuring data durability even in the event of system failures.  
The [**Memtable**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/memtable.go) uses a [**Skiplist**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/skip-list.go) data structure for efficient data management in memory. Skiplists enable fast insertion, deletion, and lookup operations, making them ideal for the Memtable's role.

### SSTable Creation
As the Memtable grows and reaches a predefined size, its contents are sorted by key and persisted as an [**SSTable (Sorted String Table)**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/sstable.go) on disk. An SSTable contains the actual data values and is accompanied by the following elements:

- **Data File:** This file contains the sorted data values from the Memtable.
- [**Filter (Bloom Filter):**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/bloom-filter.go) A Bloom filter is generated from all keys present in the data file. It is used to accelerate key existence checks.
- [**Index:**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/index.go) An index of all keys in the data file is created. The index consists of an array of items, each containing a key from the data file and the corresponding offset of that key in the data file.
- [**Summary:**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/summary.go) The summary contains file boundaries at the beginning and end of index items. Each item in the summary holds a key from the index file and its offset in the index file.

### LSM Tree Structure and Manual Compactions
Data organization is achieved using a [**Log-Structured Merge (LSM)**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/lsm-tree.go) tree structure. The LSM tree includes:

- **First Level (Memtable):** The Memtable is the initial storage for incoming data items in memory.
- **Subsequent Levels (SSTables):** As data accumulates, it is moved to SSTables on disk in sorted order. Manual compactions facilitate efficient movement of data from lower to higher levels in the LSM tree, optimizing storage and retrieval while managing disk space usage.

### Merkle Tree for Data Validation
A [**Merkle tree**](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/structures/merkle-tree.go) is constructed from all values within the data file. This tree ensures the integrity of data records by validating hash consistency throughout the structure. This mechanism guarantees the accuracy of stored data.

### Logical Deletes and In-Place Edits
Deletion operations in the database are logical, marking an item as deleted without immediate removal from data storage. Updates are performed in-place if the key is in memory (Memtable), otherwise, an update operation is treated as another put operation.

### Data Persistence and Storage Location
All durable data components, including Write-Ahead Log (WAL) logs, SSTable files and other database files, are stored within the [`kv-system/data`](https://github.com/coma007/No-SQL-Database/blob/main/kv-system/data) directory. This dedicated directory serves as the repository for maintaining data durability and consistency.


## Read Path
The read path focuses on retrieving data from the database efficiently. It involves querying, indexing, and caching mechanisms to optimize read operations.

### Flow of the Read Path

![Read Path](https://github.com/coma007/No-SQL-Database/blob/main/docs/read-path.png)

When a user initiates a GET request, the database executes the following sequence of steps:

1. **Cache Check:** The first step involves checking whether the requested key is already present in the Cache structure. If the key is found in the Cache, the database promptly returns the corresponding value to the user.

2. **Bloom Filter Verification:** If the key is not found in the Cache, the database checks the Bloom Filter. This probabilistic data structure is used to quickly assess whether the key might exist in the dataset. If the Bloom Filter indicates a potential presence, the database proceeds to the next step.

3. **Summary Range Check:** The database then examines the Summary structure to verify whether the key falls within its range. If the key is not in the Summary range, the system responds to the user with the information that the key is not present.

4. **Index Lookup:** If the key is within the Summary's range, the database uses the Index structure to determine the precise position of the key within the data.

5. **Data Retrieval:** Using the obtained position from the Index, the database accesses the Data section of the appropriate SSTable. It reads the value associated with the requested key.

6. **Cache Update and Response:** After locating the value, the database adds it to the Cache for potential future access. Finally, the requested value is returned to the user, successfully completing the read request.

### In-Memory Structures
Certain structures are loaded into memory for efficient querying:

- The Bloom Filter is loaded strictly into memory due to its small size and fast access requirements.
- The Summary structure is loaded into memory if the key is within its range, enabling quick range validation.
- Other structures, such as the Index and Data sections of SSTables, are accessed using seek operations, optimizing data retrieval.

The carefully orchestrated flow of the read path ensures that data is retrieved accurately and swiftly, maximizing the performance of the our NoSQL key-value database.


## Getting Started

### Prerequisites
Before you begin using the Key-Value Engine, ensure that you have the following prerequisites in place:

- **Go SDK:** You will need Go SDK version 1.20.2 or later installed on your system. You can download and install Go from the official [Go website](https://golang.org/dl/).

### Installation
To get started with the Key-Value Engine, follow these steps:

1. Clone the repository:
   ```sh
   git clone git@github.com:coma007/No-SQL-Database.git
   cd No-SQL-Database
   ```
2. Build the project:
   ```sh
   go build
   ```
3. Run the executable:
   ```sh
   ./Key-Value-Engine
   ```

#### Compatibility

The No-SQL-Database is compatible with all major operating systems, including Windows, Linux, and macOS.


### Dependencies

The No-SQL-Database has a single external dependency, which is automatically managed by Go's dependency management system. The project uses the [murmur3 package](github.com/spaolacci/murmur3) for efficient hashing. You don't need to manually install this dependency as Go's package manager will handle it for you during the build process.


## Usage

Explore the usage instructions bellow to understand how to interact with the Key-Value Engine and use its features. The provided menu system offers a user-friendly way to access various operations within the database.

### Menu

```
======= MENU =======
 1. PUT
 2. GET
 3. DELETE
 4. EDIT
--- HyperLogLog  ---
 5. CREATE HLL
 6. ADD TO HLL
 7. ESTIMATE HLL
-- CountMinSketch --
 8. CREATE CMS
 9. ADD TO CMS
10. QUERY IN CMS
--------------------
0. EXIT

Chose option from menu: 
```

Bellow are menu options explanations:

**Basic Operations**

- **PUT:** To store data in the database, select this option. You will be prompted to enter a key and its corresponding value.
- **GET:** Retrieve data from the database by providing the key. The associated value will be displayed.
- **DELETE:** Remove data from the database by specifying the key.
- **EDIT:** Modify existing data in the database. Provide the key to access the value, and then update it.

**HyperLogLog** 
- **CREATE HLL:** Create a HyperLogLog (HLL) structure with a given key. HLL is a probabilistic data structure used to estimate the number of distinct elements in a set.
- **ADD TO HLL:** Add an item (value) to an existing HLL structure specified by its key.
- **ESTIMATE HLL:** Estimate the cardinality (number of distinct elements) of an existing HLL structure specified by its key.

**CountMinSketch** 

- **CREATE CMS:** Create a CountMinSketch (CMS) structure with a given key. CMS is a probabilistic data structure used to estimate frequencies of items in a dataset.
-  **ADD TO CMS:** Add an item (value) to an existing CMS structure specified by its key
-  **QUERY IN CMS:** Query the estimated frequency of an item in an existing CMS structure specified by its key.

**Program Termination**
- **EXIT:** Exit the menu and terminate the program.

### Happy storing ! 💾 🗂️
