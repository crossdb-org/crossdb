INMEM Test

in-tmpfs Test

===============================================================================
LMDB
===============================================================================
MDB_NOSYNC
MDB_WRITEMAP

	/** don't fsync after commit */
#define MDB_NOSYNC		0x10000
	/** read only */
#define MDB_RDONLY		0x20000
	/** don't fsync metapage after commit */
#define MDB_NOMETASYNC		0x40000
	/** use writable mmap */
#define MDB_WRITEMAP		0x80000
	/** use asynchronous msync when #MDB_WRITEMAP is used */
#define MDB_MAPASYNC		0x100000
	/** tie reader locktable slots to #MDB_txn objects instead of to threads */
#define MDB_NOTLS		0x200000
	/** don't do any locking, caller must manage their own locks */
#define MDB_NOLOCK		0x400000
	/** don't do readahead (no effect on Windows) */
#define MDB_NORDAHEAD	0x800000
	/** don't initialize malloc'd memory before writing to datafile */
#define MDB_NOMEMINIT	0x1000000

===============================================================================
SQLite
===============================================================================
PRAGMA schema.synchronous = 0 | OFF | 1 | NORMAL | 2 | FULL | 3 | EXTRA;
PRAGMA schema.journal_mode = DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF
PRAGMA schema.locking_mode = NORMAL | EXCLUSIVE
rc = sqlite3_open(":memory:", &db);
h2sql SQLite 3.36.0.3, configured to use WAL and with synchronous=NORMAL

PRAGMA journal_mode = WAL
PRAGMA locking_mode = EXCLUSIVE

===============================================================================
STL:  map, unordered_map
===============================================================================

===============================================================================
GlibC: tsearch, hsearch
===============================================================================

The GNU C Library (glibc) provides several search APIs that can be used to perform operations on data structures such as binary search trees and hash tables. Some of the search APIs provided by glibc are:

bsearch: This function performs a binary search on a sorted array. It takes a pointer to the array, the number of elements in the array, the size of each element, a pointer to the key to search for, and a pointer to a comparison function as arguments. The comparison function is used to compare the key with the elements in the array.

lfind: This function searches for an element in an array using a linear search. It takes a pointer to the array, the number of elements in the array, the size of each element, a pointer to the key to search for, and a pointer to a comparison function as arguments. The comparison function is used to compare the key with the elements in the array.

lsearch: This function searches for an element in an array using a linear search. If the element is not found, it is inserted into the array. It takes a pointer to the array, the number of elements in the array, the size of each element, a pointer to the key to search for, and a pointer to a comparison function as arguments. The comparison function is used to compare the key with the elements in the array.

tsearch: This function creates and searches a binary search tree. It takes a pointer to the key to insert or search for, a pointer to a pointer to the root of the tree, and a pointer to a comparison function as arguments. The comparison function is used to compare the keys in the tree.

tfind: This function searches for a key in a binary search tree. It takes a pointer to the key to search for, a pointer to the root of the tree, and a pointer to a comparison function as arguments. The comparison function is used to compare the keys in the tree.

tdelete: This function deletes a key from a binary search tree. It takes a pointer to the key to delete, a pointer to a pointer to the root of the tree, and a pointer to a comparison function as arguments. The comparison function is used to compare the keys in the tree.

hcreate: This function creates a hash table. It takes the size of the hash table as an argument.

hsearch: This function searches for a key in a hash table. It takes a pointer to the key to search for, an action to perform (such as FIND or ENTER), a pointer to a pointer to the hash table, and a pointer to a ENTRY structure as arguments.

hdestroy: This function destroys a hash table. It takes a pointer to the hash table as an argument.

These search APIs can be used to perform efficient and flexible operations on various data structures in C programs.



The GNU C Library (glibc) provides several collection data structures that can be used to store and manipulate collections of data in C programs. Some of the collection data structures provided by glibc are:

Array List: A dynamic array implementation that provides random access to elements in the list. Elements can be added to the end of the list, removed from the end, and the size of the list can be changed dynamically.

Linked List: A linked list implementation that provides a flexible way to add and remove elements from the list. Each element in the list is stored in a separate node, which contains a pointer to the next node in the list.

Binary Heap: A binary heap implementation that provides efficient priority queue operations. The binary heap can be used to store a collection of elements, where each element has a priority. The highest priority element can be retrieved from the heap in constant time.

Binary Search Tree: A binary search tree implementation that provides efficient search, insertion, and deletion operations. Each node in the tree contains a key and a value, and the keys are sorted in ascending order.

Hash Table: A hash table implementation that provides efficient lookup, insertion, and deletion operations. A hash table is a collection of key-value pairs, where the keys are hashed to an index in the table.

These collection data structures provided by glibc can be used to store and manipulate different types of data in C programs. They are designed to provide efficient and flexible operations on collections of data, making them useful in a variety of applications.


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <search.h>

#define TABLE_SIZE 100

struct person {
    char name[50];
    int age;
};

int main()
{
    struct person people[] = {
        {"Alice", 25},
        {"Bob", 32},
        {"Charlie", 19},
        {"David", 47}
    };

    size_t num_people = sizeof(people) / sizeof(people[0]);

    ENTRY item, *found;
    struct hsearch_data table = {0};

    if (hcreate_r(TABLE_SIZE, &table) == 0) {
        fprintf(stderr, "Failed to create hash table\n");
        return 1;
    }

    for (size_t i = 0; i < num_people; i++) {
        item.key = people[i].name;
        item.data = &people[i];
        if (hsearch_r(item, ENTER, &found, &table) == 0) {
            fprintf(stderr, "Failed to insert person %s into hash table\n", people[i].name);
        }
    }

    // Search for a person by name
    item.key = "Bob";
    if ((found = hsearch_r(item, FIND, &table)) != NULL) {
        struct person *p = found->data;
        printf("%s is %d years old\n", p->name, p->age);
    } else {
        printf("Person not found\n");
    }

    // Destroy the hash table
    hdestroy_r(&table);

    return 0;
}



#include <stdio.h>
#include <stdlib.h>
#include <search.h>

int main()
{
    ENTRY e, *ep;
    int i;

    hcreate(4);

    e.key = "foo";
    e.data = (void*) 42;
    hsearch(e, ENTER);

    e.key = "bar";
    e.data = (void*) 99;
    hsearch(e, ENTER);

    e.key = "baz";
    e.data = (void*) 78;
    hsearch(e, ENTER);

    ep = hsearch((ENTRY){.key="bar"}, FIND);
    if (ep != NULL) {
        printf("Found %s -> %d\n", ep->key, (int)(long)ep->data);
    } else {
        printf("Not found\n");
    }

    hdestroy();
    return 0;
}


#include <stdio.h>
#include <stdlib.h>
#include <search.h>

struct node {
    int value;
    struct node *left;
    struct node *right;
};

int compare(const void *a, const void *b)
{
    int va = *(const int*)a;
    int vb = *(const int*)b;
    return (va < vb) ? -1 : (va > vb);
}

void print_tree(const void *nodep, VISIT value, int level)
{
    if (value == postorder || value == leaf) {
        struct node *node = *(struct node**)nodep;
        printf("%*s%d\n", level*4, "", node->value);
    }
}

int main()
{
    struct node *root = NULL;

    int arr[] = {5, 2, 8, 1, 3, 7, 9};
    size_t n = sizeof(arr) / sizeof(arr[0]);

    for (size_t i = 0; i < n; i++) {
        int *node = &arr[i];
        tsearch(node, (void**)&root, compare);
    }

    twalk(root, print_tree);

    return 0;
}
