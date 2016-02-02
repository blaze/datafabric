# Data Fabric

This is a design document for a distributed data warehouse that leverages the POSIX
shared memory API -- see http://man7.org/linux/man-pages/man7/shm_overview.7.html -- to avoid unnecessary copies. Our intent is ultimately to provide typed binary storage, distributed across nodes and accessible by many languages.

Interoperability across languages will require a C++ library -- potentially C++11 so as to make use of `<future>`, as well as other asynchronous functionality, in the C++ standard library. An appropriate C API will also be required.

However, our short term objective is to build a "minimal viable proof of concept" in Python. We will do
that by leveraging what already exists in the [Blaze ecosystem](http://blaze.pydata.org/) as much as possible.  From there we will work to ensure access from multiple languages and port potential bottlenecks to C++. 


Scope
-----

The initial demo version of data fabric will simply support data allocation and access in
a distributed setting.  It will not provide compute functionality.  Nor will it provide serialization functionality.   It will provide CREATE/LOAD, READ, and DELETE functionality for names in the in-memory store and will return a memory address given the name

The entire purpose of our "minimal viable demo" is to scope out an acceptable API for the
data fabric. And, also, to educate the development team about how to build with distributed.

The project will not involve distributed values or data-structures.   Such a distributed data-structure could live on top of this data-fabric using sharding and multiple keys.


Distributed Framework
---------------------

For the proof of concept we will build our data fabric on top of the [Distributed](https://github.com/blaze/distributed) library in Python. We envision a simple key-value storage, where a key (some string that tags the distributed data) maps to a list of (node, shared-memory name, offset) containing the key.   

As part of the prototype, we will define a data-fabric yellow-pages (YP) class that will be the center-point of the data-fabric data fabric.   It will learn heavily from the Center class in [Distributed](https://github.com/blaze/distributed). This YP class will be responsible for CREATING shared-memory blocks on all the nodes of the Distributed cluster.    It will also keep track of where all the variables stored in the system are stored.  

Datafabric will be a script that connects to an already running distributed cluster, allocates a certain amount of total shared-memory evenly distributed across the nodes of the cluster starting with one block per node and keeps track of the names as they are created and deleted and will be the place to find out where specific names are listed

MANAGE
======

allocate(num_megabytes) -> [(node, shared_id),...]

Allocate new memory for the data-fabric with room for num_megabytes.   This will create the
shared-memory blocks owned by the system.


CREATE (COPY)
=============

There are two (three) ways to get data into shared memory: 

   1) Read from Disk
   2) Copy from other memory
   3) possibly memory mapping 

insert(variable_name, documentation, tp, bytes)

variable_name -- string to key the variable
documentation -- a small string describing the variable
tp -- an ndt.type from [DyND](https://github.com/libdynd/libdynd).
where bytes is an open-file, a memory-view, or a memory-mapped file. 

This command will find an empty place to put the bytes and track them in the YP class

READ
====

get_list()
get_locations(variable_name) --> list of (node, shared_id, offset)
    node is the ip address of the machine
    shared_id is the id of the shared-memory block on that machine
    offset is the byte offset into the shared-memory block where the start of the data is located

get_ptr(node, shared_id, offset) -> ndt.type, memory pointer (ctype)
    
    Question?  Should the ndt.type be stored in the YP Class or near the bytes themselves (i.e. at the front of them?)  So that memory pointers are self-describing. 

    To start let's store the ndt.type in the YP Class. 

DELETE
======

delete(variable_name) 

 removes the variable name from the data-fabric and allows the memory it is using to be re-used.

defragment()

 free up contiguous space by moving variables around in the shared-segments. 

deallocate(node_id, shared_id)

 Remove the shared_memory block from the system.


Eventually, at the C++ stage, we could move away from the concept of a distributed center towards a model where the stored information is more distributed using things like the CRUSH algorithm. 


Possible data uses
==========================

Data Abstraction
----------------

Variables in our data fabric will be write-once.  Variables in the data-fabric can be represented in the YPClass with a lightweight class either in Python (using `ctypes`)
or in Cython. An example of what variable could look like, in Cython, is below.

```
cdef class variable:
    const char *name # the name of the variable
    const key_t id # the key of the shared-memory

    type _tp # the ndt.type that describes the data
    char *_metadata # the low-level datashape, i.e. DyND metadata
    char *_data # a pointer to the data as return by the POSIX API function `mmap`

    def __cinit__(self, tp):
        # default-initialize the metadata (low-level datashape)
        # allocate the data in shared memory, retrieve the pointer via `mmap`

    property type:
        def __get__(self):
            # return the ndt.type

    property metadata:
        def __get__(self):
            # return a ctype pointer to metadata (low-level datashape) serialized as a char *

    property data:
        def __get__(self):
            # return a ctype pointer to data as a char *
```

We will pre-allocate shared-memory blocks  avoid the problem of not ha limited number of shared-memory blocks are available in Linux.   


Example Workflow
----------------

Here is an example workflow of how the demo version of Data Fabric could work.

- Setup distributed in Python.
- Create a data fabric YP Class and allocate shared memory
- Create a `ndt.type` that describes your data, e.g. `ndt.type("40 * 20 * float64")`
- Store (or read from disk) the data into a variable in the fabric.
- At this point, the `YP Class` takes care of the mapping to the local worker nodes, and each local
worker takes care of the allocation.

- Later on, we can ask the `YP Class` for a list of which nodes contain the object with name `x`. This will return a list of the nodes and their local names of `x`.

It is then up to the user to apply operations to this read-only data and store intermediate results back into the fabric.
