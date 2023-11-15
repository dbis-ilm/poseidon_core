# Poseidon Graph Database

Poseidon is a graph database system originally designed for persistent memory, but supports disk-based data, too. The data model used in Poseidon is the property graph model where nodes and relationships have labels (type names) and properties (key-value pairs). This module `poseidon_core` provides the implementation for the storage and transaction manager as well as the query engine consisting of a parser for a simple algebraic query language, a query interpreter, and a LLVM-based query compiler.

## Installation

Simply, clone the repository, create a build directory and run the build tools `cmake` and `make`:

```bash
mkdir build; cd build
cmake ..
make
```

## Docker

Alternatively, you can build a docker container with all the necessary tools and a compiled version of Poseidon. Simply, use the provided `Dockerfile`:

```bash
docker build . -t poseidon-dev
docker run -it poseidon-dev
```

## Using Poseidon

Poseidon provides a command line interface called `pcli` for managing databases and executing queries. The following options are supported:

Option | Description
-------|------------
  `-v` [ `--verbose` ]     | verbose - show debug output
  `-d` [ `--db` ] arg      | specifies the database name (required)
  `-p` [ `--pool` ] arg    | the path to the PMem pool (for PMDK) or the directory containing all databases
  `-o` [ `--output` ] arg  | dumps the graph to the given file (in DOT format)
  `--strict`             | strict mode - assumes that all columns contain values of the same type
  `--delimiter` arg  | specifies the delimiter character
  `-f` [ `--format` ] arg  | specifies CSV format: n4j | gtpc | ldbc
  `--import-path` arg    | specifies the directory containing import files
  `--import` arg         | imports the files in CSV format either `nodes:<node-type>:<filename>` or `relationships:<rship-type>:<filename>`
  `-q` [ `--query` ] arg   | executes the query from the given file
  `-s` [ `--shell` ]       | starts the interactive shell
  `--qmode` arg          | specifies the query compile mode: interp (default), llvm, or adapt

In the following example, we create a new database and load the graph data from CSV files in Neo4j file format. Note, that all files representing a database have to imported with a single invocation of `pcli` as in the following example:

```bash
./build/pcli --pool demo --db testdb  -f n4j --delimiter , \
     --import nodes:Movie:./test/movies.csv \
     --import nodes:Actor:./test/actors.csv \
     --import relationships:./test/roles.csv 
```

Alternatively, CVS files in a plain CSV format can be also imported.

After creating the database, we can execute queries, e.g.

```bash
./build/pcli --pool demo --db testdb
poseidon> NodeScan()
poseidon> NodeScan('Movie')
poseidon> Filter($0.title == 'Inception (2010)', NodeScan('Movie'))
poseidon> Expand(IN, 'Actor', ForeachRelationship(TO, 'PLAYED_IN', NodeScan('Movie')))
```

Poseidon supports a simple algebraic query notation in the following form of a query expression:

```
query-op(args, query-expr)
```

A query is evaluated from the inside out: *query-expr* is first evaluated and produces the input for *query-op*. Each operator in a query expression adds its result (node, relationship, value(s)) to the result of its input expression. For example, an expression such as

```
ForeachRelationship(FROM, 'isLocatedIn', 
   NodeScan('Person'))
```
produces a result with two columns: column #0 contains the `Person` node, column #1, the outgoing relationships of type `isLocatedIn`.  In expressions such as `Filter` or `Project` one can refer to these columns by `$0`, `$1` etc., e.g.:

```
Project([$0.firstName:string, $0.lastName:string, $2.id:uint64, $2.name:string],
    Match((p1:Person {id: 933})-[:isLocatedIn]->(p2:Place))
```

The following query operators are supported:

Operator | Parameter | Example | Description 
---------| ----------|---------|------------
NodeScan | node type (optional) | `NodeScan()`<br>`NodeScan('Person')` | Scans the node table and returns all nodes of the optionally given type.
IndexScan | node type, property, key | `IndexScan('Person', 'id', 933)` | Performs an index lookup and returns all nodes of the given type that satisfy the predicate condition
Filter | filter expression, input expression | `Filter($0.id == 42, NodeScan('Person'))` | Processes the input list of nodes and rships produced by input expression *query-expr* and returns all tuples satisfying the given condition. In the expressions, the input columns are denoted by $0, $1, $2 etc. 
ForeachRelationship | TO or FROM or ALL, RelationshipType, input | `ForeachRelationship(FROM, 'isLocatedIn', NodeScan('Person'))` | Traverses all incoming or outgoing or both relationships of the given type
Expand | `IN` or `OUT`, node type, input expression | `Expand(OUT, 'Place', ForeachRelationship(FROM, 'isLocatedIn', NodeScan('Person')))` | Gets all the source or destination nodes of the given type. Used after `ForeachRelationship` operator.
Match | path pattern, input expression | `Match((p1:Person {id: 933})-[:isLocatedIn]->(p2:Place))` | Evaluates the given path pattern (in Cypher's ASCII art notation) and returns the matching nodes/relationships. In fact, this operator is internally translated to a sequence of corresponding `ForeachRelationship`-`Expand` expressions.
Project | [ projection list ], input expr | `Project([$0.firstName:string, $0.lastName:string, $0.id:uint64], NodeScan('Person'))` | Projects query results based on the given projection list
Limit | number of tuples, input expr | `Limit(10, NodeScan('Person'))` | Limits the input list to the given number of tuples
Join | condition expression, input1, input2 || Computes a join from the given inputs
LeftOuterJoin | condition expression, input1, input2 || Computes a left outer join from the given inputs
Sort |  sort function, input || Orders tuples according to the sorting function
Aggregate |  [ AggregateType list  ], input || 
GroupBy | [ GroupKey list ], [ AggregateType list  ], input || Groups all tuples based on grouping key(s) and apply aggregate functions
Union | [ query list ], input expr || Combines the tuples of multiple queries
Create | (n:NodeType { key: val, ...} ), input || Creates a new node from the literals or the input
Create | ($1)-[r:RelationshipType { key: val, ...} ]->($2), input || Creates a new relationship from the literals or the input
RemoveNode | input expression || Deletes the nodes returned from the input expression. If a node is still connected by a relationship then the query is aborted.
RemoveRelationship | input expression || Deletes the relationships returned from the input expression.
DetachNode | input expression || Deletes the nodes returned from the input expression together with all their relationships. 

In addition to executing queries, `pcli` provides several utility commands:

```
poseidon> help
Available commands:
	help                             show this help
	string s                         display the dictionary code of the string s
	code c                           display the string of the dictionary code c
	load <library>                   load the given shared library
	stats                            print database statistics
	sync                             ensure that all pages are written to disk
	create index <label> <property>  create an index for the given label/property
	drop index <label> <property>    delete the index for the given label/property
	@file                            execute the query stored in the given file
	explain <query-expr>             execute the given query and print the plan
	<query-expr>                     execute the given query
	print node|rship <id>            print the raw data of the node/relationship with given id
```

## Query implementation

Queries - or better query plans - can be also directly implemented in C++ by using the `query` class. This class provides methods to construct a query plan from a set of separate operators. Poseidon provides a push-based query engine, i.e. the query plan starts with scans. The following example shows an implementation of LDBC interactive short query #1:

```c++
namespace pj = builtin;

auto q = query(gdb)
          .nodes_where("Person", "id", [&](auto &p) { return p.equal(933); })
          .from_relationships(":isLocatedIn")
          .to_node("Place")
          .project(
              {PExpr_(0, pj::string_property(res, "firstName")),
               PExpr_(0, pj::string_property(res, "lastName")),
               PExpr_(0, pj::int_to_datestring(pj::int_property(res, "birthday"))),
               PExpr_(0, pj::string_property(res, "locationIP")),
               PExpr_(0, pj::string_property(res, "browser")),
               PExpr_(2, pj::int_property(res, "id")),
               PExpr_(0, pj::string_property(res, "gender")),
               PExpr_(0, pj::int_to_datetimestring(
                   pj::int_property(res, "creationDate"))) })
          .print();
  q.start();
```

### Poseidon Python API

In addition, there is a thin wrapper library for providing a Python API. This API allows creating and opening graph databases, importing CVS data, and specifying query execution plans. The latter reflects the C++ query class (see above).

In the following Python example the database `imdb` is created by loading the IMDB data from three files.

```python
import poseidon

# create a new graph database
graph = poseidon.graph_db()
graph.create("imdb")

# we need a mapping table for mapping logical node ids to ids used for
# creating relationships
m = graph.mapping()

# import nodes and relationships for the IMDB database
n = graph.import_nodes("Movies", "imdb-data/movies.csv", m)
print(n, "movies imported.")
n = graph.import_nodes("Actor", "imdb-data/actors.csv", m)
print(n, "actors imported.")
n = graph.import_relationships("imdb-data/roles.csv", m)
print(n, "roles imported.")
```

After creating the graph database, it can be queried:

```python
q = poseidon.query(graph) \
    .all_nodes("Movies") \
    .print()

q.start()
```

## Storage structure

Poseidon stores nodes and relationships in separate vectors where each vector is implemented as a chunked vector stored on disk pages, i.e. a linked list of array of fixed size. Furthermore, properties are stored separately in a third vector. Whereas for nodes and relationships each object is represented by its own record, properties belonging to the same node or relationship are stored in batches of five properties per record. Note, that strings a compressed via dictionary compression and neither stored directly in nodes, relationships, or properties.

## Transaction Processing

For transaction processing Poseidon implements a multiversion timestamp ordering (MVTO) protocol. Here, the most recent committed version is always stored persistently while dirty versions (nodes/relationships which are currently inserted or updated) as well as outdated versions are stored in a dirty list in volatile memory.

## Publications

The architecture of the persistent memory-based version is described **[here](https://doi.org/10.5441/002/edbt.2021.05)**. This engine is the foundation for other research works, e.g.
* [query compilation](https://doi.org/10.1007/s10619-023-07430-4),
* [query recovery](https://doi.org/10.1145/3465998.3466011),
* [processing-in-memory](https://doi.org/10.1145/3592980.3595323),
* [temporal graph processing](https://doi.org/10.1007/978-3-031-42914-9_8).
