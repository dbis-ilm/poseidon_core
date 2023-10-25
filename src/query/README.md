# Poseidon Query Processor

This directory contains the code for the query processor of the Poseidon graph database. We support both 
* an interpreter (ahead of time) that connects and executes pre-compiled operators plus an interpreter for expressions
* as well as an LLVM-based JIT compiler.

## Directory structure

* `parser`: classes for the query parser
* `expr`: classes for representing expressions in filters etc.
* `plan_op`: classes representing plan operators create from the AST and used by the interpreter and code generator
* `interp`: code of the query interpreter (ahead of time compiler) both for query plans and expressions
* `codegen`: code for generating LLVM-based IR code from the query plan
* `qmain`: driver classes for the query processor
* `utils`: utility classes such as a thread pool, code for query profiling, and printing plans
