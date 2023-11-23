#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include <strstream>

#include "query_proc.hpp"

TEST_CASE("Testing the poseidon query processor", "[query_proc]") {
    query_ctx qctx;
    query_proc qp(qctx);
    std::ostrstream os;

    SECTION("NodeScan") {
        auto plan = qp.prepare_query("NodeScan('Person')");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Limit") {
        auto plan = qp.prepare_query("Limit(20, NodeScan('Person'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "limit([20]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("CrossJoin") {
        auto plan = qp.prepare_query("Limit(20, CrossJoin(NodeScan('Comment'), NodeScan('Person')))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "limit([20]) - { in=0 | out=0 | time=0s }\n└── cross_join() - { in=0 | out=0 | time=0s }\n    ├── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n    └── scan_nodes([Comment]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Union") {
        auto plan = qp.prepare_query("Union(NodeScan('Comment'), NodeScan('Person'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "union_all() - { in=0 | out=0 | time=0s }\n├── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Comment]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Project") {
        auto plan = qp.prepare_query("Project([$1.attr:datetime, $0.attr:string, $0.attr:int], NodeScan('Person'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "project([ $1.func $0.func $0.func ]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Expand") {
        auto plan = qp.prepare_query("Expand(OUT, 'Person', ForeachRelationship(FROM, 'knows', 1, 3, NodeScan('Person')))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "node_has_label([Person]) - { in=0 | out=0 | time=0s }\n└── get_to_node(){ in=0 | out=0 | time=0s }\n    └── foreach_variable_from_relationship([knows, (1,3)]) - { in=0 | out=0 | time=0s }\n        └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Aggregate") {
        auto plan = qp.prepare_query("Aggregate([count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "aggregate([ count(0.attr) sum(1.attr) ]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Nodes]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("GroupBy") {
        auto plan = qp.prepare_query("GroupBy([$0.attr2:string], [count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "group_by([ count($0.attr) sum($1.attr) ],[$0.attr2 ]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Nodes]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Match") {
        auto plan = qp.prepare_query("Match((p1:Person)-[:knows]->(p2:Person))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "node_has_label([Person]) - { in=0 | out=0 | time=0s }\n└── get_to_node(){ in=0 | out=0 | time=0s }\n    └── foreach_from_relationship([knows]) - { in=0 | out=0 | time=0s }\n        └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Match2") {
        auto plan = qp.prepare_query("Match((p1:Person { id: 42 } )-[:knows]->(p2:Person))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "node_has_label([Person]) - { in=0 | out=0 | time=0s }\n└── get_to_node(){ in=0 | out=0 | time=0s }\n    └── foreach_from_relationship([knows]) - { in=0 | out=0 | time=0s }\n        └── filter([$0.id==42]) - { in=0 | out=0 | time=0s }\n            └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Match3") {
        auto plan = qp.prepare_query("Match((p1:Person { id: 42, name: 'Peter' } )-[:knows]->(p2:Person))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "node_has_label([Person]) - { in=0 | out=0 | time=0s }\n└── get_to_node(){ in=0 | out=0 | time=0s }\n    └── foreach_from_relationship([knows]) - { in=0 | out=0 | time=0s }\n        └── filter([$0.id==42&&$0.name==Peter]) - { in=0 | out=0 | time=0s }\n            └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }

   SECTION("Filter") {
        auto plan = qp.prepare_query("Filter($0.attr > 42, NodeScan('Nodes'))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "filter([$0.attr>42]) - { in=0 | out=0 | time=0s }\n└── scan_nodes([Nodes]) - { in=0 | out=0 | time=0s }\n");
    }

    SECTION("Create Node") {
        auto plan = qp.prepare_query("Create((n:Label { name1: 'Val1', name2: 42 }))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "create_node([Label], {name1: \"Val1\", name2: 42})\n");
    }

    SECTION("Create Relationship") {
        auto plan = qp.prepare_query("Create(($0)-[r:knows { creationDate: '2010-07-21' } ]->($1), CrossJoin(Filter($0.id == 1, NodeScan('Person')), Filter($0.id == 2, NodeScan('Person'))))");
        plan.print_plan(os);
        os << std::ends;
        REQUIRE(std::string(os.str()) == "create_relationship(0->1, [knows], {creationDate: \"2010-07-21\"})\n└── cross_join() - { in=0 | out=0 | time=0s }\n    ├── filter([$0.id==2]) - { in=0 | out=0 | time=0s }\n    │   └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n    └── filter([$0.id==1]) - { in=0 | out=0 | time=0s }\n        └── scan_nodes([Person]) - { in=0 | out=0 | time=0s }\n");
    }
}