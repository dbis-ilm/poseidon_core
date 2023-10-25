#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>

#include "query_proc.hpp"

TEST_CASE("Testing the poseidon parser", "[parser]") {
    query_ctx qctx;
    query_proc qp(qctx);

    SECTION("NodeScan") {
        REQUIRE(qp.parse_("NodeScan()"));
        REQUIRE(qp.parse_("NodeScan('Person')"));
        REQUIRE(qp.parse_("NodeScan(['Comment', 'Post'])"));
    }

    SECTION("IndexScan") {
        REQUIRE(qp.parse_("IndexScan('Person', 'id', 42)"));
        REQUIRE(qp.parse_("IndexScan('Person', 'name', 'Dave')"));
        REQUIRE_FALSE(qp.parse_("IndexScan()"));
        REQUIRE_FALSE(qp.parse_("IndexScan('Person')"));
    }

    SECTION("Filter") {
        REQUIRE(qp.parse_("Filter($0.id == 42, NodeScan('Person'))"));
        REQUIRE(qp.parse_("Filter($0.num > 1.0, NodeScan('Person'))"));
        REQUIRE(qp.parse_("Filter($0.num > 1.0 && $1.id == 13, NodeScan('Person'))"));
    }

    SECTION("Limit") {
        REQUIRE(qp.parse_("Limit(20, NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("Limit(Hallo, NodeScan('Person'))"));
    }

    SECTION("Project") {
        REQUIRE(qp.parse_("Project([$1.attr:datetime, $0.attr:string, $0.attr:int], NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("Project([$1.attr:something], NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("Project($1.attr:something, NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("Project([$1.attr], NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("Project([])"));
        REQUIRE(qp.parse_("Project([udf::year($0.creationDate:datetime), $0.length:int], NodeScan(['Post', 'Comment']))"));
    }

    SECTION("Join") {
        REQUIRE(qp.parse_("CrossJoin(NodeScan('Order'), NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("CrossJoin(NodeScan('Order'))"));

        REQUIRE(qp.parse_("HashJoin($0.id == $1.id, NodeScan('Order'), NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("HashJoin(NodeScan('Order'))"));
    }

    SECTION("Aggregate") {
        REQUIRE(qp.parse_("Aggregate([count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))"));
        REQUIRE_FALSE(qp.parse_("Aggregate(NodeScan('Nodes'))"));
    }

    SECTION("GroupBy") {
        REQUIRE(qp.parse_("GroupBy([$0:int], [count($1:string), sum($2:int)], Project([$0.attr1:int, $0.attr2:string, $0.attr3:int], NodeScan('Nodes')))"));
        REQUIRE(qp.parse_("GroupBy([$0.attr:int], [count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))"));
        REQUIRE(qp.parse_("GroupBy([$0.attr:int, $0.attr:datetime], [count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))"));
        REQUIRE_FALSE(qp.parse_("GroupBy([count($0.attr:string), sum($1.attr:int)], NodeScan('Nodes'))"));
        REQUIRE_FALSE(qp.parse_("GroupBy(NodeScan('Nodes'))"));
    }

    SECTION("ForeachRelationship") {
        REQUIRE(qp.parse_("ForeachRelationship(TO, ':knows', NodeScan('Person'))"));
        REQUIRE(qp.parse_("ForeachRelationship(TO, ':knows', $1, NodeScan('Person'))"));
        REQUIRE(qp.parse_("ForeachRelationship(FROM, ':knows', NodeScan('Person'))"));
        REQUIRE(qp.parse_("ForeachRelationship(FROM, ':knows', 1, 5, NodeScan('Person'))"));
        REQUIRE(qp.parse_("ForeachRelationship(ALL, ':knows', 1, 5, NodeScan('Person'))"));
        REQUIRE(qp.parse_("ForeachRelationship(ALL, ':knows', 1, 5, $1, NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("ForeachRelationship(':knows', NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("ForeachRelationship(SOME, ':knows', NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("ForeachRelationship(TO, NodeScan('Person'))"));
        REQUIRE_FALSE(qp.parse_("ForeachRelationship(TO, 'knows')"));
    }

    SECTION("Expand") {
        REQUIRE(qp.parse_("Expand(OUT, 'Person', ForeachRelationship(TO, ':knows', NodeScan('Person')))"));
        REQUIRE_FALSE(qp.parse_("Expand('Person', ForeachRelationship(TO, ':knows', NodeScan('Person')))"));
        REQUIRE_FALSE(qp.parse_("Expand(OUT, ForeachRelationship(TO, ':knows', NodeScan('Person')))"));
        REQUIRE_FALSE(qp.parse_("Expand(OUT, 'Person')"));
    }

    SECTION("Match") {
        REQUIRE_FALSE(qp.parse_("Match((p1:Person)-[:knows]->(p2:Person)-[:has])"));
        REQUIRE_FALSE(qp.parse_("Match([:knows]->(p2:Person))"));
    
        REQUIRE(qp.parse_("Match((p1:Person)-[:knows]->(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person)-[:knows]-(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person)-[:knows*0..5]-(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person)-[:knows*0..]-(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person { id: 42})-[:knows]->(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person { id: 42})<-[:knows]-(p2:Person))"));
        REQUIRE(qp.parse_("Match((p1:Person)-[:knows]->(p2:Person)-[:has]->(o:Order))"));
        REQUIRE(qp.parse_("Match((p1:Person)<-[:knows]->(p2:Person)-[:has]->(o:Order))"));
    }

    SECTION("NonSense") {
        REQUIRE_FALSE(qp.parse_("Blubs('Person')"));
    }

    SECTION("Sort") {
        REQUIRE(qp.parse_("Sort([$0:datetime DESC, $1:uint64 ASC], Project([$0.birthday:datetime, $0.id:uint64], NodeScan('Person')))"));
    }
    
    SECTION("Create") {
        REQUIRE(qp.parse_("Create((n:Label { name1: 'Val1', name2: 42 }))"));
        REQUIRE(qp.parse_("Create((p:Person))"));
        REQUIRE(qp.parse_("Create((n:Label { name1: 'Val1', name2: 42 }), NodeScan('Person'))"));

        REQUIRE(qp.parse_("Create(($0)-[r:knows]->($1))"));
        REQUIRE(qp.parse_("Create(($0)-[r:knows { creationDate: '2010-07-21' } ]->($1), CrossJoin(Filter($0.id == 1, NodeScan('Person')), Filter($0.id == 2, NodeScan('Person'))))"));
    }
}