#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <iostream>
#include <boost/variant.hpp>
#include <boost/algorithm/string.hpp>

#include "config.h"
#include "defs.hpp"
#include "qop.hpp"
#include "qop_scans.hpp"
#include "qop_relationships.hpp"
#include "graph_db.hpp"
#include "graph_pool.hpp"

#ifdef USE_LLVM
#include "query_proc.hpp"

const std::string test_path = PMDK_PATH("qcomp_tst");

TEST_CASE("Transform a given query into graph algebra", "[qcomp]") {
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_qc_graph");

    query_ctx ctx(graph);
    query_proc qlc(ctx);

    SECTION("Transform a scan query into a valid graph algebra expression") {
        std::string scan_query = "NodeScan('Person')";
        result_set rs;
        auto op = qlc.prepare_query(scan_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);
        auto scanop = std::dynamic_pointer_cast<scan_nodes>(op);
        REQUIRE(boost::equals(scanop->label, "Person"));
    }

    SECTION("Transform a ForeachRship FROM query into a valid graph algebra expression") {
        std::string scan_query = "ForeachRelationship(FROM, ':HAS_READ', NodeScan('Person'))";
        auto op = qlc.prepare_query(scan_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);
        
        auto fe = op->subscriber();
        REQUIRE(fe->type_ == qop_type::foreach_rship);

        auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(fe);
        REQUIRE(boost::equals(fe_op->label, ":HAS_READ"));
        REQUIRE(fe_op->dir_ == RSHIP_DIR::FROM);
    }

    SECTION("Transform a ForeachRship TO query into a valid graph algebra expression") {
        std::string scan_query = "ForeachRelationship(TO, ':HAS_READ', NodeScan('Person'))";
        auto op = qlc.prepare_query(scan_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);
        
        auto fe = op->subscriber();
        REQUIRE(fe->type_ == qop_type::foreach_rship);

        auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(fe);
        REQUIRE(boost::equals(fe_op->label, ":HAS_READ"));
        REQUIRE(fe_op->dir_ == RSHIP_DIR::TO);
    }

    SECTION("Transform a ExpandIn query into a valid graph algebra expression") {
        std::string scan_query = "Expand(IN, 'Book', ForeachRelationship(FROM, ':HAS_READ', NodeScan('Person')))";
        auto op = qlc.prepare_query(scan_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);
        
        auto fe = op->subscriber();
        REQUIRE(fe->type_ == qop_type::foreach_rship);

        auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(fe);
        REQUIRE(boost::equals(fe_op->label, ":HAS_READ"));
        REQUIRE(fe_op->dir_ == RSHIP_DIR::FROM);

        auto exp = fe->subscriber();
        REQUIRE(exp->type_ == qop_type::expand);

        auto exp_op = std::dynamic_pointer_cast<expand>(exp);
        REQUIRE(exp_op->dir_ == EXPAND::IN);

        auto hl = exp_op->subscriber();
        auto hl_op = std::dynamic_pointer_cast<node_has_label>(hl);
        REQUIRE(hl_op->label == "Book");
    }

    SECTION("Transform a ExpandOut query into a valid graph algebra expression") {
        std::string scan_query = "Expand(OUT, 'Book', ForeachRelationship(FROM, ':HAS_READ', NodeScan('Person')))";
        auto op = qlc.prepare_query(scan_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);
        
        auto fe = op->subscriber();
        REQUIRE(fe->type_ == qop_type::foreach_rship);

        auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(fe);
        REQUIRE(boost::equals(fe_op->label, ":HAS_READ"));
        REQUIRE(fe_op->dir_ == RSHIP_DIR::FROM);

        auto exp = fe->subscriber();
        REQUIRE(exp->type_ == qop_type::expand);

        auto exp_op = std::dynamic_pointer_cast<expand>(exp);
        REQUIRE(exp_op->dir_ == EXPAND::OUT);

        auto hl = exp_op->subscriber();
        auto hl_op = std::dynamic_pointer_cast<node_has_label>(hl);
        REQUIRE(hl_op->label == "Book");
    }

    SECTION("Transform a Filter query into a valid graph algebra expression") {
        std::string filter_query = "Filter($2.age == 42, NodeScan('Person'))";
        auto op = qlc.prepare_query(filter_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);

        auto filter = op->subscriber();
        REQUIRE(filter->type_ == qop_type::filter);

        auto fil_op = std::dynamic_pointer_cast<filter_tuple>(filter);
        auto fexp = fil_op->ex_;

        REQUIRE(fexp->name_ == "EQ");

        auto bin_expr = std::dynamic_pointer_cast<binary_predicate>(fexp);
        REQUIRE(bin_expr->fop_ == FOP::EQ);

        auto lhs = bin_expr->left_;
        REQUIRE(lhs->ftype_ == FOP_TYPE::KEY);

        auto rhs = bin_expr->right_;
        REQUIRE(rhs->ftype_ == FOP_TYPE::INT);
    }


    SECTION("Transform a Projection query into a valid graph algebra expression") {
        std::string prj_query = "Project([$0.age:int, $42.name:string], NodeScan('Person'))";
        auto op = qlc.prepare_query(prj_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);

        auto prj = op->subscriber();
        REQUIRE(prj->type_ == qop_type::project);

        auto prj_op = std::dynamic_pointer_cast<projection>(prj);
        auto pexp = prj_op->prexpr_;
        REQUIRE(pexp.size() == 2);

        auto prj1 = pexp.front();
        auto prj2 = pexp.back();

        REQUIRE(prj1.id == 0);
        REQUIRE(prj1.key == "age");
        REQUIRE(prj1.type == result_type::integer);

        REQUIRE(prj2.id == 42);
        REQUIRE(prj2.key == "name");
        REQUIRE(prj2.type == result_type::string);
    }

    SECTION("Transform a Filter and Projection into valid graph algebra expression") {
        std::string prj_query = "Project([$0.age:int, $42.name:string], Filter($2.age == 42 ,NodeScan('Person')))";
        auto op = qlc.prepare_query(prj_query).front().plan_head();

        REQUIRE(op->type_ == qop_type::scan);

        auto filter = op->subscriber();
        REQUIRE(filter->type_ == qop_type::filter);

        auto fil_op = std::dynamic_pointer_cast<filter_tuple>(filter);
        auto fexp = fil_op->ex_;

        REQUIRE(fexp->name_ == "EQ");

        auto bin_expr = std::dynamic_pointer_cast<binary_predicate>(fexp);
        REQUIRE(bin_expr->fop_ == FOP::EQ);

        auto lhs = bin_expr->left_;
        REQUIRE(lhs->ftype_ == FOP_TYPE::KEY);

        auto rhs = bin_expr->right_;
        REQUIRE(rhs->ftype_ == FOP_TYPE::INT);

        auto prj = filter->subscriber();
        REQUIRE(prj->type_ == qop_type::project);

        auto prj_op = std::dynamic_pointer_cast<projection>(prj);
        auto pexp = prj_op->prexpr_;
        REQUIRE(pexp.size() == 2);

        auto prj1 = pexp.front();
        auto prj2 = pexp.back();

        REQUIRE(prj1.id == 0);
        REQUIRE(prj1.key == "age");
        REQUIRE(prj1.type == result_type::integer);

        REQUIRE(prj2.id == 42);
        REQUIRE(prj2.key == "name");
        REQUIRE(prj2.type == result_type::string);
    }

  graph_pool::destroy(pool);    
}

#else
TEST_CASE("dummy test qcomp") {
    REQUIRE(true);
}
#endif