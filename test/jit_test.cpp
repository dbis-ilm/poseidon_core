/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "graph_db.hpp"
#include "graph_pool.hpp"

#include <set>
#include <iostream>
#include <boost/variant.hpp>

#ifdef USE_LLVM
#include "query_builder.hpp"
#include "query_proc.hpp"


std::string test_path = PMDK_PATH("jit_tst");

#ifdef USE_PMDK

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;
int i = 0;
nvm::pool_base prepare_pool() {
  test_path += std::to_string(i++);
  nvm::pool_base pop;
	if (access(test_path.c_str(), F_OK) != 0) {
    	pop = nvm::pool_base::create(test_path, "poseidon", PMEMOBJ_POOL_SIZE);
  	} else {
    	pop = nvm::pool_base::open(test_path, "poseidon");
  	}
	return pop;
}
#endif


TEST_CASE("Query the graph", "[jit_query_read]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_jit_graph");
#endif
  query_ctx ctx(graph);

  ctx.begin_transaction();

  int num_persons = 100;
  int num_books = 42;

  for (int i = 0; i < num_persons; i++) {
    graph->add_node("Person",
                              {{"name", std::any(std::string("John Doe"))},
                               {"age", std::any(42)},
                               {"group", std::any(int(i%4))},
                               {"id", std::any((int)i)},
                               {"num", std::any(uint64_t(1234567890123412))},
                               {"dummy1", std::any(std::string("Dummy"))},
                               {"dummy2", std::any(1.2345)}},
                              true);
  }

  for (int i = 0; i < num_books; i++) {
    auto b = graph->add_node("Book",
                              {{"title", std::any(std::string("Book Title"))},
                               {"year", std::any(1942)},
                               {"id", std::any((int)i)}},
                              true);
    graph->add_relationship(i, b, ":HAS_READ", {});
  }

  ctx.commit_transaction();
  
    SECTION("Scan all nodes for given label") {
        qcompiler queryEngine(ctx);

        result_set rss;
        auto q = query_builder(ctx).all_nodes("Person").collect(rss).get_pipeline();

        arg_builder args;
	      args.arg(1, "Person");
        args.arg(2, &rss);

        queryEngine.generate(q.plan_head(), false);
	      queryEngine.run(args);

        REQUIRE(rss.data.size() == (unsigned int)num_persons);
        //REQUIRE(boost::get<std::string>(rs.data[43][0]) == "Person[42]{age: 42, dummy1: \"Dummy\", dummy2: 1.2345, id: 42, name: \"John Doe\"}");
    }

    /*
   SECTION("Using an index to retrieve a certain node") {
        query_engine queryEngine(graph, 1, chunks);
        auto expr = IndexScan(Collect());
        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, "id");
        args.arg(3, 42);

        result_set rs;
        queryEngine.generate(expr, false);
        queryEngine.run(&rs, args);

        REQUIRE(rs.data.size() == 1);
    }*/
  
    arg_builder args;
    result_set rs;
    SECTION("Find a outgoing relationship from each Person node") {
        qcompiler queryEngine(ctx);
        auto expr = query_builder(ctx).all_nodes("Person").from_relationships(":HAS_READ").collect(rs).get_pipeline().plan_head();
        
        args.arg(1, "Person");
        args.arg(2, ":HAS_READ");
        args.arg(3, &rs);
        
        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_books);
        //REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "Person[0]{age: 42, dummy1: \"Dummy\", dummy2: 1.2345, id: 0, name: \"John Doe\", num: 1234567890123412}");
        //REQUIRE(boost::get<std::string>(rs.data.front()[1]) == "::HAS_READ[0]{}");

        queryEngine.cleanup();
        rs.data.clear();

        auto expr2 = query_builder(ctx).all_nodes("Book").to_relationships(":HAS_READ").collect(rs).get_pipeline().plan_head();
  
        args.arg(1, "Book");
        args.arg(2, ":HAS_READ");
        args.arg(3, &rs);

        queryEngine.generate(expr2, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_books);
        //REQUIRE(boost::get<std::string>(rs.data.front()[1]) == "::HAS_READ[0]{}");
        //REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "Book[100]{id: 0, title: \"Book Title\", year: 1942}");

        queryEngine.cleanup();
        rs.data.clear();

        auto expr3 = query_builder(ctx).all_nodes("Person").from_relationships(":HAS_READ").to_node("Book").collect(rs).get_pipeline().plan_head();
        args.arg(1, "Person");
        args.arg(2, ":HAS_READ");
        args.arg(4, "Book");
        args.arg(5, &rs);

        queryEngine.generate(expr3, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_books);
    }
   

    SECTION("Find a ingoing relationship from each Book node") {
        qcompiler queryEngine(ctx);
        result_set rs;

        auto expr = query_builder(ctx).all_nodes("Book").to_relationships(":HAS_READ").collect(rs).get_pipeline().plan_head();
        arg_builder args;
        args.arg(1, "Book");
        args.arg(2, ":HAS_READ");
        args.arg(3, &rs);

        
        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(true);
        //REQUIRE(rs.data.size() == num_books);
        //REQUIRE(boost::get<std::string>(rs.data[0][1]) == "Person[0]{age: 42, dummy1: \"Dummy\", dummy2: 1.2345, id: 0, name: \"John Doe\"}");
        //REQUIRE(boost::get<std::string>(rs.data[0][0]) == "Book[100]{title: \"Book Title\", year: 1942, id: 0}");
    }

    SECTION("Find the destination node for each relationship with the given label") {
        qcompiler queryEngine(ctx);
        result_set rs;
        auto expr = query_builder(ctx).all_nodes("Person").from_relationships(":HAS_READ").to_node("Book").collect(rs).get_pipeline().plan_head();
        
        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, ":HAS_READ");
        args.arg(4, "Book");
        args.arg(5, &rs);

        
        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_books);
    }
 
    SECTION("Find the source node for each relationship with the given label") {
        qcompiler queryEngine(ctx);
        result_set rs;
        auto expr = query_builder(ctx).all_nodes("Book").to_relationships(":HAS_READ").from_node("Person").collect(rs).get_pipeline().plan_head();

        arg_builder args;
        args.arg(1, "Book");
        args.arg(2, ":HAS_READ");
        args.arg(4, "Person");
        args.arg(5, &rs);

        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_books);
    }

    SECTION("Filter a node for a given property condition") {
        qcompiler queryEngine(ctx);
        result_set rs;
        auto expr = query_builder(ctx).all_nodes("Person").filter(EQ(Key(0, "id"), Int(42))).collect(rs).get_pipeline().plan_head();

        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, 42);
        args.arg(3, &rs);

        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == 1);
        //REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "Person[42]{age: 42, dummy1: \"Dummy\", dummy2: 1.2345, id: 42, name: \"John Doe\", num: 1234567890123412}");
    }

    SECTION("Apply a Projection to a tuple result") {
        qcompiler queryEngine(ctx);
        result_set rs;
        auto expr = query_builder(ctx).all_nodes("Person")
                      .filter(EQ(Key(0, "id"), Int(42)))
                        .project({{0, "name", result_type::string}})
                          .collect(rs).get_pipeline().plan_head();
        
        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, 42);
        args.arg(3, &rs);

        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == 1);
        //REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "John Doe");
    }


    SECTION("Apply a Projection on all tuple results") {
        qcompiler queryEngine(ctx);
        result_set rs;
        auto expr = query_builder(ctx).all_nodes("Person")
                        .project({{0, "name", result_type::string}})
                          .collect(rs).get_pipeline().plan_head();
        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, &rs);

        
        queryEngine.generate(expr, false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_persons);
        //REQUIRE(boost::get<std::string>(rs.data.back()[0]) == "John Doe");     
    }

    SECTION("CrossJoin two tuple results") {
        qcompiler queryEngine(ctx);

        result_set rss;        
        auto rhs = query_builder(ctx).all_nodes("Book").finish().get_pipeline();
        auto rhsp = rhs.plan_head();
        auto lhs = query_builder(ctx).all_nodes("Person").cross_join(rhs).collect(rs).get_pipeline().plan_head();

        arg_builder args;
        cross_joiner j;
        args.arg(1, "Person");
        args.arg(2, &j);
        
        queryEngine.generate(rhsp, false);
        queryEngine.run(args);
        
        args.arg(1, "Book");
        args.arg(2, &j);
        args.arg(3, &rss);

        queryEngine.generate(lhs, false);
        queryEngine.run(args);
        
        REQUIRE(rss.data.size() == (unsigned int)num_persons * (unsigned int)num_books);
    }

    REQUIRE(true);

#ifdef USE_PMDK
	nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
	pop.close();
	remove(test_path.c_str());
#else
   graph_pool::destroy(pool);
#endif

}

TEST_CASE("Test the Projection operator", "[jit_query_projection]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto pool = graph_pool::create(test_path);
  auto graph = pool->create_graph("my_jit_graph2");
#endif
  query_ctx ctx(graph);

  ctx.begin_transaction();

  int num_persons = 100;
  int num_books = 42;

  for (int i = 0; i < num_persons; i++) {
    auto p = graph->add_node("Person",
                              {{"name", std::any(std::string("John Doe"))},
                               {"age", std::any(42)},
                               {"group", std::any(int(i%4))},
                               {"id", std::any(i)},
                               {"num", std::any(uint64_t(1234567890123412))},
                               {"dummy1", std::any(std::string("Dummy"))},
                               {"dummy2", std::any(1.2345)}},
                              false);

    if (i < (int)num_books) {
      auto b = graph->add_node("Book",
                                {{"title", std::any(std::string("Book Title"))},
                                {"year", std::any(1942)},
                                {"id", std::any(i)}},
                                false);
      graph->add_relationship(p, b, ":HAS_READ", {});
    }
  }
  ctx.commit_transaction();

	// auto chunks = graph->get_nodes()->num_chunks(); 
  
    SECTION("Single Projection - string type") {
        qcompiler queryEngine(ctx);

        result_set rs;
        auto q = query_builder(ctx).all_nodes("Person").project({{0, "name", result_type::string}}).collect(rs).get_pipeline();

        arg_builder args;
	      args.arg(1, "Person");
        args.arg(2, &rs);

        
        queryEngine.generate(q.plan_head(), false);
	      queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_persons);
        REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "John Doe");
    }

    SECTION("Single Projection - int type") {
        qcompiler queryEngine(ctx);
        
        result_set rs;
        auto q = query_builder(ctx).all_nodes("Person").project({{0, "age", result_type::integer}}).collect(rs).get_pipeline();
        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, &rs);

        
        queryEngine.generate(q.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_persons);
        REQUIRE(boost::get<int>(rs.data.front()[0]) == 42);
    }

    SECTION("Single Projection - uint64_t type ") {
        qcompiler queryEngine(ctx);

        result_set rs;
        auto q = query_builder(ctx).all_nodes("Person").project({{0, "num", result_type::uint64}}).collect(rs).get_pipeline();

        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, &rs);

        queryEngine.generate(q.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_persons);
        REQUIRE(boost::get<uint64_t>(rs.data.front()[0]) == 1234567890123412);
    }

    SECTION("Multi Projection - string, int, uint64_t ") {
        qcompiler queryEngine(ctx);
        
        result_set rs;
        auto q = query_builder(ctx).all_nodes("Person").project({{0, "name", result_type::string}, 
                                            {0, "age", result_type::integer}, 
                                            {0, "num", result_type::uint64}}).collect(rs).get_pipeline();

        arg_builder args;
        args.arg(1, "Person");
        args.arg(2, &rs);

        queryEngine.generate(q.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == (unsigned int)num_persons);
        REQUIRE(boost::get<std::string>(rs.data.front()[0]) == "John Doe");
        REQUIRE(boost::get<int>(rs.data.front()[1]) == 42);
        REQUIRE(boost::get<uint64_t>(rs.data.front()[2]) == 1234567890123412);
    }

    SECTION("Project over several query results") {
        qcompiler queryEngine(ctx);
        result_set rss;
        auto r = query_builder(ctx).all_nodes("Book").finish().get_pipeline();
        auto r_expr = r.plan_head();

        arg_builder args;
        cross_joiner j;

        args.arg(1, "Person");
        args.arg(2, &j);

        queryEngine.generate(r_expr, false);
        queryEngine.run(args);

        auto l_expr = query_builder(ctx).all_nodes("Person")
                        .from_relationships(":HAS_READ")
                        .to_node("Book")
                        .cross_join(r)
                        .project({{0, "age", result_type::integer}, {0, "num", result_type::uint64}, 
                        {3, "name", result_type::string}})
                        .collect(rss).get_pipeline().plan_head();

        args.arg(1, "Person");
        args.arg(2, ":HAS_READ");
        args.arg(4, "Book");
        args.arg(5, &j);
        args.arg(6, &rss);

        queryEngine.generate(l_expr, false);
        queryEngine.run(args);        

        REQUIRE(rss.data.front().size() == 3);
    }

    SECTION("Limit results") {
        qcompiler queryEngine(ctx);
        result_set rss;
        auto r = query_builder(ctx).all_nodes("Book").limit(3).collect(rss).get_pipeline();
        auto r_expr = r.plan_head();

        arg_builder args;
        args.arg(1, "Book");
        args.arg(2, &rss);

        queryEngine.generate(r.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(rss.data.size() == 3);
    }

    SECTION("Sort results") {
        auto srtfct = [&](const qr_tuple &qr1, const qr_tuple &qr2) {
          return boost::get<int>(qr1[0]) > boost::get<int>(qr2[0]); 
        };

        qcompiler queryEngine(ctx);
        result_set rss;
        auto r = query_builder(ctx).all_nodes("Book")
                            .project({{0, "id", result_type::integer}})
                            .orderby(srtfct)
                            .collect(rss).get_pipeline();

        auto r_expr = r.plan_head();

        arg_builder args;
        args.arg(1, "Book");
        args.arg(2, &rss);

        queryEngine.generate(r.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(boost::get<int>(rss.data.front()[0]) == 83);
    }

    SECTION("Nested Loop Join") {
        qcompiler queryEngine(ctx);
        result_set rs;
        arg_builder args;

        nested_loop_joiner j(0);

        auto r_expr = query_builder(ctx).all_nodes("Book").finish().get_pipeline();
        args.arg(1, "Book");
        args.arg(2, &j);
        queryEngine.generate(r_expr.plan_head(), false);
        queryEngine.run(args);

        auto l_expr = query_builder(ctx).all_nodes("Person").nested_loop_join({0,0}, r_expr).collect(rs).get_pipeline();
        args.arg(1, "Book");
        args.arg(2, &j);
        args.arg(3, &rs);

        queryEngine.generate(l_expr.plan_head(), false);
        queryEngine.run(args);

        REQUIRE(rs.data.size() == 42);
        REQUIRE(rs.data.front().size() == 2);
    }

    SECTION("Argument generation") {
      qcompiler queryEngine(ctx);
      result_set rs;
      auto l_expr = query_builder(ctx).all_nodes("Person").from_relationships(":HAS_READ").to_node("Book").collect(rs).get_pipeline();

      queryEngine.generate(l_expr.plan_head(), false);
      queryEngine.run();

      REQUIRE(rs.data.size() != 0);
    }

/*
    SECTION("Hash Join") {
        qcompiler queryEngine(graph);
        auto r_expr = Scan("Person",  End(JOIN_OP::HASH_JOIN, 0));

        auto l_expr = Scan("Person", Join(JOIN_OP::HASH_JOIN, {0,0}, Collect(), r_expr));
        arg_builder args;
        joiner j;
        args.arg(4, &j);
        args.arg(1, "Person");
        args.arg(2, &j);
        args.arg(3, "Person");

        result_set rs;
        queryEngine.generate(l_expr, false);
        queryEngine.run(&rs, args);

        REQUIRE(rs.data.front().size() == 102);
    }

  SECTION("Test the compiled variable foreach relationship operator") {
        qcompiler queryEngine(graph);
        auto fev = Scan("Town", ForeachRship(RSHIP_DIR::FROM, {1, 2}, ":CONNECTED", 
                       Collect()));
        arg_builder args;
        args.arg(1, "Town");
        args.arg(2, ":CONNECTED");

        result_set rs;
        queryEngine.generate(fev, false);
        queryEngine.run(&rs, args);

        REQUIRE(rs.data.size() == 0);
    }   

    SECTION("Test Aggregation count") {
          qcompiler queryEngine(graph);
          auto fev = Scan("Person", Project({{0, "group", FTYPE::INT}}, GroupBy({0}, Aggr({{"count", 0}, {"avg", 0}}, Collect()))));
          arg_builder args;
          grouper g1;
          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);

          result_set rs;
          queryEngine.cleanup();
          queryEngine.generate(fev, false);
          queryEngine.run(&rs, args);
          
          REQUIRE(rs.data.size() == 4);
          REQUIRE(rs.data.front().size() == 3);
          REQUIRE(boost::get<int>(rs.data.front()[1]) == 35);
    }

    SECTION("Test Aggregation sum") {
          qcompiler queryEngine(graph);
          auto fev = Scan("Person", Project({{0, "group", FTYPE::INT}}, GroupBy({0}, Aggr({{"sum", 0}}, Collect()))));
          arg_builder args;
          grouper g1;
          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);

          result_set rs;
          queryEngine.cleanup();
          queryEngine.generate(fev, false);
          queryEngine.run(&rs, args);

          REQUIRE(rs.data.size() == 4);
          REQUIRE(rs.data.front().size() == 2);

          std::vector<int> expected_sum = {0, 72, 14, 45};

          int i = 0;
          for(auto & res : rs.data) {
            REQUIRE(boost::get<int>(res[1]) == expected_sum[i++]);
          }
    }

    SECTION("Test Aggregation avg") {
          qcompiler queryEngine(graph);
          auto fev = Scan("Person", Project({{0, "group", FTYPE::INT}}, GroupBy({0}, Aggr({{"avg", 0}}, Collect()))));
          arg_builder args;
          grouper g1;
          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);

          result_set rs;
          queryEngine.cleanup();
          queryEngine.generate(fev, false);
          queryEngine.run(&rs, args);

          REQUIRE(rs.data.size() == 4);
          REQUIRE(rs.data.front().size() == 2);
          std::vector<double> expected_avg = {0, 2, 1, 3};

          int i = 0;
          for(auto & res : rs.data) {
            REQUIRE(boost::get<double>(res[1]) == expected_avg[i++]);
          }
    }

    SECTION("Test Aggregation pcount") {
          qcompiler queryEngine(graph);
          auto fev = Scan("Person", Project({{0, "group", FTYPE::INT}}, GroupBy({0}, Aggr({{"pcount", 0}}, Collect()))));
          arg_builder args;
          grouper g1;
          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);

          result_set rs;
          queryEngine.cleanup();
          queryEngine.generate(fev, false);
          queryEngine.run(&rs, args);

          REQUIRE(rs.data.size () == 4);
    }

    SECTION("Test multiple aggregations") {
          qcompiler queryEngine(graph);
          auto fev = Scan("Person", 
            Project({{0, "group", FTYPE::INT}}, 
              GroupBy({0}, Aggr({{"count", 0}}, 
                GroupBy({0}, Aggr({{"count", 0}},
                  Collect()))))));
          arg_builder args;
          grouper g1;
          grouper g2;
          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);
          args.arg(4, &g2);
          args.arg(5, &g2);

          result_set rs;
          queryEngine.cleanup();
          queryEngine.generate(fev, false);
          queryEngine.run(&rs, args);

          REQUIRE(rs.data.front().size() == 2);
          REQUIRE(rs.data.size () == 4);
    }

    SECTION("Test multiple aggregations, continue pipeline") {
          qcompiler queryEngine(graph);
          auto gs = Scan("Person",
              GroupBy({0}, Aggr({{"count", 0}}, ForeachRship(RSHIP_DIR::FROM, ":CONNECTED", 0, 
                    Collect()))));
          arg_builder args;
          grouper g1;

          args.arg(1, "Person");
          args.arg(2, &g1);
          args.arg(3, &g1);
          args.arg(4, ":HAS_READ");

          result_set rs2;
          queryEngine.cleanup();
          queryEngine.generate(gs, false);
          queryEngine.run(&rs2, args);
          
          REQUIRE(rs2.data.front().size() == 3);
          REQUIRE(rs2.data.size() == 42);
    } 

    SECTION("Aggregation count with multiple tuple element keys") {
          qcompiler queryEngine(graph);
          auto gs = Scan("Person",
                      ForeachRship(RSHIP_DIR::FROM, ":HAS_READ", 0, 
                        Expand(EXPAND::OUT, "Book", 
                          ForeachRship(RSHIP_DIR::TO, ":HAS_READ", 2,
                            GroupBy({0, 1}, Aggr({{"count", 0}, {"count", 1}},
                              Collect()))))));
          arg_builder args;
          grouper g1;

          args.arg(1, "Person");
          args.arg(2, ":HAS_READ");
          args.arg(3, "Book");
          args.arg(4, ":HAS_READ");
          args.arg(5, &g1);
          args.arg(6, &g1);

          result_set rs2;
          queryEngine.cleanup();
          queryEngine.generate(gs, false);
          queryEngine.run(&rs2, args);

          REQUIRE(rs2.data.front().size() == 4);
          REQUIRE(rs2.data.size() == 42);
    }

    SECTION("Aggregation sum, avg with multiple tuple element keys") {
          qcompiler queryEngine(graph);
          auto gs = Scan("Person",
                      ForeachRship(RSHIP_DIR::FROM, ":HAS_READ", 0, 
                        Expand(EXPAND::OUT, "Book", 
                          ForeachRship(RSHIP_DIR::TO, ":HAS_READ", 2,
                            Project({{0, "group", FTYPE::INT}},
                            GroupBy({0}, Aggr({{"sum", 0}, {"avg", 0}},
                              Collect())))))));
          arg_builder args;
          grouper g1;

          args.arg(1, "Person");
          args.arg(2, ":HAS_READ");
          args.arg(3, "Book");
          args.arg(4, ":HAS_READ");
          args.arg(5, &g1);
          args.arg(6, &g1);

          result_set rs2;
          queryEngine.cleanup();
          queryEngine.generate(gs, false);
          queryEngine.run(&rs2, args);

          REQUIRE(rs2.data.front().size() == 3);
          //REQUIRE(rs2.data.size() == 2);
    }
*/
#ifdef USE_PMDK
	nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
	pop.close();
	remove(test_path.c_str());
#else
	graph_pool::destroy(pool);
#endif
}

#else

TEST_CASE("dummy test") {
  REQUIRE(true);
}
#endif