#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include "catch.hpp"
#include "defs.hpp"
#include "ldbc.hpp"
#include "query.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create("/mnt/pmem0/ldbc/ldbc_test", "",
                                    PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

graph_db_ptr create_graph(
#ifdef USE_PMDK
    nvm::pool_base &pop
#endif
) {
#ifdef USE_PMDK
  graph_db_ptr graph;
  nvm::transaction::run(pop, [&] { graph = p_make_ptr<graph_db>(); });
#else
  auto graph = p_make_ptr<graph_db>();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  // TODO: create LDBC SNB data
  auto p1 = graph->add_node(
      // id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed
      // 933|Mahinda|Perera|male|1989-12-03|2010-02-14T15:32:10.447+0000|119.235.7.103|Firefox
      "Person",
      {{"id", boost::any(933)},
       {"firstName", boost::any(std::string("Mahinda"))},
       {"lastName", boost::any(std::string("Perera"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1989-12-03"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-02-14 15:32:10.447"))},
       {"locationIP", boost::any(std::string("119.235.7.103"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto p2 = graph->add_node(
      // 10027|Meera|Rao|female|1982-12-08|2010-01-22T19:59:59.221+0000|49.249.98.96|Firefox
      "Person",
      {{"id", boost::any(10027)},
       {"firstName", boost::any(std::string("Meera"))},
       {"lastName", boost::any(std::string("Rao"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1982-12-08"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-01-22 19:59:59.221"))},
       {"locationIP", boost::any(std::string("49.249.98.96"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto c1 = graph->add_node(
      // id|name|url|type
      // 129|Gobichettipalayam|http://dbpedia.org/resource/Gobichettipalayam|city
      "Place",
      {
          {"id", boost::any(129)},
          {"name", boost::any(std::string("Gobichettipalayam"))},
          {"url", boost::any(std::string(
                      "http://dbpedia.org/resource/Gobichettipalayam"))},
          {"type", boost::any(std::string("city"))},
      });
  auto c2 = graph->add_node(
      // 1353|Kelaniya|http://dbpedia.org/resource/Kelaniya|city
      "Place",
      {
          {"id", boost::any(1353)},
          {"name", boost::any(std::string("Kelaniya"))},
          {"url",
           boost::any(std::string("http://dbpedia.org/resource/Kelaniya"))},
          {"type", boost::any(std::string("city"))},
      });
  auto c3 = graph->add_node(
      // 656|Firefox|2010-01-22 19:59:59.221|119.235.7.103|Bla Bla|7
      "Post",
      {
          {"id", boost::any(656)},
          {"browserUsed", boost::any(std::string("Firefox"))},
          {"creationDate",
           boost::any(builtin::dtimestring_to_int("2010-01-22 19:59:59.221"))},
          {"locationIP", boost::any(std::string("119.235.7.103"))},
          {"content", boost::any(std::string("Bla Bla"))},
          {"length", boost::any(7)},
      });

  graph->add_relationship(p1, c2, ":isLocatedIn", {});
  graph->add_relationship(p2, c1, ":isLocatedIn", {});

#ifdef USE_TX
  graph->commit_transaction();
#endif

  return graph;
}

TEST_CASE("Testing LDBC interactive short queries", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  SECTION("query interactive short #1") {
    result_set rs, expected;

    // firstName, lastName, birthday, locationIP, browser, placeId, gender
    // Mahinda|Perera|1989-12-03|119.235.7.103|Firefox|1353|male
    expected.data.push_back(
        {query_result("Mahinda"), query_result("Perera"),
         query_result("1989-12-03"), query_result("119.235.7.103"),
         query_result("Firefox"), query_result("1353"), query_result("male"),
         query_result("2010-02-14 15:32:10")});

    ldbc_is_query_1(graph, rs);

    REQUIRE(rs == expected);
  }

  SECTION("query update #1") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[0]{birthday: 628646400, browser: \"Firefox\", "
                      "creationDate: 1266161530, firstName: \"Mahinda\", "
                      "gender: \"male\", id: 933, lastName: \"Perera\", "
                      "locationIP: \"119.235.7.103\"}"),
         query_result("Post[4]{browserUsed: \"Firefox\", content: \"Bla Bla\", "
                      "creationDate: 1264190399, id: 656, length: 7, "
                      "locationIP: \"119.235.7.103\"}"),
         query_result(":LIKES[2]{creationDate: 1266161530}")});
    ldbc_iu_query_2(graph, rs);
    REQUIRE(rs == expected);
  }

#ifdef USE_TX
  graph->abort_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove("/mnt/pmem0/ldbc/ldbc_test");
#endif
}