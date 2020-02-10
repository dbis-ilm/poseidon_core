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
    auto mahinda = graph->add_node(
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
  auto meera = graph->add_node(
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
  auto baruch = graph->add_node(
      // 4139|Baruch|Dego|male|1987-10-25|2010-01-28T01:38:17.824+0000|213.55.127.9|Internet Explorer
      "Person",
      {{"id", boost::any(4139)},
       {"firstName", boost::any(std::string("Baruch"))},
       {"lastName", boost::any(std::string("Dego"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1987-10-25"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-01-28 01:38:17.824"))},
       {"locationIP", boost::any(std::string("213.55.127.9"))},
       {"browser", boost::any(std::string("Internet Explorer"))}}); 
  auto fritz = graph->add_node(
      // 6597069777240|Fritz|Muller|female|1987-12-01|2010-08-24T20:13:46.569+0000|46.19.159.176|Safari
      "Person",
      {{"id", boost::any(65970697)},
       {"firstName", boost::any(std::string("Fritz"))},
       {"lastName", boost::any(std::string("Muller"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1987-12-01"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-08-24 20:13:46.569"))},
       {"locationIP", boost::any(std::string("46.19.159.176"))},
       {"browser", boost::any(std::string("Safari"))}});
  auto andrei = graph->add_node(
      // 10995116284808|Andrei|Condariuc|male|1982-02-04|2010-12-26T14:40:36.649+0000|92.39.58.88|Chrome
      "Person",
      {{"id", boost::any(10995116)},
       {"firstName", boost::any(std::string("Andrei"))},
       {"lastName", boost::any(std::string("Condariuc"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1982-02-04"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-12-26 14:40:36.649"))},
       {"locationIP", boost::any(std::string("92.39.58.88"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto ottoR = graph->add_node(
      // 32985348838375|Otto|Richter|male|1980-11-22|2012-07-12T03:11:27.663+0000|204.79.128.176|Firefox
      "Person",
      {{"id", boost::any(838375)},
       {"firstName", boost::any(std::string("Otto"))},
       {"lastName", boost::any(std::string("Richter"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1980-11-22"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-07-12 03:11:27.663"))},
       {"locationIP", boost::any(std::string("204.79.128.176"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto ottoB = graph->add_node(
      // 32985348833579|Otto|Becker|male|1989-09-23|2012-09-03T07:26:57.953+0000|31.211.182.228|Safari
      "Person",
      {{"id", boost::any(833579)},
       {"firstName", boost::any(std::string("Otto"))},
       {"lastName", boost::any(std::string("Becker"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1989-09-23"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-09-03 07:26:57.953"))},
       {"locationIP", boost::any(std::string("31.211.182.228"))},
       {"browser", boost::any(std::string("Safari"))}});
  auto Gobi = graph->add_node(
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
  auto Kelaniya = graph->add_node(
      // 1353|Kelaniya|http://dbpedia.org/resource/Kelaniya|city
      "Place",
      {
          {"id", boost::any(1353)},
          {"name", boost::any(std::string("Kelaniya"))},
          {"url",
           boost::any(std::string("http://dbpedia.org/resource/Kelaniya"))},
          {"type", boost::any(std::string("city"))},
      });
  auto post_656 = graph->add_node(
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
  auto post_13743895 = graph->add_node(
      // id|imageFile|creationDate|locationIP|browserUsed|language|content|length
      // 1374389534791|photo1374389534791.jpg|2011-10-05T14:38:36.019+0000|119.235.7.103|Firefox|||0	
      "Post",
      {
          {"id", boost::any(13743895)}, /* boost::any(std::string("1374389534791"))} */
          {"imageFile", boost::any(std::string("photo1374389534791.jpg"))}, /* String[0..1] */
          {"creationDate",
           boost::any(builtin::dtimestring_to_int("2011-10-05 14:38:36.019"))},
          {"locationIP", boost::any(std::string("119.235.7.103"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"language", boost::any(std::string("uz"))}, /* String[0..1] */ 
          {"content", boost::any(std::string(""))},
          {"length", boost::any(0)}
      });
  auto comment_12362343 = graph->add_node(
      // id|creationDate|locationIP|browserUsed|content|length 
      // 1236950581249|2011-08-17T14:26:59.961+0000|92.39.58.88|Chrome|yes|3
      "Comment",
      {
          {"id", boost::any(12362343)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2011-08-17 14:26:59.961"))},
          {"locationIP", boost::any(std::string("92.39.58.88"))},
          {"browser", boost::any(std::string("Chrome"))},
          {"content", boost::any(std::string("yes"))},
          {"length", boost::any(3)}
      });

  /**
   * Relationships for query interactive short #1
   */
  graph->add_relationship(mahinda, Kelaniya, ":isLocatedIn", {});
  graph->add_relationship(baruch, Gobi, ":isLocatedIn", {});

  /**
   * Relationships for query interactive short #2
   */

  /**
   * Relationships for query interactive short #3
  	
    Person.id|Person.id|creationDate
	933|4139|2010-03-13T07:37:21.718+0000
	933|6597069777240|2010-09-20T09:42:43.187+0000
	933|10995116284808|2011-01-02T06:43:41.955+0000
	933|32985348833579|2012-09-07T01:11:30.195+0000
	933|32985348838375|2012-07-17T08:04:49.463+0000
   */
  graph->add_relationship(mahinda, baruch, ":knows", {{"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-03-13 07:37:21.718"))}, {"relship_pr", boost::any(std::string("true"))}});
  graph->add_relationship(mahinda, fritz, ":knows", {{"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-09-20 09:42:43.187"))}, {"relship_pr", boost::any(std::string("true"))}});
  graph->add_relationship(mahinda, andrei, ":knows", {{"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-01-02 06:43:41.955"))}, {"relship_pr", boost::any(std::string("true"))}});
  graph->add_relationship(mahinda, ottoB, ":knows", {{"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))}, {"relship_pr", boost::any(std::string("true"))}});
  graph->add_relationship(mahinda, ottoR, ":knows", {{"creationDate", /* testing date order */
        boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))}, {"relship_pr", boost::any(std::string("true"))}});

  /**
   * Relationships for query interactive short #4

	id|imageFile|creationDate|locationIP|browserUsed|language|content|length
	1374389534791|photo1374389534791.jpg|2011-10-05T14:38:36.019+0000|119.235.7.103|Firefox|||0	
   */

  /**
   * Relationships for query interactive short #5

  	Comment.id|Person.id
  	1236950581249|10995116284808
   */
  graph->add_relationship(comment_12362343, andrei, ":hasCreator", {});

  /**
   * Relationships for query interactive short #6
   */

  /**
   * Relationships for query interactive short #7
   */

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

  SECTION("query interactive short #3") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("833579"), query_result("Otto"),
         query_result("Becker"), query_result("1346980290"), query_result("true")});
    expected.data.push_back(
        {query_result("838375"), query_result("Otto"),
         query_result("Richter"), query_result("1346980290"), query_result("true")});
    expected.data.push_back(
        {query_result("10995116"), query_result("Andrei"),
         query_result("Condariuc"), query_result("1293950621"), query_result("true")});
    expected.data.push_back(
        {query_result("65970697"), query_result("Fritz"),
         query_result("Muller"), query_result("1284975763"), query_result("true")});
    expected.data.push_back(
        {query_result("4139"), query_result("Baruch"),
         query_result("Dego"), query_result("1268465841"), query_result("true")});
    
    ldbc_is_query_3(graph, rs);
    //std::cout << rs;

    REQUIRE(rs == expected);
  }

  SECTION("query interactive short #4") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("1317825516"),
         query_result("photo1374389534791.jpg")});
    

    ldbc_is_query_4(graph, rs);
    std::cout << rs;

    REQUIRE(rs == expected);
  }

  SECTION("query interactive short #5") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("10995116"),
         query_result("Andrei"),
         query_result("Condariuc")});
    

    ldbc_is_query_5(graph, rs);
    //std::cout << rs;

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