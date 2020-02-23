#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <boost/algorithm/string.hpp>

#include "catch.hpp"
#include "config.h"
#include "defs.hpp"
#include "ldbc.hpp"
#include "query.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

const std::string test_path = poseidon::gPmemPath + "ldbc_test";

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create(test_path, "", PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif

const std::string snb_dir("/tmp/snb-data/social_network/");

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
  return graph;
}

void load_snb_data(graph_db_ptr &graph, 
                    std::vector<std::string> &node_files,
                    std::vector<std::string> &rship_files){
  auto delim = '|';
  graph_db::mapping_t mapping;
  
  if (!node_files.empty()){
    std::cout << "\n######## \n# NODES \n######## \n";

    std::vector<std::size_t> num_nodes(node_files.size());
    auto i = 0;
    for (auto &file : node_files){
      std::vector<std::string> fp;
      boost::split(fp, file, boost::is_any_of("/"));
      assert(fp.back().find(".csv") != std::string::npos);
      auto pos = fp.back().find("_0_0");
      auto label = fp.back().substr(0, pos);
      if (label[0] >= 'a' && label[0] <= 'z')
        label[0] -= 32;

      num_nodes[i] = graph->import_nodes_from_csv(label, file, delim, mapping);
      std::cout << num_nodes[i] << " \"" << label << "\" node objects imported \n";
      i++;
    }
  }

  if (!rship_files.empty()){
    std::cout << "\n \n################ \n# RELATIONSHIPS \n################ \n";
    
    std::vector<std::size_t> num_rships(rship_files.size());
    auto i = 0;
    for (auto &file : rship_files){
      std::vector<std::string> fp;
      boost::split(fp, file, boost::is_any_of("/"));
      assert(fp.back().find(".csv") != std::string::npos);
      std::vector<std::string> fn;
      boost::split(fn, fp.back(), boost::is_any_of("_"));
      auto label = ":" + fn[1];

      num_rships[i] = graph->import_relationships_from_csv(file, delim, mapping);
      std::cout << num_rships[i] << " (" << fn[0] << ")-[\"" << label << "\"]->(" 
                                  << fn[2] << ") relationship objects imported \n";
      i++;
    }
  }
}

TEST_CASE("LDBC Interactive Short Query 1", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "place_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_isLocatedIn_place_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("Mahinda"), query_result("Perera"),
        query_result("1989-12-03"), query_result("119.235.7.103"),
        query_result("Firefox"), query_result("1353"), query_result("male"),
        query_result("2010-02-14T15:32:10.447+0000")});

  ldbc_is_query_1(graph, rs);

  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 2", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "post_0_0.csv",
                                          snb_dir + "comment_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "post_hasCreator_person_0_0.csv",
                                          snb_dir + "comment_hasCreator_person_0_0.csv",
                                          snb_dir + "comment_replyOf_post_0_0.csv",
                                          snb_dir + "comment_replyOf_comment_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("2199029789250"), query_result("fine"), query_result("2012-09-13T06:10:24.610+0000"),
      query_result("2199029789233"), query_result("4398046519022"), 
      query_result("Zheng"), query_result("Xu")});
  expected.data.push_back(
      {query_result("2199023895468"), query_result("About Pope John Paul II, vanni Paolo II,About Alfred, "
        "Lord Tennyson, t / T"), query_result("2012-09-12T10:03:59.850+0000"),
      query_result("2199023895464"), query_result("21990232556018"), 
      query_result("K."), query_result("Kumar")});
  expected.data.push_back(
      {query_result("2061590835573"), query_result("About Catherine the Great, ge of 67. She was born "
        "in Stettin, PomeraniaAbout Bangladesh"), query_result("2012-08-20T04:48:28.061+0000"),
      query_result("2061590835571"), query_result("17592186052552"), 
      query_result("A."), query_result("Nair")});
  expected.data.push_back(
      {query_result("2061590836130"), query_result("thanks"), query_result("2012-07-28T00:28:44.339+0000"),
      query_result("2061590836122"), query_result("15393162794737"), 
      query_result("Wilson"), query_result("Agudelo")});
  expected.data.push_back(
      {query_result("2061590835519"), query_result("LOL"), query_result("2012-07-25T05:05:52.401+0000"),
      query_result("2061590835503"), query_result("26388279068563"), 
      query_result("Patrick"), query_result("Ambane")});
  expected.data.push_back(
      {query_result("2061590835517"), query_result("yes"), query_result("2012-07-25T02:09:45.010+0000"),
      query_result("2061590835503"), query_result("26388279068563"), 
      query_result("Patrick"), query_result("Ambane")});
  expected.data.push_back(
      {query_result("962079207711"), query_result("roflol"), query_result("2011-04-12T14:02:40.962+0000"),
      query_result("962079207708"), query_result("13194139542623"), 
      query_result("Andre"), query_result("Dia")});
  expected.data.push_back(
      {query_result("962079207716"), query_result("yes"), query_result("2011-04-12T11:45:11.947+0000"),
      query_result("962079207708"), query_result("13194139542623"), 
      query_result("Andre"), query_result("Dia")});
  expected.data.push_back(
      {query_result("962079207714"), query_result("I see"), query_result("2011-04-11T21:22:03.107+0000"),
      query_result("962079207708"), query_result("13194139542623"), 
      query_result("Andre"), query_result("Dia")});
  expected.data.push_back(
      {query_result("962079207709"), query_result("About Jerry Lewis, r his slapstick humor in film, "
        "television, stage and radio."), query_result("2011-04-11T21:01:46.154+0000"),
      query_result("962079207708"), query_result("13194139542623"), 
      query_result("Andre"), query_result("Dia")});

  ldbc_is_query_2(graph, rs);

  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 3", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_knows_person_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("32985348833579"), query_result("Otto"),
        query_result("Becker"), query_result("2012-09-07T01:11:30.195+0000")});
  expected.data.push_back(
      {query_result("32985348838375"), query_result("Otto"),
        query_result("Richter"), query_result("2012-07-17T08:04:49.463+0000")});
  expected.data.push_back(
      {query_result("10995116284808"), query_result("Andrei"),
        query_result("Condariuc"), query_result("2011-01-02T06:43:41.955+0000")});
  expected.data.push_back(
      {query_result("6597069777240"), query_result("Fritz"),
        query_result("Muller"), query_result("2010-09-20T09:42:43.187+0000")});
  expected.data.push_back(
      {query_result("4139"), query_result("Baruch"),
        query_result("Dego"), query_result("2010-03-13T07:37:21.718+0000")});
  
  ldbc_is_query_3(graph, rs);

  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 4", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "post_0_0.csv"};

  std::vector<std::string> rship_files = {};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("2011-10-05T14:38:36.019+0000"),
        query_result("photo1374389534791.jpg")});
  

  ldbc_is_query_4(graph, rs);

  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 5", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "comment_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "comment_hasCreator_person_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("10995116284808"),
        query_result("Andrei"),
        query_result("Condariuc")});
  

  ldbc_is_query_5(graph, rs);

  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 6", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "post_0_0.csv",
                                          snb_dir + "comment_0_0.csv", snb_dir + "forum_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "comment_replyOf_post_0_0.csv",
                                          snb_dir + "comment_replyOf_comment_0_0.csv", 
                                          snb_dir + "forum_containerOf_post_0_0.csv",
                                          snb_dir + "forum_hasModerator_person_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("37"),
        query_result("Wall of Hồ Chí Do"),
        query_result("4194"),
        query_result("Hồ Chí"),
        query_result("Do")});
  expected.data.push_back(
      {query_result("37"),
        query_result("Wall of Hồ Chí Do"),
        query_result("4194"),
        query_result("Hồ Chí"),
        query_result("Do")});

  ldbc_is_query_6(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Short Query 7", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "comment_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "comment_hasCreator_person_0_0.csv",
                                          snb_dir + "comment_replyOf_comment_0_0.csv",};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.data.push_back(
      {query_result("1649267442217"),
        query_result("maybe"),
        query_result("2012-01-10T06:31:18.533+0000"),
        query_result("15393162795439"),
        query_result("Lomana Trésor"),
        query_result("Kanam"),
        query_result("false")});

  expected.data.push_back(
      {query_result("1649267442213"),
        query_result("I see"),
        query_result("2012-01-10T14:57:10.420+0000"),
        query_result("19791209307382"),
        query_result("Amin"),
        query_result("Kamkar"),
        query_result("false")});

  ldbc_is_query_7(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 1", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "place_0_0.csv",
                                          snb_dir + "tag_0_0.csv", snb_dir + "organisation_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_isLocatedIn_place_0_0.csv",
                                          snb_dir + "person_hasInterest_tag_0_0.csv",
                                          snb_dir + "person_studyAt_organisation_0_0.csv",
                                          snb_dir + "person_workAt_organisation_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Person[35387]{birthday: 348883200, browserUsed: \"Safari\", "
                    "creationDate: \"2011-01-11 01:51:21.746\", email: \"\"new1@email1.com\", \"new@email2.com\"\", "
                    "firstName: \"New\", gender: \"female\", id: \"9999999999999\", language: \"\"zh\", \"en\"\", "
                    "lastName: \"Person\", locationIP: \"1.183.127.173\"}"),
      query_result("Place[10397]{id: \"505\", name: \"Artux\", type: \"city\", url: \"http://dbpedia.org/resource/Artux\"}"),
      query_result("::isLocatedIn[268661]{}"),
      query_result("Tag[11413]{id: \"61\", name: \"Kevin_Rudd\", url: \"http://dbpedia.org/resource/Kevin_Rudd\"}"),
      query_result("::hasInterest[268662]{}"),
      query_result("Organisation[29645]{id: \"2213\", name: \"Anhui_University_of_Science_and_Technology\", type: \"university\", "
                    "url: \"http://dbpedia.org/resource/Anhui_University_of_Science_and_Technology\"}"),
      query_result("::studyAt[268663]{classYear: 2001}"),
      query_result("Organisation[28347]{id: \"915\", name: \"Chang'an_Airlines\", type: \"company\", "
                    "url: \"http://dbpedia.org/resource/Chang'an_Airlines\"}"),
      query_result("::workAt[268664]{workFrom: 2001}") });
  
  ldbc_iu_query_1(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 2", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "post_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_likes_post_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Person[0]{birthday: 628646400, browserUsed: \"Firefox\", "
                    "creationDate: \"2010-02-14T15:32:10.447+0000\", firstName: \"Mahinda\", "
                    "gender: \"male\", id: \"933\", lastName: \"Perera\", "
                    "locationIP: \"119.235.7.103\"}"),
        query_result("Post[532552]{browserUsed: \"Firefox\", content: \"About Alexander I of Russia, "
                      "lexander tried to introduce liberal reforms, while in the second half\", "
                      "creationDate: \"2012-08-20T09:51:28.275+0000\", id: \"2061587303627\", language: \"uz\", "
                      "length: 98, locationIP: \"14.205.203.83\"}"),
        query_result("::likes[751677]{creationDate: \"2010-02-14 15:32:10.447\"}")});
  
  ldbc_iu_query_2(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 3", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv", snb_dir + "comment_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_likes_comment_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Person[4807]{birthday: 467251200, browserUsed: \"Chrome\", "
                    "creationDate: \"2010-01-02T06:04:55.320+0000\", firstName: \"Emperor of Brazil\", "
                    "gender: \"female\", id: \"1564\", lastName: \"Silva\", "
                    "locationIP: \"192.223.88.63\"}"),
        query_result("Comment[10108]{browserUsed: \"Firefox\", content: \"About Louis I of Hungary, "
                    "ittle lasting political results. Louis is theAbout Union of Sou\", "
                    "creationDate: \"2012-01-19T11:39:51.385+0000\", id: \"1649267442250\", length: 89, "
                    "locationIP: \"85.154.120.237\"}"),
        query_result("::likes[1438418]{creationDate: \"2012-01-23 08:56:30.617\"}")});
  
  ldbc_iu_query_3(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 4", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "forum_0_0.csv", snb_dir + "person_0_0.csv", 
                                          snb_dir + "tag_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "forum_hasModerator_person_0_0.csv",
                                            snb_dir + "forum_hasTag_tag_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Forum[116464]{creationDate: \"2010-01-02 06:05:05.320\", id: \"53975\", title: "
                    "\"Wall of Emperor of Brazil Silva\"}"),
      query_result("Person[95299]{birthday: 467251200, browserUsed: \"Chrome\", "
                    "creationDate: \"2010-01-02T06:04:55.320+0000\", firstName: \"Emperor of Brazil\", "
                    "gender: \"female\", id: \"1564\", lastName: \"Silva\", "
                    "locationIP: \"192.223.88.63\"}"), 
      query_result("::hasModerator[400258]{}"),
      query_result("Tag[100590]{id: \"206\", name: \"Charlemagne\", url: "
                    "\"http://dbpedia.org/resource/Charlemagne\"}"),
      query_result("::hasTag[400259]{}") });
  
  ldbc_iu_query_4(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 5", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "forum_0_0.csv", snb_dir + "person_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "forum_hasMember_person_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Forum[33]{creationDate: \"2010-02-15T00:46:27.657+0000\", id: \"37\", title: \"Wall of Hồ Chí Do\"}"),
      query_result("Person[95299]{birthday: 467251200, browserUsed: \"Chrome\", "
                    "creationDate: \"2010-01-02T06:04:55.320+0000\", firstName: \"Emperor of Brazil\", "
                    "gender: \"female\", id: \"1564\", lastName: \"Silva\", "
                    "locationIP: \"192.223.88.63\"}"),
      query_result("::hasMember[1611869]{creationDate: \"2010-02-23 09:10:25.466\"}")});
  
  ldbc_iu_query_5(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 6", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "post_0_0.csv", snb_dir + "person_0_0.csv", 
                                          snb_dir + "forum_0_0.csv", snb_dir + "place_0_0.csv",
                                          snb_dir + "tag_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "post_hasCreator_person_0_0.csv",
                                            snb_dir + "forum_containerOf_post_0_0.csv",
                                            snb_dir + "post_isLocatedIn_place_0_0.csv",
                                            snb_dir + "post_hasTag_tag_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Post[1121529]{browser: \"Safari\", content: \"About Alexander I of "
                      "Russia,  (23 December  1777 – 1 December  1825), (Russian: "
                      "Александр Благословенный, Aleksandr Blagoslovennyi, meaning Alexander the Bless\", "
                      "creationDate: \"2011-09-07 14:52:27.809\", id: \"13439\", imageFile: \"\", language: \"\"uz\"\", "
                      "length: 159, locationIP: \"46.19.159.176\"}"),
      query_result("Person[1013316]{birthday: 565315200, browserUsed: \"Safari\", "
                    "creationDate: \"2010-08-24T20:13:46.569+0000\", firstName: \"Fritz\", "
                    "gender: \"female\", id: \"6597069777240\", lastName: \"Muller\", "
                    "locationIP: \"46.19.159.176\"}"),
      query_result("::hasCreator[3724073]{}"),
      query_result("Forum[1060792]{creationDate: \"2010-09-21T16:25:35.425+0000\", id: \"549755871489\", "
                    "title: \"Group for Alexander_I_of_Russia in Umeå\"}"),
      query_result("::containerOf[3724074]{}"),
      query_result("Place[1104039]{id: \"50\", name: \"Germany\", type: \"country\", "
                    "url: \"http://dbpedia.org/resource/Germany\"}"),
      query_result("::isLocatedn[3724075]{}"),
      query_result("Tag[1107128]{id: \"1679\", name: \"Alexander_I_of_Russia\", "
                    "url: \"http://dbpedia.org/resource/Alexander_I_of_Russia\"}"),
      query_result("::hasTag[3724076]{}")});
  
  ldbc_iu_query_6(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 7", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "comment_0_0.csv", snb_dir + "post_0_0.csv", 
                                          snb_dir + "person_0_0.csv", snb_dir + "place_0_0.csv",
                                          snb_dir + "tag_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "comment_hasCreator_person_0_0.csv",
                                            snb_dir + "comment_replyOf_post_0_0.csv",
                                            snb_dir + "comment_isLocatedIn_place_0_0.csv",
                                            snb_dir + "comment_hasTag_tag_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

result_set rs, expected;
  expected.append(
      {query_result("Comment[3083206]{browser: \"Chrome\", content: \"fine\", creationDate: "
                    "\"2012-01-09 11:49:15.991\", "
                      "id: \"442214\", length: 4, locationIP: \"91.149.169.27\"}"),
      query_result("Person[3059734]{birthday: 631324800, browserUsed: \"Chrome\", "
                    "creationDate: \"2010-12-25T08:07:55.284+0000\", "
                    "firstName: \"Ivan Ignatyevich\", gender: \"male\", id: \"10995116283243\", "
                    "lastName: \"Aleksandrov\", locationIP: \"91.149.169.27\"}"),
      query_result("::hasCreator[7814151]{}"),
      query_result("Post[2052494]{browserUsed: \"Internet Explorer\", content: \"About Louis I of Hungary, "
                    "dwig der Große, Bulgarian: Лудвиг I, Serbian: Лајош I Анжујски, Czech: Ludvík I. "
                    "Veliký, Li\", creationDate: \"2012-01-09T07:50:59.110+0000\", id: \"1649267442210\", "
                    "language: \"tk\", length: 117, locationIP: \"192.147.218.174\"}"),
      query_result("::replyOf[7814152]{}"),
      query_result("Place[3065729]{id: \"63\", name: \"Belarus\", type: \"country\", url: "
                    "\"http://dbpedia.org/resource/Belarus\"}"),
      query_result("::isLocatedn[7814153]{}"),
      query_result("Tag[3068805]{id: \"1679\", name: \"Alexander_I_of_Russia\", "
                    "url: \"http://dbpedia.org/resource/Alexander_I_of_Russia\"}"),
      query_result("::hasTag[7814154]{}")});
  
  ldbc_iu_query_7(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}

TEST_CASE("LDBC Interactive Insert Query 8", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  std::vector<std::string> node_files = {snb_dir + "person_0_0.csv"};

  std::vector<std::string> rship_files = {snb_dir + "person_knows_person_0_0.csv"};

  load_snb_data(graph, node_files, rship_files);

  result_set rs, expected;
  expected.append(
      {query_result("Person[2]{birthday: 592790400, browserUsed: \"Internet Explorer\", "
                    "creationDate: \"2010-02-15T00:46:17.657+0000\", firstName: \"Hồ Chí\", "
                    "gender: \"male\", id: \"4194\", lastName: \"Do\", "
                    "locationIP: \"103.2.223.188\"}"),
      query_result("Person[4807]{birthday: 467251200, browserUsed: \"Chrome\", "
                    "creationDate: \"2010-01-02T06:04:55.320+0000\", firstName: \"Emperor of Brazil\", "
                    "gender: \"female\", id: \"1564\", lastName: \"Silva\", "
                    "locationIP: \"192.223.88.63\"}"),
        query_result("::KNOWS[180623]{creationDate: \"2010-02-23 09:10:15.466\"}")});
  
  ldbc_iu_query_8(graph, rs);
  
  REQUIRE(rs == expected);

#ifdef USE_TX
  graph->commit_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}
