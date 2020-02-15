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
  auto hoChi = graph->add_node(
      // 4194|Hồ Chí|Do|male|1988-10-14|2010-02-15T00:46:17.657+0000|103.2.223.188|Internet Explorer
      "Person",
      {{"id", boost::any(4194)},
       {"firstName", boost::any(std::string("Hồ Chí"))}, 
       {"lastName", boost::any(std::string("Do"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1988-10-14"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-02-15 00:46:17.657"))},
       {"locationIP", boost::any(std::string("103.2.223.188"))},
       {"browser", boost::any(std::string("Internet Explorer"))}}); 
  auto lomana = graph->add_node(
      // 15393162795439|Lomana Trésor|Kanam|male|1986-09-22|2011-04-02T23:53:29.932+0000|41.76.137.230|Chrome
      "Person",
      {{"id", boost::any(15393)},
       {"firstName", boost::any(std::string("Lomana Trésor"))},
       {"lastName", boost::any(std::string("Kanam"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1986-09-22"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-04-02 23:53:29.932"))},
       {"locationIP", boost::any(std::string("41.76.137.230"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto amin = graph->add_node(
      // 19791209307382|Amin|Kamkar|male|1989-05-24|2011-08-30T05:41:09.519+0000|81.28.60.168|Internet Explorer
      "Person",
      {{"id", boost::any(19791)},
       {"firstName", boost::any(std::string("Amin"))},
       {"lastName", boost::any(std::string("Kamkar"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1989-05-24"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-08-30 05:41:09.519"))},
       {"locationIP", boost::any(std::string("81.28.60.168"))},
       {"browser", boost::any(std::string("Internet Explorer"))}});
  auto silva = graph->add_node(
      // 1564|Emperor of Brazil|Silva|female|1984-10-22|2010-01-02T06:04:55.320+0000|192.223.88.63|Chrome
      "Person",
      {{"id", boost::any(1564)},
       {"firstName", boost::any(std::string("Emperor of Brazil"))},
       {"lastName", boost::any(std::string("Silva"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1984-10-22"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-01-02 06:04:55.320"))},
       {"locationIP", boost::any(std::string("192.223.88.63"))},
       {"browser", boost::any(std::string("Chrome"))}});
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
  graph->add_relationship(Kelaniya, post_656, ":hasTested", {});
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
  auto post_16492674 = graph->add_node(
    /* 1649267442210||2012-01-09T07:50:59.110+0000|192.147.218.174|Internet Explorer|tk|About Louis I of Hungary, dwig der Große,
	      Bulgarian: Лудвиг I, Serbian: Лајош I Анжујски, Czech: Ludvík I. Veliký, Li|117 */
      "Post",
      {{"id", boost::any(16492674)}, //{"id", boost::any(std::string("1649267442210"))},
       //{"type", boost::any(std::string("post"))},
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"language", boost::any(std::string("tk"))}, //String[0..1]
       {"browser", boost::any(std::string("Internet Explorer"))},
       {"locationIP", boost::any(std::string("192.147.218.174"))},
       {"content", boost::any(std::string("About Louis I of Hungary, dwig der Große, Bulgarian: Лудвиг I, Serbian: Лајош I Анжујски, Czech: Ludvík I. Veliký, Li"))},
       {"length", boost::any(117)},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-01-09 08:05:28.922"))}    
       });
  auto comment_12362343 = graph->add_node(
      // id|creationDate|locationIP|browserUsed|content|length 
      // 1236950581249|2011-08-17T14:26:59.961+0000|92.39.58.88|Chrome|yes|3
      "Comment",
      {
          {"id", boost::any(12362343)},
          //{"type", boost::any(std::string("comment"))},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2011-08-17 14:26:59.961"))},
          {"locationIP", boost::any(std::string("92.39.58.88"))},
          {"browser", boost::any(std::string("Chrome"))},
          {"content", boost::any(std::string("yes"))},
          {"length", boost::any(3)}
      });
  auto comment_16492675 = graph->add_node(
      /* 1649267442211|2012-01-09T08:05:28.922+0000|91.149.169.27|Chrome|About Louis I of Hungary, rchs of the Late Middle Ages,
	        extending terrAbout Louis XIII |87 */
      "Comment",
      {
          {"id", boost::any(16492675)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2011-10-05 14:38:36.019"))},
          {"locationIP", boost::any(std::string("91.149.169.27"))},
          {"browser", boost::any(std::string("Chrome"))},
          {"content", boost::any(std::string("About Louis I of Hungary, rchs of the Late Middle Ages, "
          "extending terrAbout Louis XIII "))},
          {"length", boost::any(87)}
      });
  auto comment_16492676 = graph->add_node(
      /* 1649267442212|2012-01-10T03:24:33.368+0000|14.196.249.198|Firefox|About Bruce Lee,  sources, in the spirit of his personal
	        martial arts philosophy, whic|86 */
      "Comment",
      {
          {"id", boost::any(16492676)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 03:24:33.368"))},
          {"locationIP", boost::any(std::string("14.196.249.198"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("About Bruce Lee,  sources, in the spirit of "
          "his personal martial arts philosophy, whic"))},
          {"length", boost::any(86)}
      });
  auto comment_16492677 = graph->add_node(
      /* 1649267442213|2012-01-10T14:57:10.420+0000|81.28.60.168|Internet Explorer|I see|5 */ 
      "Comment",
      {
          {"id", boost::any(16492677)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 14:57:10.420"))},
          {"locationIP", boost::any(std::string("81.28.60.168"))},
          {"browser", boost::any(std::string("Internet Explorer"))},
          {"content", boost::any(std::string("I see"))},
          {"length", boost::any(5)}
      });
  auto comment_1642250 = graph->add_node(
      /* 1649267442250|2012-01-19T11:39:51.385+0000|85.154.120.237|Firefox|About Louis I of Hungary, 
          ittle lasting political results. Louis is theAbout Union of Sou|89  */
      "Comment",
      {
          {"id", boost::any(1642250)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-19 11:39:51.385"))},
          {"locationIP", boost::any(std::string("85.154.120.237"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("Firefox|About Louis I of Hungary, ittle lasting political results. "
            "Louis is theAbout Union of Sou"))},
          {"length", boost::any(89)}
      });
  auto comment_1642217 = graph->add_node(
      /* 1649267442217|2012-01-10T06:31:18.533+0000|41.76.137.230|Chrome|maybe|5
  */
      "Comment",
      {
          {"id", boost::any(1642217)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2012-01-10 06:31:18.533"))},
          {"locationIP", boost::any(std::string("41.76.137.230"))},
          {"browser", boost::any(std::string("Chrome"))},
          {"content", boost::any(std::string("maybe"))},
          {"length", boost::any(5)}
      });
  auto forum_37 = graph->add_node(
    // id|title|creationDate 
    // 37|Wall of Hồ Chí Do|2010-02-15T00:46:27.657+0000
      "Forum",
      {{"id", boost::any(37)},
       {"title", boost::any(std::string("Wall of Hồ Chí Do"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-02-15 00:46:27.657"))}      
       });
  auto tag_206 = graph->add_node(
    // id|name|url
    // 206|Charlemagne|http://dbpedia.org/resource/Charlemagne
      "Tag",
      {{"id", boost::any(206)},
       {"name", boost::any(std::string("Charlemagne"))},
       {"url",
           boost::any(std::string("http://dbpedia.org/resource/Charlemagne"))}     
       });

  /**
   * Relationships for query interactive short #1
   */
  graph->add_relationship(mahinda, Kelaniya, ":isLocatedIn", {});
  graph->add_relationship(baruch, Gobi, ":isLocatedIn", {});

  /**
   * Relationships for query interactive short #2

  Comment.id|Person.id
  1099518161711|65
  1099518161722|65
  1236957114306|65
  1236957114312|65
  1374390163914|65
  1374390164865|65
  1374390164871|65
  1374390164873|65
  1374390164877|65
  1374390164880|65

Comment.id|Comment.id ---> ---> --- ---> Comment.id|Post.id
1236957114306|1236957114304 --> 1236957114304|1236957114302
1236957114312|1236957114305 --> 1236957114305|1236957114302


1099518161711|1099518161705
1099518161722|1099518161705


  Post.id|Person.id
  1374396068983|65
  1374396069001|65
  1374396069019|65
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
  graph->add_relationship(mahinda, baruch, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2010-03-13 07:37:21.718"))}, 
        {"dummy_property", boost::any(std::string("dummy_1"))}});
  graph->add_relationship(mahinda, fritz, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2010-09-20 09:42:43.187"))},
        {"dummy_property", boost::any(std::string("dummy_2"))}});
  graph->add_relationship(mahinda, andrei, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2011-01-02 06:43:41.955"))},
        {"dummy_property", boost::any(std::string("dummy_3"))}});
  graph->add_relationship(mahinda, ottoB, ":KNOWS", {
        {"creationDate", boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))},
        {"dummy_property", boost::any(std::string("dummy_4"))}});
  graph->add_relationship(mahinda, ottoR, ":KNOWS", {
        {"creationDate", /* testing date order */ boost::any(builtin::dtimestring_to_int("2012-09-07 01:11:30.195"))},
        {"dummy_property", boost::any(std::string("dummy_5"))}});

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

	Comment.id|Comment.id
	1649267442213|1649267442212 //replyOf
	1649267442212|1649267442211 //replyOf
	
	
	Comment.id|Post.id
	1649267442211|1649267442210 //replyOf	
	
	Forum.id|Post.id
	37|1649267442210 //containerOf
	
	Forum.id|Person.id
	37|4194 //hasModerator
   */
  graph->add_relationship(forum_37, post_16492674, ":containerOf", {});
  graph->add_relationship(forum_37, hoChi, ":hasModerator", {});
  graph->add_relationship(comment_16492675, post_16492674, ":replyOf", {});
  graph->add_relationship(comment_16492676, comment_16492675, ":replyOf", {});
  graph->add_relationship(comment_16492677, comment_16492676, ":replyOf", {});

  /**
   * Relationships for query interactive short #7
   * 
  Comment.id|Comment.id
  1649267442217|1649267442212   // :replyOf

  Comment.id|Person.id
  1649267442217|15393162795439    // :hasCreator
  1649267442213|19791209307382

   */
  graph->add_relationship(comment_1642217, comment_16492676, ":replyOf", {});
  graph->add_relationship(comment_1642217, lomana, ":hasCreator", {});
  graph->add_relationship(comment_16492677, amin, ":hasCreator", {});

  /**
   * Relationships for query update #1
   */

  /**
   * Relationships for query update #2
   */

  /**
   * Relationships for query update #3

  Person.id|Comment.id|creationDate
  1564|1649267442250|2012-01-23T08:56:30.617+0000
   */

  /**
   * Relationships for query update #4

  Forum.id|Person.id
  53975|1564  // :hasModerator

  Forum.id|Tag.id
  53975|206   // :hasTag

  id|name|url
  206|Charlemagne|http://dbpedia.org/resource/Charlemagne

  id|title|creationDate 
  53975|Wall of Emperor of Brazil Silva|2010-01-02T06:05:05.320+0000
   */

  /**
   * Relationships for query update #5

  Forum.id|Person.id|joinDate
  37|1564|2010-02-23T09:10:25.466+0000  
   */

  /**
   * Relationships for query update #8

  Person.id|Person.id|creationDate
  1564|4194|2010-02-23T09:10:15.466+0000
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
         query_result("2010-Feb-14 15:32:10")});

    ldbc_is_query_1(graph, rs);

    REQUIRE(rs == expected);
  }

  SECTION("query interactive short #3") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("833579"), query_result("Otto"),
         query_result("Becker"), query_result("2012-Sep-07 01:11:30")});
    expected.data.push_back(
        {query_result("838375"), query_result("Otto"),
         query_result("Richter"), query_result("2012-Sep-07 01:11:30")});
    expected.data.push_back(
        {query_result("10995116"), query_result("Andrei"),
         query_result("Condariuc"), query_result("2011-Jan-02 06:43:41")});
    expected.data.push_back(
        {query_result("65970697"), query_result("Fritz"),
         query_result("Muller"), query_result("2010-Sep-20 09:42:43")});
    expected.data.push_back(
        {query_result("4139"), query_result("Baruch"),
         query_result("Dego"), query_result("2010-Mar-13 07:37:21")});
    
    ldbc_is_query_3(graph, rs);
    //std::cout << rs;

    REQUIRE(rs == expected);
  }

  SECTION("query interactive short #4") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("2011-Oct-05 14:38:36"),
         query_result("photo1374389534791.jpg")});
    

    ldbc_is_query_4(graph, rs);
    //std::cout << rs;

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

  SECTION("query interactive short #6") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("37"),
         query_result("Wall of Hồ Chí Do"),
         query_result("4194"),
         query_result("Hồ Chí"),
         query_result("Do")});

    ldbc_is_query_6(graph, rs);
    //std::cout << rs;
    
    REQUIRE(rs == expected);
  }

  SECTION("query interactive short #7") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("expected")});

    //ldbc_is_query_7(graph, rs);
    //std::cout << rs;
    
    //REQUIRE(rs == expected);
  }

  SECTION("query update #2") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[0]{birthday: 628646400, browser: \"Firefox\", "
                      "creationDate: 1266161530, firstName: \"Mahinda\", "
                      "gender: \"male\", id: 933, lastName: \"Perera\", "
                      "locationIP: \"119.235.7.103\"}"),
         query_result("Post[13]{browserUsed: \"Firefox\", content: \"Bla Bla\", "
                      "creationDate: 1264190399, id: 656, length: 7, "
                      "locationIP: \"119.235.7.103\"}"),
         query_result(":LIKES[17]{creationDate: 1266161530}")});
    ldbc_iu_query_2(graph, rs);
    REQUIRE(rs == expected);
  }

  SECTION("query update #3") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[10]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"),
         query_result("Comment[20]{browser: \"Firefox\", content: \"Firefox|About Louis I of Hungary, "
                      "ittle lasting political results. Louis is theAbout Union of Sou\", "
                      "creationDate: 1326973191, id: 1642250, length: 89, "
                      "locationIP: \"85.154.120.237\"}"),
         query_result(":LIKES[17]{creationDate: 1327308990}")});
    ldbc_iu_query_3(graph, rs);
    REQUIRE(rs == expected);
  }

  SECTION("query update #4") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[4]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"),
         query_result("Comment[18]{browser: \"Firefox\", content: \"Firefox|About Louis I of Hungary, "
                      "ittle lasting political results. Louis is theAbout Union of Sou\", "
                      "creationDate: 1326973191, id: 1642250, length: 89, "
                      "locationIP: \"85.154.120.237\"}"),
         query_result(":LIKES[14]{creationDate: 1327308990}")});
    //ldbc_iu_query_4(graph, rs);
    //REQUIRE(rs == expected);
  }

  SECTION("query update #5") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[10]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"),
         query_result("Forum[22]{creationDate: 1266194787, id: 37, title: \"Wall of Hồ Chí Do\"}"),
         query_result("::hasMember[17]{creationDate: 1266916225}")});
    ldbc_iu_query_5(graph, rs);
    REQUIRE(rs == expected);
  }

  SECTION("query update #8") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[10]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"),
        query_result("Person[7]{birthday: 592790400, browser: \"Internet Explorer\", "
                      "creationDate: 1266194777, firstName: \"Hồ Chí\", "
                      "gender: \"male\", id: 4194, lastName: \"Do\", "
                      "locationIP: \"103.2.223.188\"}"),
         query_result(":KNOWS[17]{creationDate: 1266916215}")});
    ldbc_iu_query_8(graph, rs);
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