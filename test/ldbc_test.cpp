#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include "catch.hpp"
#include "config.h"
#include "defs.hpp"
#include "ldbc.hpp"
#include "query.hpp"

#ifdef USE_PMDK
namespace nvm = pmem::obj;

#define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 80))

namespace nvm = pmem::obj;
const std::string test_path = poseidon::gPmemPath + "ldbc_test";

nvm::pool_base prepare_pool() {
  auto pop = nvm::pool_base::create(test_path, "", PMEMOBJ_POOL_SIZE);
  return pop;
}
#endif


graph_db_ptr create_graph2(
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

  auto ravalomanana = graph->add_node(
      // 65|Marc|Ravalomanana|female|1989-06-15|2010-02-26T23:17:18.465+0000|41.204.119.20|Firefox
      "Person",
      {{"id", boost::any(65)},
       {"firstName", boost::any(std::string("Marc"))},
       {"lastName", boost::any(std::string("Ravalomanana"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1989-06-15"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-02-26 23:17:18.465"))},
       {"locationIP", boost::any(std::string("41.204.119.20"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto person2_1 = graph->add_node(
      // 19791209302379|Muhammad|Iqbal|female|1983-09-13|2011-08-14T03:06:21.524+0000|202.14.71.199|Chrome
      "Person",
      {{"id", boost::any(1379)},
       {"firstName", boost::any(std::string("Muhammad"))},
       {"lastName", boost::any(std::string("Iqbal"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1983-09-13"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-08-14 03:06:21.524"))},
       {"locationIP", boost::any(std::string("202.14.71.199"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto person2_2 = graph->add_node(
      // 17592186055291|Wei|Li|female|1986-09-24|2011-05-10T20:09:44.151+0000|1.4.4.26|Chrome
      "Person",
      {{"id", boost::any(1291)},
       {"firstName", boost::any(std::string("Wei"))},
       {"lastName", boost::any(std::string("Li"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1986-09-24"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-05-10 20:09:44.151"))},
       {"locationIP", boost::any(std::string("1.4.4.26"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto person2_3 = graph->add_node(
      // 15393162799121|Karl|Beran|male|1983-05-30|2011-04-02T00:14:40.528+0000|31.130.85.235|Chrome
      "Person",
      {{"id", boost::any(1121)},
       {"firstName", boost::any(std::string("Karl"))},
       {"lastName", boost::any(std::string("Beran"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1983-05-30"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-04-02 00:14:40.528"))},
       {"locationIP", boost::any(std::string("31.130.85.235"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto person2_4 = graph->add_node(
      // 8796093028680|Rahul|Singh|female|1981-12-29|2010-10-09T07:08:12.913+0000|61.17.209.13|Firefox
      "Person",
      {{"id", boost::any(1680)},
       {"firstName", boost::any(std::string("Rahul"))},
       {"lastName", boost::any(std::string("Singh"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1981-12-2"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-10-09 07:08:12.913"))},
       {"locationIP", boost::any(std::string("61.17.209.13"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto person2_5 = graph->add_node(
      // 19791209302377|John|Smith|male|1983-08-31|2011-08-10T15:59:24.890+0000|24.245.233.94|Firefox
      "Person",
      {{"id", boost::any(1377)},
       {"firstName", boost::any(std::string("John"))},
       {"lastName", boost::any(std::string("Smith"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1983-08-31"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-08-10 15:59:24.890"))},
       {"locationIP", boost::any(std::string("24.245.233.94"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto person2_6 = graph->add_node(
      // 10995116278350|Abdul|Aman|male|1982-05-24|2010-11-17T00:16:33.065+0000|180.222.141.92|Internet Explorer
      "Person",
      {{"id", boost::any(1350)},
       {"firstName", boost::any(std::string("Abdul"))},
       {"lastName", boost::any(std::string("Aman"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1982-05-24"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-11-17 00:16:33.065"))},
       {"locationIP", boost::any(std::string("180.222.141.92"))},
       {"browser", boost::any(std::string("Internet Explorer"))}});
  auto person2_7 = graph->add_node(
      // 4398046514661|Rajiv|Singh|male|1983-02-17|2010-05-13T06:57:29.021+0000|49.46.196.167|Firefox
      "Person",
      {{"id", boost::any(1661)},
       {"firstName", boost::any(std::string("Rajiv"))},
       {"lastName", boost::any(std::string("Singh"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1983-02-17"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-05-13 06:57:29.021"))},
       {"locationIP", boost::any(std::string("49.46.196.167"))},
       {"browser", boost::any(std::string("Firefox"))}});
  auto person2_8 = graph->add_node(
      // 10995116278353|Otto|Muller|male|1988-10-28|2010-12-19T22:06:54.592+0000|204.79.148.6|Firefox
      "Person",
      {{"id", boost::any(18353)},
       {"firstName", boost::any(std::string("Otto"))},
       {"lastName", boost::any(std::string("Muller"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1988-10-28"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-12-19 22:06:54.592"))},
       {"locationIP", boost::any(std::string("204.79.148.6"))},
       {"browser", boost::any(std::string("Firefox"))}});


  auto post2_1 = graph->add_node(
    // id|imageFile|creationDate|locationIP|browserUsed|language|content|length
    /* 1374390164863||2011-10-17T05:40:34.561+0000|41.204.119.20|Firefox|uz|About Paul Keres,  in 
        the Candidates' Tournament on four consecutive occasions. Due to these and other strong results, 
        many chess historians consider Keres the strongest player never to be|188 */
      "Post",
      {{"id", boost::any(1863)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-10-17 05:40:34.561"))},
       {"locationIP", boost::any(std::string("41.204.119.20"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"uz\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Paul Keres,  in the "
       "Candidates' Tournament on four consecutive occasions. Due to these "
       "and other strong results, many chess historians consider Keres the strongest player never to be"))},
       {"length", boost::any(188)}           
       });
  auto post2_2 = graph->add_node(
    /* 1649268071976||2012-01-14T09:41:00.992+0000|24.245.233.94|Firefox|uz|About Paul Keres, hampionship "
      match against champion Alexander Alekhine, but the match never took place due to World War|120 */
      "Post",
      {{"id", boost::any(1976)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-01-14 09:41:00.992"))},
       {"locationIP", boost::any(std::string("24.245.233.94"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"uz\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Paul Keres, hampionship "
        "match against champion Alexander Alekhine, but the match never took place due to World War"))},
       {"length", boost::any(120)}           
       });
  auto post2_3 = graph->add_node(
    /* 1374390165125||2011-10-16T15:05:23.955+0000|204.79.148.6|Firefox|uz|About Otto von Bismarck, onsible 
    for the unifiAbout Muammar Gaddafi, e styled himself as LAbout Pete T|102 */
      "Post",
      {{"id", boost::any(1125)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-10-16 15:05:23.955"))},
       {"locationIP", boost::any(std::string("204.79.148.6"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"uz\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Otto von Bismarck, onsible "
        "for the unifiAbout Muammar Gaddafi, e styled himself as LAbout Pete T"))},
       {"length", boost::any(188)}           
       });
  auto post2_4 = graph->add_node(
    /* 1374390165164||2011-10-16T23:30:53.955+0000|1.4.4.26|Chrome|uz|About Muammar Gaddafi, June 1942Sirte, 
    Libya Died 20 October 2About James Joyce, he Jesuit schools Clongowes and BelvedeAbout Laurence Olivier,  and 
    British drama. He was the first arAbout Osa|192 */
      "Post",
      {{"id", boost::any(1164)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-10-16 23:30:53.955"))},
       {"locationIP", boost::any(std::string("1.4.4.26"))},
       {"browser", boost::any(std::string("Chrome"))},
       {"language", boost::any(std::string("\"uz\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Muammar Gaddafi, June 1942Sirte, "
        "Libya Died 20 October 2About James Joyce, he Jesuit schools Clongowes and BelvedeAbout Laurence Olivier,  and "
        "British drama. He was the first arAbout Osa"))},
       {"length", boost::any(192)}           
       });
  auto post2_5 = graph->add_node(
    /* 1786712928767||2012-03-29T11:17:50.625+0000|31.130.85.235|Chrome|ar|About Catherine the 
    Great, (2 May  1729 – 17 November  1796), was the most renowned and th|90 */
      "Post",
      {{"id", boost::any(1767)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2012-03-29 11:17:50.625"))},
       {"locationIP", boost::any(std::string("31.130.85.235"))},
       {"browser", boost::any(std::string("Chrome"))},
       {"language", boost::any(std::string("\"ar\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Catherine the "
        "Great, (2 May  1729 – 17 November  1796), was the most renowned and th"))},
       {"length", boost::any(90)}           
       });
  auto post2_6 = graph->add_node(
    /* 1099518161705||2011-06-24T22:26:11.884+0000|61.17.209.13|Firefox|ar|About Catherine the Great, 
    e largest share. In the east, Russia started to colonise Al|86 */
      "Post",
      {{"id", boost::any(1705)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-06-24 22:26:11.884"))},
       {"locationIP", boost::any(std::string("61.17.209.13"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"ar\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Catherine the Great, "
        "e largest share. In the east, Russia started to colonise Al"))},
       {"length", boost::any(86)}           
       });
  auto post2_7 = graph->add_node(
    /* 1374396068816||2011-09-27T05:59:43.468+0000|49.46.196.167|Firefox|ar|About Fernando González, 
    y Chile's best tennis player oAbout Vichy France,  (GPRF). Most |89 */
      "Post",
      {{"id", boost::any(1816)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-09-27 05:59:43.468"))},
       {"locationIP", boost::any(std::string("49.46.196.167"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"ar\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Fernando González, "
        "y Chile's best tennis player oAbout Vichy France,  (GPRF). Most "))},
       {"length", boost::any(89)}           
       });
  auto post2_8 = graph->add_node(
    /* 1374396068820||2011-09-26T16:39:28.468+0000|49.46.196.167|Firefox|ar|About Fernando González, ian Open, 
    losing tAbout Mary, Queen of Scots, ter. In 1558, she About David, to p|106 */
      "Post",
      {{"id", boost::any(1816)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-09-26 16:39:28.468"))},
       {"locationIP", boost::any(std::string("49.46.196.167"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"ar\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Fernando González, ian Open, "
        "losing tAbout Mary, Queen of Scots, ter. In 1558, she About David, to p"))},
       {"length", boost::any(106)}           
       });
  auto post2_9 = graph->add_node(
    /* 1374396068835||2011-09-27T09:25:13.468+0000|49.46.196.167|Firefox|ar|About Fernando González, 
    en, Marat SafiAbout Cole Porter, u Under My SkiAbout Ray Bradbury, 953) and for tA|107 */
      "Post",
      {{"id", boost::any(1835)}, 
       {"imageFile", boost::any(std::string(""))}, //String[0..1]
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-09-27 09:25:13.468"))},
       {"locationIP", boost::any(std::string("49.46.196.167"))},
       {"browser", boost::any(std::string("Firefox"))},
       {"language", boost::any(std::string("\"ar\""))}, //String[0..1]       
       {"content", boost::any(std::string("About Fernando González, "
        "en, Marat SafiAbout Cole Porter, u Under My SkiAbout Ray Bradbury, 953) and for tA"))},
       {"length", boost::any(107)}           
       });
  auto comment2_1 = graph->add_node(
      /* 1374390164865|2011-10-17T09:17:43.567+0000|41.204.119.20|Firefox|About Paul Keres, Alexander Alekhine, 
      but the match never toAbout Birth of a Prince|83 */
      "Comment",
      {
          {"id", boost::any(1865)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-01-17 09:17:43.567"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("About Paul Keres, Alexander Alekhine, "
            "but the match never toAbout Birth of a Prince"))},
          {"length", boost::any(83)}
      });
  auto comment2_2 = graph->add_node(
      /* 1374390164877|2011-10-17T10:59:33.177+0000|41.204.119.20|Firefox|yes|3 */
      "Comment",
      {
          {"id", boost::any(1877)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-2-17 10:59:33.177"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("yes"))},
          {"length", boost::any(3)}
      });
  auto comment2_3 = graph->add_node(
      /* 1649268071978|2012-01-14T16:57:46.045+0000|41.204.119.20|Firefox|yes|3 */
      "Comment",
      {
          {"id", boost::any(1978)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-03-14 16:57:46.045"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("yes"))},
          {"length", boost::any(3)}
      });
  auto comment2_4 = graph->add_node(
      /* 1374390165126|2011-10-16T21:16:03.354+0000|41.204.119.20|Firefox|roflol|6 */
      "Comment",
      {
          {"id", boost::any(1126)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-04-16 21:16:03.354"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("roflol"))},
          {"length", boost::any(6)}
      });
  auto comment2_5 = graph->add_node(
      /* 1374390165171|2011-10-17T19:37:26.339+0000|41.204.119.20|Firefox|yes|3 */
      "Comment",
      {
          {"id", boost::any(1171)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-05-17 19:37:26.339"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("yes"))},
          {"length", boost::any(3)}
      });
  auto comment2_6 = graph->add_node(
      /* 1786712928768|2012-03-29T17:57:51.844+0000|41.204.119.20|Firefox|LOL|3 */
      "Comment",
      {
          {"id", boost::any(1768)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-06-29 17:57:51.844"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("LOL"))},
          {"length", boost::any(3)}
      });
  auto comment2_7 = graph->add_node(
      /* 1099518161711|2011-06-25T07:54:01.976+0000|41.204.119.20|Firefox|no|2 */
      "Comment",
      {
          {"id", boost::any(1711)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-07-25 07:54:01.976"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("no"))},
          {"length", boost::any(2)}
      });
  auto comment2_8 = graph->add_node(
      /* 1099518161722|2011-06-25T12:56:57.280+0000|41.204.119.20|Firefox|LOL|3 */
      "Comment",
      {
          {"id", boost::any(1722)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-08-25 12:56:57.280"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("LOL"))},
          {"length", boost::any(3)}
      });
  auto comment2_9 = graph->add_node(
      /* 1374396068819|2011-09-27T09:41:01.413+0000|41.204.119.20|Firefox|About Fernando González, 
      er from Chile. He is kAbout George W. Bush, 04 for a description oAbout Vichy Franc|108 */
      "Comment",
      {
          {"id", boost::any(1819)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-09-27 09:41:01.413"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("About Fernando González, "
            "er from Chile. He is kAbout George W. Bush, 04 for a description oAbout Vichy Franc"))},
          {"length", boost::any(108)}
      });
  auto comment2_10 = graph->add_node(
      /* 1374396068821|2011-09-26T23:46:18.580+0000|41.76.205.156|Firefox|About Fernando González, 
      les at Athens 2004About Ronald Reagan, st in films and laAbou|86 */
      "Comment",
      {
          {"id", boost::any(1821)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-10-26 23:46:18.580"))},
          {"locationIP", boost::any(std::string("41.76.205.156"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("About Fernando González, "
            "les at Athens 2004About Ronald Reagan, st in films and laAbou"))},
          {"length", boost::any(86)}
      });
  auto comment2_11 = graph->add_node(
      /* 1374396068827|2011-09-26T17:09:07.283+0000|41.204.119.20|Firefox|About Fernando González, 
      Safin, and Pete SAbout Mary, Queen of Scots,  had previously cAbout Edward the Confessor, isintegration of Abo|135 */
      "Comment",
      {
          {"id", boost::any(1827)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-11-26 17:09:07.283"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("About Fernando González, "
            "Safin, and Pete SAbout Mary, Queen of Scots,  had previously cAbout Edward the Confessor, isintegration of Abo"))},
          {"length", boost::any(135)}
      });
  auto comment2_12 = graph->add_node(
      /* 1374396068837|2011-09-27T11:32:19.336+0000|41.204.119.20|Firefox|maybe|5 */
      "Comment",
      {
          {"id", boost::any(1837)},
          {"creationDate", boost::any(builtin::dtimestring_to_int("2013-12-27 11:32:19.336"))},
          {"locationIP", boost::any(std::string("41.204.119.20"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"content", boost::any(std::string("maybe"))},
          {"length", boost::any(5)}
      });

  /**
   * Relationships for query interactive short #2

  Comment.id|Person.id
  1099518161711|65
  1099518161722|65
  1374390164865|65
  1374390164877|65
  1374390165126|65
  1374390165171|65
  1374396068819|65
  1374396068821|65
  1374396068827|65
  1374396068837|65
  1649268071978|65
  1786712928768|65


  Comment.id|Post.id
  1374390164865|1374390164863
  1374390164877|1374390164863
  1649268071978|1649268071976
  1374390165126|1374390165125
  1374390165171|1374390165164
  1786712928768|1786712928767
  1099518161711|1099518161705
  1099518161722|1099518161705
  1374396068819|1374396068816
  1374396068821|1374396068820
  1374396068827|1374396068820
  1374396068837|1374396068835

  Post.id|Person.id
  1374390164863|65
  1649268071976|19791209302377
  1374390165125|10995116278353
  1374390165164|17592186055291
  1786712928767|15393162799121
  1099518161705|8796093028680
  1374396068816|4398046514661
  1374396068820|4398046514661
  1374396068835|4398046514661
   */
  graph->add_relationship(comment2_1, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_2, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_3, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_4, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_5, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_6, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_7, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_8, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_9, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_10, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_11, ravalomanana, ":hasCreator", {});
  graph->add_relationship(comment2_12, ravalomanana, ":hasCreator", {});

  graph->add_relationship(comment2_1, post2_1, ":replyOf", {});
  graph->add_relationship(comment2_2, post2_1, ":replyOf", {});
  graph->add_relationship(comment2_3, post2_2, ":replyOf", {});
  graph->add_relationship(comment2_4, post2_3, ":replyOf", {});
  graph->add_relationship(comment2_5, post2_4, ":replyOf", {});
  graph->add_relationship(comment2_6, post2_5, ":replyOf", {});
  graph->add_relationship(comment2_7, post2_6, ":replyOf", {});
  graph->add_relationship(comment2_8, post2_6, ":replyOf", {});
  graph->add_relationship(comment2_9, post2_7, ":replyOf", {});
  graph->add_relationship(comment2_10, post2_8, ":replyOf", {});
  graph->add_relationship(comment2_11, post2_8, ":replyOf", {});
  graph->add_relationship(comment2_12, post2_9, ":replyOf", {});

  graph->add_relationship(post2_1, ravalomanana, ":hasCreator", {});
  graph->add_relationship(post2_2, person2_5, ":hasCreator", {});
  graph->add_relationship(post2_3, person2_8, ":hasCreator", {});
  graph->add_relationship(post2_4, person2_2, ":hasCreator", {});
  graph->add_relationship(post2_5, person2_3, ":hasCreator", {});
  graph->add_relationship(post2_6, person2_4, ":hasCreator", {});
  graph->add_relationship(post2_7, person2_7, ":hasCreator", {});
  graph->add_relationship(post2_8, person2_7, ":hasCreator", {});
  graph->add_relationship(post2_9, person2_7, ":hasCreator", {});

#ifdef USE_TX
  graph->commit_transaction();
#endif

  return graph;
}

TEST_CASE("Testing LDBC interactive short query 2", "[ldbc]") {
#ifdef USE_PMDK
  auto pop = prepare_pool();
  auto graph = create_graph(pop);
#else
  auto graph = create_graph2();
#endif

#ifdef USE_TX
  auto tx = graph->begin_transaction();
#endif

  SECTION("query interactive short #2") {
    result_set rs, expected;

    expected.data.push_back(
        {query_result("1863"), 
        query_result("About Paul Keres,  in the Candidates' Tournament on four consecutive "
            "occasions. Due to these and other strong results, many chess historians consider "
            "Keres the strongest player never to be"),
        query_result("2011-Oct-17 05:40:34"), query_result("1863"), query_result("65"),
        query_result("Marc"), query_result("Ravalomanana")});
    expected.data.push_back({
        query_result("1837"), query_result("maybe"), query_result("2013-Dec-27 11:32:19"), 
        query_result("1835"), query_result("1661"), query_result("Rajiv"), query_result("Singh")});
    expected.data.push_back({
        query_result("1827"), query_result("About Fernando González, Safin, and Pete SAbout Mary, Queen of "
            "Scots,  had previously cAbout Edward the Confessor, isintegration of Abo"), 
        query_result("2013-Nov-26 17:09:07"), query_result("1816"), query_result("1661"), 
        query_result("Rajiv"), query_result("Singh")});
    expected.data.push_back({
        query_result("1821"), query_result("About Fernando González, les at Athens 2004About Ronald Reagan, st "
            "in films and laAbou"), 
        query_result("2013-Oct-26 23:46:18"), query_result("1816"), query_result("1661"), 
        query_result("Rajiv"), query_result("Singh")});
    expected.data.push_back({
        query_result("1819"), query_result("About Fernando González, er from Chile. He is kAbout George W. Bush, "
            "04 for a description oAbout Vichy Franc"), 
        query_result("2013-Sep-27 09:41:01"), query_result("1816"), query_result("1661"), 
        query_result("Rajiv"), query_result("Singh")});
    expected.data.push_back({
        query_result("1722"), query_result("LOL"), query_result("2013-Aug-25 12:56:57"), 
        query_result("1705"), query_result("1680"), query_result("Rahul"), query_result("Singh")});
    expected.data.push_back({
        query_result("1711"), query_result("no"), query_result("2013-Jul-25 07:54:01"), 
        query_result("1705"), query_result("1680"), query_result("Rahul"), query_result("Singh")});
    expected.data.push_back({
        query_result("1768"), query_result("LOL"), query_result("2013-Jun-29 17:57:51"), 
        query_result("1767"), query_result("1121"), query_result("Karl"), query_result("Beran")});
    expected.data.push_back({
        query_result("1171"), query_result("yes"), query_result("2013-May-17 19:37:26"), 
        query_result("1164"), query_result("1291"), query_result("Wei"), query_result("Li")});
    expected.data.push_back({
        query_result("1126"), query_result("roflol"), query_result("2013-Apr-16 21:16:03"), 
        query_result("1125"), query_result("18353"), query_result("Otto"), query_result("Muller")});

    ldbc_is_query_2(graph, rs);
    //std::cout << rs;

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
  auto ivan = graph->add_node(
      // 10995116283243|Ivan Ignatyevich|Aleksandrov|male|1990-01-03|2010-12-25T08:07:55.284+0000|91.149.169.27|Chrome
      "Person",
      {{"id", boost::any(1043)},
       {"firstName", boost::any(std::string("Ivan Ignatyevich"))},
       {"lastName", boost::any(std::string("Aleksandrov"))},
       {"gender", boost::any(std::string("male"))},
       {"birthday", boost::any(builtin::datestring_to_int("1990-01-03"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-12-25 08:07:55.284"))},
       {"locationIP", boost::any(std::string("91.149.169.27"))},
       {"browser", boost::any(std::string("Chrome"))}});
  auto bingbing = graph->add_node(
      // 15393162790796|Bingbing|Xu|female|1987-09-19|2011-04-28T23:16:44.375+0000|14.196.249.198|Firefox
      "Person",
      {{"id", boost::any(90796)},
       {"firstName", boost::any(std::string("Bingbing"))},
       {"lastName", boost::any(std::string("Xu"))},
       {"gender", boost::any(std::string("female"))},
       {"birthday", boost::any(builtin::datestring_to_int("1987-09-19"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2011-04-28 23:16:44.375"))},
       {"locationIP", boost::any(std::string("14.196.249.198"))},
       {"browser", boost::any(std::string("Firefox"))}});
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
  auto artux = graph->add_node(
      // 505|Artux|http://dbpedia.org/resource/Artux|city
      "Place",
      {
          {"id", boost::any(505)},
          {"name", boost::any(std::string("Artux"))},
          {"url",
           boost::any(std::string("http://dbpedia.org/resource/Artux"))},
          {"type", boost::any(std::string("city"))},
      });
  auto germany = graph->add_node(
      // 50|Germany|http://dbpedia.org/resource/Germany|country
      "Place",
      {
          {"id", boost::any(50)},
          {"name", boost::any(std::string("Germany"))},
          {"url",
           boost::any(std::string("http://dbpedia.org/resource/Germany"))},
          {"type", boost::any(std::string("country"))},
      });
  auto belarus = graph->add_node(
      // 63|Belarus|http://dbpedia.org/resource/Belarus|country
      "Place",
      {
          {"id", boost::any(63)},
          {"name", boost::any(std::string("Belarus"))},
          {"url",
           boost::any(std::string("http://dbpedia.org/resource/Belarus"))},
          {"type", boost::any(std::string("country"))},
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
  auto post_3627 = graph->add_node(
      /* 2061587303627||2012-08-20T09:51:28.275+0000|14.205.203.83|Firefox|uz|About Alexander I of Russia, 
          lexander tried to introduce liberal reforms, while in the second half|98 */
      "Post",
      {
          {"id", boost::any(3627)}, /* boost::any(std::string("2061587303627"))} */
          {"imageFile", boost::any(std::string(""))}, /* String[0..1] */
          {"creationDate",
           boost::any(builtin::dtimestring_to_int("2012-08-20 09:51:28.275"))},
          {"locationIP", boost::any(std::string("14.205.203.83"))},
          {"browser", boost::any(std::string("Firefox"))},
          {"language", boost::any(std::string("uz"))}, /* String[0..1] */ 
          {"content", boost::any(std::string("About Alexander I of Russia, "
            "lexander tried to introduce liberal reforms, while in the second half"))},
          {"length", boost::any(98)}
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
       {"content", boost::any(std::string("About Louis I of Hungary, dwig der Große, Bulgarian: "
        "Лудвиг I, Serbian: Лајош I Анжујски, Czech: Ludvík I. Veliký, Li"))},
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
  auto forum_71489 = graph->add_node(
    // 549755871489|Group for Alexander_I_of_Russia in Umeå|2010-09-21T16:25:35.425+0000
      "Forum",
      {{"id", boost::any(71489)},
       {"title", boost::any(std::string("Group for Alexander_I_of_Russia in Umeå"))},
       {"creationDate",
        boost::any(builtin::dtimestring_to_int("2010-09-21 16:25:35.425"))}      
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
  auto tag_61 = graph->add_node(
    // 61|Kevin_Rudd|http://dbpedia.org/resource/Kevin_Rudd
      "Tag",
      {{"id", boost::any(61)},
       {"name", boost::any(std::string("Kevin_Rudd"))},
       {"url",
           boost::any(std::string("http://dbpedia.org/resource/Kevin_Rudd"))}     
       });
  auto tag_1679 = graph->add_node(
    // 1679|Alexander_I_of_Russia|http://dbpedia.org/resource/Alexander_I_of_Russia
      "Tag",
      {{"id", boost::any(1679)},
       {"name", boost::any(std::string("Alexander_I_of_Russia"))},
       {"url",
           boost::any(std::string("http://dbpedia.org/resource/Alexander_I_of_Russia"))}     
       });
  auto uni_2213 = graph->add_node( 
    // id|type|name|url
    // 2213|university|Anhui_University_of_Science_and_Technology|
    // http://dbpedia.org/resource/Anhui_University_of_Science_and_Technology 
      "Organisation",
      {{"id", boost::any(2213)},
       {"type", boost::any(std::string("university"))},
       {"name", boost::any(std::string("Anhui_University_of_Science_and_Technology"))},
       {"url",
           boost::any(std::string("http://dbpedia.org/resource/Anhui_University_of_Science_and_Technology"))}     
       });
  auto company_915 = graph->add_node( 
    // id|type|name|url 
    // 915|company|Chang'an_Airlines|http://dbpedia.org/resource/Chang'an_Airlines
      "Organisation",
      {{"id", boost::any(915)},
       {"type", boost::any(std::string("company"))},
       {"name", boost::any(std::string("Chang'an_Airlines"))},
       {"url",
           boost::any(std::string("http://dbpedia.org/resource/Chang'an_Airlines"))}     
       });


  /**
   * Relationships for query interactive short #1
   */
  graph->add_relationship(mahinda, Kelaniya, ":isLocatedIn", {});
  graph->add_relationship(baruch, Gobi, ":isLocatedIn", {});
  

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
  graph->add_relationship(comment_16492676, bingbing, ":hasCreator", {});
  graph->add_relationship(lomana, bingbing, ":KNOWS", {});

  /**
   * Relationships for query update #1

  Person.id|Organisation.id|classYear
  2370|2213|2001

  Person.id|Organisation.id|workFrom
  2370|915|2002

  Person.id|Place.id
  2370|505

  Person.id|Tag.id
  2370|61
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
   * Relationships for query update #6

  Post.id|Person.id
  1374392536304|6597069777240

  Forum.id|Post.id
  549755871489|1374392536304

  Post.id|Place.id
  1374392536304|50

  Post.id|Tag.id
  1374392536304|1679
   */

  /**
   * Relationships for query update #7

  Comment.id|Person.id
  1649267442214|10995116283243

  Comment.id|Post.id
  1649267442214|1649267442210

  Comment.id|Place.id
  1649267442214|63

  Post.id|Tag.id
  1649267442214|1679
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
        {query_result("16492677"),
         query_result("I see"),
         query_result("2012-Jan-10 14:57:10"),
         query_result("19791"),
         query_result("Amin"),
         query_result("Kamkar"),
         query_result("false")});

    expected.data.push_back(
        {query_result("1642217"),
         query_result("maybe"),
         query_result("2012-Jan-10 06:31:18"),
         query_result("15393"),
         query_result("Lomana Trésor"),
         query_result("Kanam"),
         query_result("true")});

    ldbc_is_query_7(graph, rs);
    //std::cout << rs;
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #1") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[34]{birthday: 383097600, browser: \"Internet Explorer\", "
                      "creationDate: 1263715167, email: \"\"Yang2370@gmail.com\", \"Yang2370@hotmail.com\"\", firstName: \"Yang\", "
                      "gender: \"male\", id: 2370, language: \"\"zh\", \"en\"\", lastName: \"Zhu\", "
                      "locationIP: \"1.183.127.173\"}"),
        query_result("Place[15]{id: 505, name: \"Artux\", type: \"city\", url: \"http://dbpedia.org/resource/Artux\"}"),
        query_result("::isLocatedIn[18]{}"),
        query_result("Tag[30]{id: 61, name: \"Kevin_Rudd\", url: \"http://dbpedia.org/resource/Kevin_Rudd\"}"),
        query_result("::hasInterest[19]{}"),
        query_result("Organisation[32]{id: 2213, name: \"Anhui_University_of_Science_and_Technology\", type: \"university\", "
                      "url: \"http://dbpedia.org/resource/Anhui_University_of_Science_and_Technology\"}"),
        query_result("::studyAt[20]{classYear: 2001}"),
        query_result("Organisation[33]{id: 915, name: \"Chang'an_Airlines\", type: \"company\", "
                      "url: \"http://dbpedia.org/resource/Chang'an_Airlines\"}"),
        query_result("::workAt[21]{workFrom: 2002}") });
    
    ldbc_iu_query_1(graph, rs);
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #2") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[0]{birthday: 628646400, browser: \"Firefox\", "
                      "creationDate: 1266161530, firstName: \"Mahinda\", "
                      "gender: \"male\", id: 933, lastName: \"Perera\", "
                      "locationIP: \"119.235.7.103\"}"),
         query_result("Post[19]{browser: \"Firefox\", content: \"About Alexander I of Russia, "
                        "lexander tried to introduce liberal reforms, while in the second half\", "
                        "creationDate: 1345456288, id: 3627, imageFile: \"\", language: \"uz\", "
                        "length: 98, locationIP: \"14.205.203.83\"}"),
         query_result("::LIKES[18]{creationDate: 1266161530}")});
    
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
         query_result("Comment[25]{browser: \"Firefox\", content: \"Firefox|About Louis I of Hungary, "
                      "ittle lasting political results. Louis is theAbout Union of Sou\", "
                      "creationDate: 1326973191, id: 1642250, length: 89, "
                      "locationIP: \"85.154.120.237\"}"),
         query_result("::LIKES[18]{creationDate: 1327308990}")});
    
    ldbc_iu_query_3(graph, rs);
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #4") {
    result_set rs, expected;

    expected.append(
        {query_result("Forum[34]{creationDate: 1262412305, id: 53975, title: "
                      "\"Wall of Emperor of Brazil Silva\"}"),
        query_result("Person[10]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"), 
        query_result("::hasModerator[18]{}"),
        query_result("Tag[29]{id: 206, name: \"Charlemagne\", url: "
                      "\"http://dbpedia.org/resource/Charlemagne\"}"),
        query_result("::hasTag[19]{}") });
    
    ldbc_iu_query_4(graph, rs);
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #5") {
    result_set rs, expected;

    expected.append(
        {query_result("Person[10]{birthday: 467251200, browser: \"Chrome\", "
                      "creationDate: 1262412295, firstName: \"Emperor of Brazil\", "
                      "gender: \"female\", id: 1564, lastName: \"Silva\", "
                      "locationIP: \"192.223.88.63\"}"),
         query_result("Forum[27]{creationDate: 1266194787, id: 37, title: \"Wall of Hồ Chí Do\"}"),
         query_result("::hasMember[18]{creationDate: 1266916225}")});
    
    ldbc_iu_query_5(graph, rs);
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #6") {
    result_set rs, expected;

    expected.append(
        {query_result("Post[34]{browser: \"Safari\", content: \"About Alexander I of "
                        "Russia,  (23 December  1777 – 1 December  1825), (Russian: "
                        "Александр Благословенный, Aleksandr Blagoslovennyi, meaning Alexander the Bless\", "
                        "creationDate: 1315407147, id: 13439, imageFile: \"\", language: \"\"uz\"\", "
                        "length: 159, locationIP: \"46.19.159.176\"}"),
        query_result("Person[3]{birthday: 565315200, browser: \"Safari\", creationDate: 1282680826, "
                      "firstName: \"Fritz\", gender: \"female\", id: 65970697, lastName: \"Muller\", "
                      "locationIP: \"46.19.159.176\"}"),
        query_result("::hasCreator[18]{}"),
        query_result("Forum[28]{creationDate: 1285086335, id: 71489, "
                      "title: \"Group for Alexander_I_of_Russia in Umeå\"}"),
        query_result("::containerOf[19]{}"),
        query_result("Place[16]{id: 50, name: \"Germany\", type: \"country\", "
                      "url: \"http://dbpedia.org/resource/Germany\"}"),
        query_result("::isLocatedn[20]{}"),
        query_result("Tag[31]{id: 1679, name: \"Alexander_I_of_Russia\", "
                      "url: \"http://dbpedia.org/resource/Alexander_I_of_Russia\"}"),
        query_result("::hasTag[21]{}")});
    
    ldbc_iu_query_6(graph, rs);
    
    REQUIRE(rs == expected);
  }

  SECTION("query update #7") {
    result_set rs, expected;

    expected.append(
        {query_result("Comment[34]{browser: \"Chrome\", content: \"fine\", creationDate: 1326109755, "
                        "id: 442214, length: 4, locationIP: \"91.149.169.27\"}"),
        query_result("Person[11]{birthday: 631324800, browser: \"Chrome\", creationDate: 1293264475, "
                      "firstName: \"Ivan Ignatyevich\", gender: \"male\", id: 1043, lastName: \"Aleksandrov\", "
                      "locationIP: \"91.149.169.27\"}"),
        query_result("::hasCreator[18]{}"),
        query_result("Post[20]{browser: \"Internet Explorer\", content: \"About Louis I of Hungary, "
                      "dwig der Große, Bulgarian: Лудвиг I, Serbian: Лајош I Анжујски, Czech: Ludvík I. "
                      "Veliký, Li\", creationDate: 1326096328, id: 16492674, imageFile: \"\", "
                      "language: \"tk\", length: 117, locationIP: \"192.147.218.174\"}"),
        query_result("::replyOf[19]{}"),
        query_result("Place[17]{id: 63, name: \"Belarus\", type: \"country\", url: "
                      "\"http://dbpedia.org/resource/Belarus\"}"),
        query_result("::isLocatedn[20]{}"),
        query_result("Tag[31]{id: 1679, name: \"Alexander_I_of_Russia\", "
                      "url: \"http://dbpedia.org/resource/Alexander_I_of_Russia\"}"),
        query_result("::hasTag[21]{}")});
    
    ldbc_iu_query_7(graph, rs);
    
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
         query_result("::KNOWS[18]{creationDate: 1266916215}")});
    
    ldbc_iu_query_8(graph, rs);
    
    REQUIRE(rs == expected);
  }

#ifdef USE_TX
  graph->abort_transaction();
#endif

#ifdef USE_PMDK
  nvm::transaction::run(pop, [&] { nvm::delete_persistent<graph_db>(graph); });
  pop.close();
  remove(test_path.c_str());
#endif
}