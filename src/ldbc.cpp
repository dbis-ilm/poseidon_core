#include "ldbc.hpp"
#include "qop.hpp"
#include "query.hpp"

namespace pj = builtin;

void ldbc_is_query_1(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("933");
  
  auto q = query(gdb)
               .nodes_where("Person", "id",
                  [&](auto &p) { 
                        auto id = gdb->get_code(personId);
                        assert(id != 0);
                        return p.equal(id); })
               .from_relationships(":isLocatedIn")
               .to_node("Place")
               .project({PExpr_(0, pj::string_property(res, "firstName")),
                         PExpr_(0, pj::string_property(res, "lastName")),
                         PExpr_(0, pj::int_to_datestring(
                                       pj::int_property(res, "birthday"))),
                         PExpr_(0, pj::string_property(res, "locationIP")),
                         PExpr_(0, pj::string_property(res, "browserUsed")),
                         PExpr_(2, pj::string_property(res, "id")),
                         PExpr_(0, pj::string_property(res, "gender")),
                         PExpr_(0, pj::string_property(res, "creationDate"))//,
                         //PExpr_(0, pj::int_to_dtimestring(
                           //            pj::int_property(res, "creationDate")))
                        })
                .collect(rs);
  q.start();
  rs.wait();
}

void ldbc_is_query_2(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("65");
  auto maxHops = 10; 

  auto q1 = query(gdb)
               .nodes_where("Person", "id",
                  [&](auto &p) { 
                        auto id = gdb->get_code(personId);
                        assert(id != 0);
                        return p.equal(id); })
               .to_relationships(":hasCreator")
               .limit(10)
               .from_node("Comment")
               .from_relationships({1, maxHops}, ":replyOf") 
               .to_node("Post")
               .from_relationships(":hasCreator")
               .to_node("Person")
               .project({PExpr_(2, pj::string_property(res, "id")),
                        PExpr_(2, pj::string_property(res, "content")),
                        PExpr_(2, pj::string_property(res, "creationDate")),
                        //PExpr_(2, pj::int_to_dtimestring(
                          //             pj::int_property(res, "creationDate"))),
                        PExpr_(4, pj::string_property(res, "id")),
                        PExpr_(6, pj::string_property(res, "id")),
                        PExpr_(6, pj::string_property(res, "firstName")),
                        PExpr_(6, pj::string_property(res, "lastName")) })
               .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                        auto &id1 = boost::get<std::string>(qr1[0]);
                        auto &id2 = boost::get<std::string>(qr2[0]);
                        uint64_t fid1 = (uint64_t)std::stoll(id1);
                        uint64_t fid2 = (uint64_t)std::stoll(id2);
                        if (boost::get<std::string>(qr1[2]) == boost::get<std::string>(qr2[2]))
                          return fid1 > fid2;
                        return boost::get<std::string>(qr1[2]) > boost::get<std::string>(qr2[2]); 
                        })
               .collect(rs);

  auto q2 = query(gdb)
               .nodes_where("Person", "id",
                  [&](auto &p) { 
                        auto id = gdb->get_code(personId);
                        assert(id != 0);
                        return p.equal(id); })
               .to_relationships(":hasCreator")
               .limit(10)
               .from_node("Post")
               .project({PExpr_(2, pj::string_property(res, "id")),
                        PExpr_(2, !pj::string_property(res, "content").empty() ? 
                            pj::string_property(res, "content") : pj::string_property(res, "imageFile")),
                        //PExpr_(2, pj::int_to_dtimestring(
                          //             pj::int_property(res, "creationDate"))),
                        PExpr_(2, pj::string_property(res, "creationDate")),
                        PExpr_(2, pj::string_property(res, "id")),
                        PExpr_(0, pj::string_property(res, "id")),
                        PExpr_(0, pj::string_property(res, "firstName")),
                        PExpr_(0, pj::string_property(res, "lastName")) })
               .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) { 
                        auto &id1 = boost::get<std::string>(qr1[0]);
                        auto &id2 = boost::get<std::string>(qr2[0]);
                        uint64_t fid1 = (uint64_t)std::stoll(id1);
                        uint64_t fid2 = (uint64_t)std::stoll(id2);
                        if (boost::get<std::string>(qr1[2]) == boost::get<std::string>(qr2[2]))
                          return fid1 > fid2;
                        return boost::get<std::string>(qr1[2]) > boost::get<std::string>(qr2[2]); })
               .collect(rs);

  query::start({&q2, &q1});
  rs.wait();
}

void ldbc_is_query_3(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("933");

  auto q = query(gdb)
                .nodes_where("Person", "id",
                  [&](auto &p) { 
                          auto id = gdb->get_code(personId);
                          assert(id != 0);
                          return p.equal(id); })
                .from_relationships(":knows")
                .to_node("Person")
                .project({PExpr_(2, pj::string_property(res, "id")),
                          PExpr_(2, pj::string_property(res, "firstName")),
                          PExpr_(2, pj::string_property(res, "lastName")),
                          PExpr_(1, pj::string_property(res, "creationDate"))
                          //PExpr_(1, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))) 
                          })
                .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                          auto &id1 = boost::get<std::string>(qr1[0]);
                          auto &id2 = boost::get<std::string>(qr2[0]);
                          uint64_t fid1 = (uint64_t)std::stoll(id1);
                          uint64_t fid2 = (uint64_t)std::stoll(id2);
                          if (boost::get<std::string>(qr1[3]) == boost::get<std::string>(qr2[3]))
                            return fid1 < fid2;
                          return boost::get<std::string>(qr1[3]) > boost::get<std::string>(qr2[3]); })
                .collect(rs);
  
  q.start();
  rs.wait();
}

void ldbc_is_query_4(graph_db_ptr &gdb, result_set &rs) {
  auto postId = std::string("1374389534791");

	auto q = query(gdb)
                .nodes_where("Post", "id",
                  [&](auto &p) { 
                          auto id = gdb->get_code(postId);
                          assert(id != 0);
                          return p.equal(id); })
                .project({PExpr_(0, pj::string_property(res, "creationDate")), 
                          //PExpr_(0, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))),
                          PExpr_(0, !pj::string_property(res, "content").empty() ? 
                            pj::string_property(res, "content") : pj::string_property(res, "imageFile")) })
                .collect(rs);
				
	q.start();
	rs.wait();
}

void ldbc_is_query_5(graph_db_ptr &gdb, result_set &rs) {
  auto commentId = std::string("1236950581249");

	auto q = query(gdb)
                .nodes_where("Comment", "id",
                  [&](auto &c) { 
                          auto id = gdb->get_code(commentId);
                          assert(id != 0);
                          return c.equal(id); })
                .from_relationships(":hasCreator")
                .to_node("Person")
                .project({PExpr_(2, pj::string_property(res, "id")),
                          PExpr_(2, pj::string_property(res, "firstName")),
                          PExpr_(2, pj::string_property(res, "lastName")) })
                .collect(rs);
	q.start();
	rs.wait();
}

void ldbc_is_query_6(graph_db_ptr &gdb, result_set &rs) {
  auto postId = std::string("1649267442210");
  auto commentId = std::string("1649267442213");
  auto maxHops = 10;
    
  auto q1 = query(gdb)
                .nodes_where("Post", "id",
                  [&](auto &p) { 
                          auto id = gdb->get_code(postId);
                          assert(id != 0);
                          return p.equal(id); })
                .to_relationships(":containerOf")
                .from_node("Forum")
                .from_relationships(":hasModerator")
                .to_node("Person")
                .project({PExpr_(2, pj::string_property(res, "id")),
                          PExpr_(2, pj::string_property(res, "title")),
                          PExpr_(4, pj::string_property(res, "id")),
                          PExpr_(4, pj::string_property(res, "firstName")),
                          PExpr_(4, pj::string_property(res, "lastName")) })
                .collect(rs);
  
  auto q2 = query(gdb)
                .nodes_where("Comment", "id",
                  [&](auto &c) { 
                          auto id = gdb->get_code(commentId);
                          assert(id != 0);
                          return c.equal(id); })
                .from_relationships({1, maxHops}, ":replyOf") 
                .to_node("Post")
                .to_relationships(":containerOf")
                .from_node("Forum")
                .from_relationships(":hasModerator")
                .to_node("Person")
                .project({PExpr_(4, pj::string_property(res, "id")),
                          PExpr_(4, pj::string_property(res, "title")),
                          PExpr_(6, pj::string_property(res, "id")),
                          PExpr_(6, pj::string_property(res, "firstName")),
                          PExpr_(6, pj::string_property(res, "lastName")) })
                .collect(rs);
	
  query::start({&q1, &q2});
	rs.wait(); 
}

void ldbc_is_query_7(graph_db_ptr &gdb, result_set &rs) {
  auto commentId = std::string("1649267442212");
     
  auto q1 = query(gdb)
                .nodes_where("Comment", "id",
                  [&](auto &c) { 
                          auto id = gdb->get_code(commentId);
                          assert(id != 0);
                          return c.equal(id); })
                .from_relationships(":hasCreator")
                .to_node("Person");
  
  auto q2 = query(gdb)
                .nodes_where("Comment", "id",
                  [&](auto &c) { 
                          auto id = gdb->get_code(commentId);
                          assert(id != 0);
                          return c.equal(id); })
                .to_relationships(":replyOf")    
                .from_node("Comment")
                .from_relationships(":hasCreator")
                .to_node("Person")
                .outerjoin({4, 2}, q1)
                .project({PExpr_(2, pj::string_property(res, "id")),
                          PExpr_(2, pj::string_property(res, "content")),
                          PExpr_(2, pj::string_property(res, "creationDate")), 
                          //PExpr_(2, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))),
                          PExpr_(4, pj::string_property(res, "id")),
                          PExpr_(4, pj::string_property(res, "firstName")),
                          PExpr_(4, pj::string_property(res, "lastName")),
                          PExpr_(8, res.type() == typeid(rship_description) ?
                                      std::string("true") : std::string("false")) })
                .orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                        auto &id1 = boost::get<std::string>(qr1[0]);
                        auto &id2 = boost::get<std::string>(qr2[0]);
                        uint64_t fid1 = (uint64_t)std::stoll(id1);
                        uint64_t fid2 = (uint64_t)std::stoll(id2);
                        if (boost::get<std::string>(qr1[2]) == boost::get<std::string>(qr2[2]))
                          return fid1 > fid2;
                        return boost::get<std::string>(qr1[3]) < boost::get<std::string>(qr2[3]); })
                .collect(rs);

  query::start({&q1, &q2});
	rs.wait();
}

void ldbc_iu_query_1(graph_db_ptr &gdb, result_set &rs) {
  uint64_t personId = 9999999999999;
  auto fName = std::string("New");
  auto lName = std::string("Person");
  auto gender = std::string("female"); 
  auto birthday = pj::datestring_to_int("1981-01-21");
  //auto creationDate = pj::dtimestring_to_int("2011-01-11 01:51:21.746");
  auto creationDate = std::string("2011-01-11 01:51:21.746");
  auto locationIP = std::string("1.183.127.173"); 
  auto browser = std::string("Safari");
  auto cityId = std::string("505");
  auto language = std::string("\"zh\", \"en\""); 
  auto email = std::string("\"new1@email1.com\", \"new@email2.com\"");  
  auto tagId = std::string("61");
  auto uniId = std::string("2213");
  auto classYear = 2001;
  auto companyId = std::string("915");
  auto workFrom = 2001;

  auto q1 = query(gdb).nodes_where("Place", "id",
                        [&](auto &c) { 
                              auto id = gdb->get_code(cityId);
                              assert(id != 0);
                              return c.equal(id); });

  auto q2 = query(gdb).nodes_where("Tag", "id",
                        [&](auto &t) {
                              auto id = gdb->get_code(tagId);
                              assert(id != 0);
                              return t.equal(id); });

  auto q3 = query(gdb).nodes_where("Organisation", "id",
                        [&](auto &u) {
                              auto id = gdb->get_code(uniId);
                              assert(id != 0);
                              return u.equal(id); });

  auto q4 = query(gdb).nodes_where("Organisation", "id",
                        [&](auto &c) {
                              auto id = gdb->get_code(companyId);
                              assert(id != 0);
                              return c.equal(id); });

  auto q5 = query(gdb).create("Person",
                              {{"id", boost::any(personId)},
                              {"firstName", boost::any(fName)},
                              {"lastName", boost::any(lName)},
                              {"gender", boost::any(gender)},
                              {"birthday", boost::any(birthday)},
                              {"creationDate", boost::any(creationDate)},
                              {"locationIP", boost::any(locationIP)},
                              {"browserUsed", boost::any(browser)},
                              {"language", boost::any(language)},
                              {"email", boost::any(email)}})
                      .crossjoin(q1)
                      .create_rship({0, 1}, ":isLocatedIn", {})
                      .crossjoin(q2)
                      .create_rship({0, 3}, ":hasInterest", {})
                      .crossjoin(q3)
                      .create_rship({0, 5}, ":studyAt", {{"classYear", boost::any(classYear)}})
                      .crossjoin(q4)
                      .create_rship({0, 7}, ":workAt", {{"workFrom", boost::any(workFrom)}})
                      .collect(rs);

  query::start({&q1, &q2, &q3, &q4, &q5});
}

void ldbc_iu_query_2(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("933");
  auto postId = std::string("2061587303627"); 
  auto creationDate = std::string("2010-02-14 15:32:10.447");

  auto q1 = query(gdb).nodes_where("Post", "id",
                        [&](auto &p) {
                              auto id = gdb->get_code(postId);
                              assert(id != 0);
                              return p.equal(id); });

  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) {
                              auto id = gdb->get_code(personId);
                              assert(id != 0);
                              return p.equal(id); })
          .crossjoin(q1)
          .create_rship({0, 1}, ":likes", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void ldbc_iu_query_3(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("1564");
  auto commentId = std::string("1649267442250");
  auto creationDate = std::string("2012-01-23 08:56:30.617");

  auto q1 = query(gdb).nodes_where("Comment", "id",
                        [&](auto &c) {
                            auto id = gdb->get_code(commentId);
                            assert(id != 0);
                            return c.equal(id); });
  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) {
                            auto id = gdb->get_code(personId);
                            assert(id != 0);
                            return p.equal(id); })
          .crossjoin(q1)
          .create_rship({0, 1}, ":likes", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void ldbc_iu_query_4(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("1564");
  auto tagId = std::string("206");
  uint64_t forumId = 53975;
  auto title = std::string("Wall of Emperor of Brazil Silva");
  auto creationDate = std::string("2010-01-02 06:05:05.320");

  auto q1 = query(gdb).nodes_where("Person", "id",
                        [&](auto &p) {
                              auto id = gdb->get_code(personId);
                              assert(id != 0);
                              return p.equal(id); });

  auto q2 = query(gdb).nodes_where("Tag", "id",
                        [&](auto &t) {
                              auto id = gdb->get_code(tagId);
                              assert(id != 0);
                              return t.equal(id); });

  auto q3 = query(gdb).create("Forum",
                              {{"id", boost::any(forumId)},
                              {"title", boost::any(title)},
                              {"creationDate", boost::any(creationDate)} })
                      .crossjoin(q1)
                      .create_rship({0, 1}, ":hasModerator", {})
                      .crossjoin(q2)
                      .create_rship({0, 3}, ":hasTag", {})
                      .collect(rs);

  query::start({&q1, &q2, &q3});
}

void ldbc_iu_query_5(graph_db_ptr &gdb, result_set &rs) {
  auto personId = std::string("1564");
  auto forumId = std::string("37");
  auto joinDate = std::string("2010-02-23 09:10:25.466");

  auto q1 = 
      query(gdb)
          .nodes_where("Person", "id",
            [&](auto &p) {
              auto id = gdb->get_code(personId);
              assert(id != 0);
              return p.equal(id); });

  auto q2 =
      query(gdb)
          .nodes_where("Forum", "id",
            [&](auto &f) {
              auto id = gdb->get_code(forumId);
              assert(id != 0);
              return f.equal(id); })
          .crossjoin(q1)
          .create_rship({0, 1}, ":hasMember", {{"creationDate", boost::any(joinDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void ldbc_iu_query_6(graph_db_ptr &gdb, result_set &rs) {
  uint64_t postId = 13439;  
  auto imageFile = std::string("");
  auto creationDate = std::string("2011-09-07 14:52:27.809");
  auto locationIP = std::string("46.19.159.176"); 
  auto browser = std::string("Safari");
  auto language = std::string("\"uz\""); 
  auto content = std::string("About Alexander I of Russia,  (23 December  1777 – 1 December  1825), (Russian: "
                            "Александр Благословенный, Aleksandr Blagoslovennyi, meaning Alexander the Bless"); 
  auto length = 159;
  auto personId = std::string("6597069777240");
  auto forumId = std::string("549755871489"); 
  auto countryId = std::string("50");
  auto tagId = std::string("1679");

  auto q1 = query(gdb)
                .nodes_where("Person", "id",
                  [&](auto &p) {
                              auto id = gdb->get_code(personId);
                              assert(id != 0);
                              return p.equal(id); });

  auto q2 = query(gdb).nodes_where("Forum", "id",
                  [&](auto &f) {
                              auto id = gdb->get_code(forumId);
                              assert(id != 0);
                              return f.equal(id); });

  auto q3 = query(gdb).nodes_where("Place", "id",
                  [&](auto &c) {
                              auto id = gdb->get_code(countryId);
                              assert(id != 0);
                              return c.equal(id); });

  auto q4 = query(gdb).nodes_where("Tag", "id",
                  [&](auto &t) {
                              auto id = gdb->get_code(tagId);
                              assert(id != 0);
                              return t.equal(id); });

  auto q5 = query(gdb).create("Post",
                            {{"id", boost::any(postId)}, 
                              {"imageFile", boost::any(imageFile)},
                              {"creationDate", boost::any(creationDate)},
                              {"locationIP", boost::any(locationIP)},
                              {"browser", boost::any(browser)},
                              {"language", boost::any(language)}, 
                              {"content", boost::any(content)},
                              {"length", boost::any(length)} })
                      .crossjoin(q1)
                      .create_rship({0, 1}, ":hasCreator", {})
                      .crossjoin(q2)
                      .create_rship({3, 0}, ":containerOf", {})
                      .crossjoin(q3)
                      .create_rship({0, 5}, ":isLocatedn", {})
                      .crossjoin(q4)
                      .create_rship({0, 7}, ":hasTag", {})
                      .collect(rs);

  query::start({&q1, &q2, &q3, &q4, &q5});
}

void ldbc_iu_query_7(graph_db_ptr &gdb, result_set &rs) {
  uint64_t commentId = 442214; 
  auto creationDate = std::string("2012-01-09 11:49:15.991");
  auto locationIP = std::string("91.149.169.27"); 
  auto browser = std::string("Chrome");
  auto content = std::string("fine"); 
  auto length = 4;
  auto personId = std::string("10995116283243");
  auto postId = std::string("1649267442210"); 
  auto countryId = std::string("63");
  auto tagId = std::string("1679");

  auto q1 = query(gdb).nodes_where("Person", "id",
                  [&](auto &p) {
                              auto id = gdb->get_code(personId);
                              assert(id != 0);
                              return p.equal(id); });

  auto q2 = query(gdb).nodes_where("Post", "id",
                  [&](auto &f) {
                              auto id = gdb->get_code(postId);
                              assert(id != 0);
                              return f.equal(id); });

  auto q3 = query(gdb).nodes_where("Place", "id",
                  [&](auto &c) {
                              auto id = gdb->get_code(countryId);
                              assert(id != 0);
                              return c.equal(id); });

  auto q4 = query(gdb).nodes_where("Tag", "id",
                  [&](auto &t) {
                              auto id = gdb->get_code(tagId);
                              assert(id != 0);
                              return t.equal(id); });

  auto q5 = query(gdb).create("Comment",
                              {{"id", boost::any(commentId)},
                              {"creationDate", boost::any(creationDate)},
                              {"locationIP", boost::any(locationIP)},
                              {"browser", boost::any(browser)},
                              {"content", boost::any(content)},
                              {"length", boost::any(length)} })
                      .crossjoin(q1)
                      .create_rship({0, 1}, ":hasCreator", {})
                      .crossjoin(q2)
                      .create_rship({0, 3}, ":replyOf", {})
                      .crossjoin(q3)
                      .create_rship({0, 5}, ":isLocatedn", {})
                      .crossjoin(q4)
                      .create_rship({0, 7}, ":hasTag", {})
                      .collect(rs);

  query::start({&q1, &q2, &q3, &q4, &q5});
}

void ldbc_iu_query_8(graph_db_ptr &gdb, result_set &rs) {
  auto personId_1 = std::string("4194");
  auto personId_2 = std::string("1564");
  auto creationDate = std::string("2010-02-23 09:10:15.466");

  auto q1 = 
      query(gdb)
          .nodes_where("Person", "id",
            [&](auto &p) {
              auto id = gdb->get_code(personId_2);
              assert(id != 0);
              return p.equal(id); });
  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
            [&](auto &p) {
              auto id = gdb->get_code(personId_1);
              assert(id != 0);
              return p.equal(id); })
          .crossjoin(q1)
          .create_rship({0, 1}, ":KNOWS", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void run_ldbc_queries(graph_db_ptr &gdb) {
  // the query set
  /*std::function<void(graph_db_ptr &, result_set &)> query_set[] = {
      ldbc_is_query_1, ldbc_is_query_2, ldbc_is_query_3, 
      ldbc_is_query_4, ldbc_is_query_5, ldbc_is_query_6, ldbc_is_query_7,
      ldbc_iu_query_1, ldbc_iu_query_2, ldbc_iu_query_3, ldbc_iu_query_4,
      ldbc_iu_query_5, ldbc_iu_query_6, ldbc_iu_query_7, ldbc_iu_query_8};*/
    
  std::function<void(graph_db_ptr &, result_set &)> query_set[] = {ldbc_is_query_1};
  std::size_t qnum = 1;

  // for each query we measure the time and run it in a transaction
  for (auto f : query_set) {
    result_set rs;
    auto start_qp = std::chrono::steady_clock::now();

    auto tx = gdb->begin_transaction();
    f(gdb, rs);
    gdb->commit_transaction();

    auto end_qp = std::chrono::steady_clock::now();
    std::cout << "Query #" << qnum++ << " executed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_qp -
                                                                       start_qp)
                     .count()
              << " ms" << std::endl;
  }
}