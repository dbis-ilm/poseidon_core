#include "ldbc.hpp"
#include "qop.hpp"
#include "query.hpp"

namespace pj = builtin;

void ldbc_is_query_1(graph_db_ptr &gdb, result_set &rs) {
  auto personId = 933;
  auto q = query(gdb)
               .nodes_where("Person", "id",
                            [&](auto &p) { return p.equal(personId); })
               .from_relationships(":isLocatedIn")
               .to_node("Place")
               .project({PExpr_(0, pj::string_property(res, "firstName")),
                         PExpr_(0, pj::string_property(res, "lastName")),
                         PExpr_(0, pj::int_to_datestring(
                                       pj::int_property(res, "birthday"))),
                         PExpr_(0, pj::string_property(res, "locationIP")),
                         PExpr_(0, pj::string_property(res, "browser")),
                         PExpr_(2, pj::int_property(res, "id")),
                         PExpr_(0, pj::string_property(res, "gender")),
                         PExpr_(0, pj::int_to_dtimestring(
                                       pj::int_property(res, "creationDate")))})
               .collect(rs);
  q.start();
  rs.wait();
}

void ldbc_is_query_2(graph_db_ptr &gdb, result_set &rs) {
  auto q = query(gdb).all_nodes().from_relationships().limit(10).collect(rs);

  q.start();
  rs.wait();
}

void ldbc_is_query_3(graph_db_ptr &gdb, result_set &rs) {
	auto personId = 933;

    auto q = query(gdb)
    			.nodes_where("Person", "id",
                       [&](auto &p) { return p.equal(personId); })
    			.from_relationships(":knows")
    			.to_node()
				  .has_label("Person")
    			.project({PExpr_(2, pj::int_property(res, "id")),
    					 PExpr_(2, pj::string_property(res, "firstName")),
    					 PExpr_(2, pj::string_property(res, "lastName")), 
                // TODO: project relationship property (datestring) 
    					 //PExpr_(1, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))), 
    					 PExpr_(1, pj::int_property(res, "creationDate")) })
    			.orderby([&](const qr_tuple &qr1, const qr_tuple &qr2) {
                   if(boost::get<int>(qr1[3]) == boost::get<int>(qr2[3]))
                   		return boost::get<int>(qr1[0]) < boost::get<int>(qr2[0]);
                   return boost::get<int>(qr1[3]) > boost::get<int>(qr2[3]);
                 })
    			.collect(rs);
    q.start();

    rs.wait();
}

void ldbc_is_query_4(graph_db_ptr &gdb, result_set &rs) {
	auto postId = 13743895;

	auto q = query(gdb)
				.nodes_where("Post", "id",
                       [&](auto &m) { return m.equal(postId); })
				.project({ 
          // TODO: project node property (datestring) 
					//PExpr_(0, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))),
          PExpr_(0, pj::int_property(res, "creationDate")),
					PExpr_(0, !pj::string_property(res, "content").empty() ? 
						pj::string_property(res, "content") : pj::string_property(res, "imageFile")) })
        .collect(rs);
				
	q.start();
	rs.wait();
}

void ldbc_is_query_5(graph_db_ptr &gdb, result_set &rs) {
	auto commentId = 12362343;

	auto q = query(gdb)
				.nodes_where("Comment", "id",
                       [&](auto &c) { return c.equal(commentId); })
				.from_relationships(":hasCreator")
				.to_node("Person")
				.project({
					PExpr_(2, pj::int_property(res, "id")),
					PExpr_(2, pj::string_property(res, "firstName")),
					PExpr_(2, pj::string_property(res, "lastName")) })
				.collect(rs);
	q.start();
	rs.wait();
}

void ldbc_is_query_6(graph_db_ptr &gdb, result_set &rs) {
    auto commentId = 16492677;
    auto commentLength = 117;
     
    auto q = query(gdb)
                  .nodes_where("Comment", "id",
                  	[&](auto &c) { return c.equal(commentId); })
                  .from_relationships({1, 3}, ":replyOf") //     if (relship.rship_label != lcode)  continue;
                  .to_node("Post")
				          .to_relationships(":containerOf")
				          .from_node("Forum")
				          .from_relationships(":hasModerator")
				          .to_node("Person")
				          /*.project({
                    PExpr_(4, pj::int_property(res, "id")),
                    PExpr_(4, pj::string_property(res, "title")),
                    PExpr_(4, pj::int_property(res, "id")),
                    PExpr_(6, pj::string_property(res, "firstName")),
                    PExpr_(6, pj::string_property(res, "lastname")) 
                    }) */
                  .collect(rs);
	
	q.start();
	rs.wait();
}

void ldbc_is_query_7(graph_db_ptr &gdb, result_set &rs) {
    auto commentId = 16492676;
     
    auto q = query(gdb)
                  .nodes_where("Comment", "id",
                  	[&](auto &c) { return c.equal(commentId); })
                  .to_relationships(":replyOf") //     
                  .from_node("Comment")
				          .from_relationships(":hasCreator")
				          .to_node("Person")
				          .project({
                    PExpr_(2, pj::int_property(res, "id")),
                    PExpr_(2, pj::string_property(res, "content")),
                    // TODO: project node property (datestring)
                    //PExpr_(0, pj::int_to_dtimestring(pj::int_property(res, "creationDate")))
                    PExpr_(0, pj::int_to_dtimestring(pj::int_property(res, "creationDate"))),
                    /*PExpr_(0, pj::int_property(res, "id")),
                    PExpr_(4, pj::string_property(res, "firstName")),
                    PExpr_(4, pj::string_property(res, "lastname"))*/
                    })
                  .collect(rs);
	
	q.start();
	rs.wait();
}

void ldbc_iu_query_2(graph_db_ptr &gdb, result_set &rs) {
  auto personId = 933;
  auto postId = 656; 
  auto creationDate = pj::dtimestring_to_int("2010-02-14 15:32:10.447");

  auto q1 = query(gdb).nodes_where("Post", "id",
                                   [&](auto &p) { return p.equal(postId); });

  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) { return p.equal(personId); })
          .crossjoin(q1)
          .create_rship("LIKES", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});

  // boost::bad_get: failed value get using boost::get
  /*auto placeId = 1353;
  auto q1 = query(gdb).nodes_where("Place", "id",
                                   [&](auto &p) { return p.equal(placeId); })
                      .from_relationships(":hasTested") 
                      .to_node("Post")
                      .property("id",
                                   [&](auto &p) { return p.equal(postId); });*/
}

void ldbc_iu_query_3(graph_db_ptr &gdb, result_set &rs) {
  auto personId = 1564;
  auto commentId = 1642250;
  auto creationDate = pj::dtimestring_to_int("2012-01-23 08:56:30.617");

  auto q1 = query(gdb).nodes_where("Comment", "id",
                                   [&](auto &c) { return c.equal(commentId); });
  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) { return p.equal(personId); })
          .crossjoin(q1)
          .create_rship("LIKES", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void ldbc_iu_query_4(graph_db_ptr &gdb, result_set &rs) {
  auto personId = 1564;
  auto tagId = 206;
  auto forumId = 53975;
  auto title = std::string("Wall of Emperor of Brazil Silva");
  auto creationDate = pj::dtimestring_to_int("2010-01-02 06:05:05.320");

  auto q1 = query(gdb).create("Forum",
                              {{"id", boost::any(53975)},
                              {"title", boost::any(std::string("Wall of Emperor of Brazil Silva"))},
                              {"creationDate",
                                boost::any(builtin::dtimestring_to_int("2010-01-02 06:05:05.320"))}      
                              })
                      .collect(rs);

  result_set rs2;
  auto q2 = query(gdb).nodes_where("Forum", "id",
                                   [&](auto &p) { return p.equal(forumId); })
                      .collect(rs2);

  q1.start();
  rs.wait();

  q2.start();
  rs2.wait();
  // we need to invoke consume_ in the update operator
  /*auto q2 = query(gdb).nodes_where("Person", "id",
                                   [&](auto &p) { return p.equal(personId); });

  auto q3 = query(gdb).nodes_where("Tag", "id",
                                   [&](auto &t) { return t.equal(tagId); });

  auto q4 =
      query(gdb)
          .nodes_where("Forum", "id",
                       [&](auto &f) { return f.equal(forumId); })
          .crossjoin(q2)
          .create_rship(":hasModerator", {})
          .collect(rs);

  auto q5 =
      query(gdb)
          .nodes_where("Forum", "id",
                       [&](auto &f) { return f.equal(forumId); })
          .crossjoin(q3)
          .create_rship(":hasTag", {})
          .collect(rs);

  query::start({&q4, &q2});
  query::start({&q5, &q3});*/
}

void ldbc_iu_query_5(graph_db_ptr &gdb, result_set &rs) {
  auto personId = 1564;
  auto forumId = 37;
  auto joinDate = pj::dtimestring_to_int("2010-02-23 09:10:25.466");

  auto q1 = query(gdb).nodes_where("Forum", "id",
                                   [&](auto &f) { return f.equal(forumId); });
  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) { return p.equal(personId); })
          .crossjoin(q1)
          .create_rship(":hasMember", {{"creationDate", boost::any(joinDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void ldbc_iu_query_8(graph_db_ptr &gdb, result_set &rs) {
  auto personId_1 = 1564;
  auto personId_2 = 4194;
  auto creationDate = pj::dtimestring_to_int("2010-02-23 09:10:15.466");

  auto q1 = query(gdb).nodes_where("Person", "id",
                                   [&](auto &p) { return p.equal(personId_2); });
  auto q2 =
      query(gdb)
          .nodes_where("Person", "id",
                       [&](auto &p) { return p.equal(personId_1); })
          .crossjoin(q1)
          .create_rship("KNOWS", {{"creationDate", boost::any(creationDate)}})
          .collect(rs);

  query::start({&q1, &q2});
}

void run_ldbc_queries(graph_db_ptr &gdb) {
  // the query set
  std::function<void(graph_db_ptr &, result_set &)> query_set[] = {
      ldbc_is_query_1, ldbc_is_query_2};
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