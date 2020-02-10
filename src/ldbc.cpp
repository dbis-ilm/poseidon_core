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
    					 PExpr_(1, pj::int_property(res, "creationDate")),
    					 PExpr_(1, pj::string_property(res, "relship_pr")) })
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
				/*.nodes_where("Message", "id",
                       [&](auto &m) {std::string msgId = "1374389534791"; // overflows as int
                       				 dcode_t msgIdCode = gdb->get_dictionary()->lookup_string(msgId);
                       				 return m.equal(msgIdCode); })*/
				.nodes_where("Post", "id",
                       [&](auto &m) { return m.equal(postId); })
				.project({ 
          // TODO: project node property (datestring) 
					//PExpr_(0, pj::int_to_dtimestring(pj::int_property(res, "creationDate")))
          PExpr_(0, pj::int_property(res, "creationDate")) })
					/*PExpr_(!pj::string_property(res, "content").empty() ? 
						(0, pj::string_property(res, "content")) : (0, pj::string_property(res, "imageFile")); 
                        })*/
        .collect(rs);
				
	q.start();
	rs.wait();
}

void ldbc_is_query_5(graph_db_ptr &gdb, result_set &rs) {
	auto commentId = 12362343;

	auto q = query(gdb)
				/*.nodes_where("Message", "id",
                       [&](auto &m) {std::string commentId = "1236950581249"; 
                       				 dcode_t commentIdCode = gdb->get_dictionary()->lookup_string(commentId);
                       				 return c.equal(commentIdCode); })*/
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

/*void ldbc_is_query_6(graph_db_ptr &gdb, result_set &rs) {
    auto msgId = 16492677;
    auto msgLength = 117;
     
    auto q = query(gdb)
                  .nodes_where("Message", "id",
                  	[&](auto &m) { return m.equal(msgId); })
                  .from_variable_relationships("replyOf", 1, 10)
                  .to_node("Message")
				  .property("length", [&] (auto &m) { return m.equal(msgLength); })*/
				  /*.property("type", [&] (auto &m) {std::string msgType = "Post";
				  									 dcode_t msgTypeCode = gdb->get_dictionary()->lookup_string(msgType);
				  									 return m.equal(msgTypeCode); }) */// m.equal(dcode_t) does no return the query result
				  /*.to_relationships(":containerOf")
				  .from_node("Forum")
				  .from_relationships(":hasModerator")
				  .to_node("Person")
				  .project({
                    PF_(pj::int_property(res[4], "id")),
				  	PF_(pj::string_property(res[4], "title")),
				  	PF_(pj::int_property(res[6], "id")),
				  	PF_(pj::string_property(res[6], "firstName")),
				  	PF_(pj::string_property(res[6], "lastName"))})
				  .collect(rs);
	
	q.start();
	rs.wait();
}*/

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