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