#ifndef ldbc_hpp_
#define ldbc_hpp_

#include "graph_db.hpp"
#include "qop.hpp"

void run_ldbc_queries(graph_db_ptr &gdb);

/* interactive short queries */
void ldbc_is_query_1(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_2(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_3(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_4(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_5(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_6(graph_db_ptr &gdb, result_set &rs);
void ldbc_is_query_7(graph_db_ptr &gdb, result_set &rs);


/* interactive update queries */
void ldbc_iu_query_1(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_2(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_3(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_4(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_5(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_6(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_7(graph_db_ptr &gdb, result_set &rs);
void ldbc_iu_query_8(graph_db_ptr &gdb, result_set &rs);

#endif