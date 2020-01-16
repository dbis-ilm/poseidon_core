include(../cmake/Testing.cmake.in)

include_directories(${CATCH_INCLUDE_DIR})

#=============================================
do_test(dict_test)
do_test(thread_pool_test)
do_test(property_test)
do_test(chunked_vec_test)
do_test(node_test)
do_test(graphdb_test)
# do_test(cypher_test)
# do_test(lua_test)
# do_test(ldbc_test)
do_test(transaction_test)
