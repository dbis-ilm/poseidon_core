include(../cmake/Testing.cmake.in)

include_directories(${CATCH_INCLUDE_DIR})

#=============================================
# do_test(dict_test)
# do_test(thread_pool_test)
# do_test(property_test)
# do_test(chunked_vec_test)
# do_test(node_test)
# do_test(graphdb_test)
# do_test(transaction_test)

file(GLOB_RECURSE ALL_TEST_CPP *.cpp)

foreach(ONE_TEST_CPP ${ALL_TEST_CPP})
    get_filename_component(ONE_TEST_EXEC ${ONE_TEST_CPP} NAME_WLE)
    do_test(${ONE_TEST_EXEC})
endforeach()