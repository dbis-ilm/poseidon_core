include(../cmake/Testing.cmake.in)

include_directories(${catch2_SOURCE_DIR}/include)

#=============================================

file(GLOB_RECURSE ALL_TEST_CPP *_test.cpp)

foreach(ONE_TEST_CPP ${ALL_TEST_CPP})
    get_filename_component(ONE_TEST_EXEC ${ONE_TEST_CPP} NAME_WLE)
    do_test(${ONE_TEST_EXEC})
endforeach()

#=============================================

add_executable(abort_insert
  abort_insert.cpp
 )

target_link_libraries(abort_insert
  poseidon_core
  Catch2::Catch2WithMain
  fmt::fmt
  ${PMDK_LIBRARIES}
)

#=============================================

add_test (NAME python-tests
  COMMAND ${Python3_EXECUTABLE} -m pytest ${CMAKE_CURRENT_SOURCE_DIR}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/test
)