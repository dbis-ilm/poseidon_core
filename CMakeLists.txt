cmake_minimum_required(VERSION 3.20)
project(poseidon_core)

if(${CMAKE_PROJECT_NAME} STREQUAL ${PROJECT_NAME})
#
# Set this only if this project is built as a standalone project.
#
#-------------------------------------------------------------------------------
# Set the mount path of your pmem device where the tests/benchmarks are stored
if(NOT PMEM_MNT_PATH)
  set(PMEM_MNT_PATH "/mnt/pmem0/poseidon")
endif()

option(USE_LLVM         "Use LLVM for query compilation"         OFF)
option(BUILD_PYTHON     "Build python interface for poseidon"    ON)
option(QOP_PROFILING    "Enable query profiling"                 ON)
#-------------------------------------------------------------------------------
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(LLVM_DIR "/opt/homebrew/opt/llvm@12/lib/cmake/llvm")
  set(ANTLR_EXECUTABLE "/opt/homebrew/Cellar/antlr/4.13.1/antlr-4.13.1-complete.jar")
else()
  set(ANTLR_EXECUTABLE "${CMAKE_SOURCE_DIR}/jars/antlr4-4.13.1-complete.jar")
endif()

# Don't try to contact github everytime.
set (FETCHCONTENT_FULLY_DISCONNECTED "OFF")

# Set the searching location for cmake 'include' locations
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Include the sanitizer module
include(sanitizers)

set(USE_SANITIZER "Memory")
set(CMAKE_MACOSX_RPATH 1)

# Include the code coverage module
# option(CODE_COVERAGE ON)
# include(code-coverage)
# add_code_coverage()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent REQUIRED)

#-------------------------------------------------------------------------------
# ANTLR4 Parser generator
#

find_package(ANTLR)

if(ANTLR_FOUND)
# add macros to generate ANTLR Cpp code from grammar
message("ANTLR found")
  antlr_target(grammar src/query/parser/poseidon.g4 
    COMPILE_FLAGS -Xexact-output-dir -visitor
    OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/src/query/antlr4_generated
  )
else()
  set(ANTLR_grammar_CXX_OUTPUTS 
    src/query/antlr4_generated/poseidonLexer.h
    src/query/antlr4_generated/poseidonLexer.cpp
    src/query/antlr4_generated/poseidonParser.h
    src/query/antlr4_generated/poseidonParser.cpp)
endif()

# message("grammar: ${ANTLR_grammar_CXX_OUTPUTS}")

#-------------------------------------------------------------------------------

if(QOP_PROFILING MATCHES ON)
  add_definitions( "-DQOP_PROFILING" )
endif()

if(USE_LLVM MATCHES ON)
  add_definitions( "-DUSE_LLVM" )
endif()

configure_file(
  ${PROJECT_SOURCE_DIR}/src/config.h.in
  ${PROJECT_BINARY_DIR}/generated/config.h
)

#---------------------------------------------------------------------------

include_directories(${PROJECT_BINARY_DIR}/generated)
include_directories(${PROJECT_SOURCE_DIR}/src/vec)
include_directories(${PROJECT_SOURCE_DIR}/src/storage)
include_directories(${PROJECT_SOURCE_DIR}/src/bp_file)
include_directories(${PROJECT_SOURCE_DIR}/src/query)
include_directories(${PROJECT_SOURCE_DIR}/src/query/parser)
include_directories(${PROJECT_SOURCE_DIR}/src/query/codegen)
include_directories(${PROJECT_SOURCE_DIR}/src/query/utils)
include_directories(${PROJECT_SOURCE_DIR}/src/query/plan_op)
include_directories(${PROJECT_SOURCE_DIR}/src/query/interp)
include_directories(${PROJECT_SOURCE_DIR}/src/query/expr)
include_directories(${PROJECT_SOURCE_DIR}/src/tx)
include_directories(${PROJECT_SOURCE_DIR}/src/dict)
include_directories(${PROJECT_SOURCE_DIR}/src/btree)
include_directories(${PROJECT_SOURCE_DIR}/src/pool)
include_directories(${PROJECT_SOURCE_DIR}/src/log)
include_directories(${PROJECT_SOURCE_DIR}/src/analytics)
include_directories(${PROJECT_SOURCE_DIR}/src/analytics/util)
include_directories(${PROJECT_SOURCE_DIR}/src/antlr4_cpp_runtime/src)
include_directories(${PROJECT_BINARY_DIR}/antlr4cpp_generated_src)

#---------------------------------------------------------------------------

set(THIRD_PARTY_DIR "${PROJECT_BINARY_DIR}/_deps")

# C++ compiler flags
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -Wall -Wno-deprecated -O3 -Wsign-compare")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -Wall -Wno-deprecated -O0 -g -Wsign-compare")
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs -Wno-#pragma-messages")
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -Wno-unused")
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Intel")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -wd488 -wd597")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-unused -Wno-uninitialized")

if(CMAKE_BUILD_TYPE MATCHES Release)
  # this disables asserts for release builds
  add_definitions("-DNDEBUG")
endif()

#---------------------------------------------------------------------------

# Catch2 =================================================================== #
FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.4.0
)

FetchContent_MakeAvailable(Catch2)

# CSVParser =================================================================== #
 FetchContent_Declare(
  CSV
  GIT_REPOSITORY https://github.com/AriaFallah/csv-parser.git
#  GIT_TAG        1.2.1
)

FetchContent_MakeAvailable(CSV)

# spdlog =================================================================== #
 FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
   GIT_TAG        v1.12.0
)

FetchContent_MakeAvailable(spdlog)

if(USE_LLVM MATCHES ON)
# LLVM ============================================================== #
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
include_directories(${LLVM_INCLUDE_DIRS})
llvm_map_components_to_libnames(LLVM_LIBRARIES
XRay WindowsManifest TableGen Symbolize DebugInfoPDB OrcJIT OrcError JITLink ObjectYAML MCA LTO Passes Coroutines ObjCARCOpts Extensions LineEditor LibDriver Interpreter FuzzMutate MCJIT ExecutionEngine RuntimeDyld DWARFLinker DlltoolDriver Option DebugInfoGSYM Coverage XCoreDisassembler XCoreCodeGen XCoreDesc XCoreInfo X86Disassembler X86AsmParser X86CodeGen X86Desc X86Info WebAssemblyDisassembler WebAssemblyCodeGen WebAssemblyDesc WebAssemblyAsmParser WebAssemblyInfo SystemZDisassembler SystemZCodeGen SystemZAsmParser SystemZDesc SystemZInfo SparcDisassembler SparcCodeGen SparcAsmParser SparcDesc SparcInfo RISCVDisassembler RISCVCodeGen RISCVAsmParser RISCVDesc RISCVUtils RISCVInfo PowerPCDisassembler PowerPCCodeGen PowerPCAsmParser PowerPCDesc PowerPCInfo NVPTXCodeGen NVPTXDesc NVPTXInfo MSP430Disassembler MSP430CodeGen MSP430AsmParser MSP430Desc MSP430Info MipsDisassembler MipsCodeGen MipsAsmParser MipsDesc MipsInfo LanaiDisassembler LanaiCodeGen LanaiAsmParser LanaiDesc LanaiInfo HexagonDisassembler HexagonCodeGen HexagonAsmParser HexagonDesc HexagonInfo BPFDisassembler BPFCodeGen BPFAsmParser BPFDesc BPFInfo AVRDisassembler AVRCodeGen AVRAsmParser AVRDesc AVRInfo ARMDisassembler ARMCodeGen ARMAsmParser ARMDesc ARMUtils ARMInfo AMDGPUDisassembler AMDGPUCodeGen MIRParser ipo Instrumentation Vectorize Linker IRReader AsmParser FrontendOpenMP AMDGPUAsmParser AMDGPUDesc AMDGPUUtils AMDGPUInfo AArch64Disassembler MCDisassembler AArch64CodeGen CFGuard GlobalISel SelectionDAG AsmPrinter DebugInfoDWARF CodeGen Target ScalarOpts InstCombine AggressiveInstCombine TransformUtils BitWriter Analysis ProfileData Object TextAPI BitReader Core Remarks BitstreamReader AArch64AsmParser MCParser AArch64Desc MC DebugInfoCodeView DebugInfoMSF BinaryFormat AArch64Utils AArch64Info Support Demangle)
        #X86Utils)
# LLVM_DYLIB_COMPONENTS in case of dynamic lib
endif() 

# linenoise =================================================================== #
 FetchContent_Declare(
  linenoise
  GIT_REPOSITORY https://github.com/yhirose/cpp-linenoise.git
 #  GIT_TAG        1.2.1
  )

FetchContent_MakeAvailable(linenoise)

# fmt =================================================================== #
set (BUILD_SHARED_LIBS ON)
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG        9.1.0
)

FetchContent_MakeAvailable(fmt)

# pybind11 =================================================================== #
 FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
#   GIT_TAG        2.6.1
  )
FetchContent_GetProperties(pybind11)

if(NOT pybind11_POPULATED)
  message(STATUS "Populating pybind11")
  FetchContent_Populate(pybind11)
  set(pybind11_INCLUDE_DIR ${pybind11_SOURCE_DIR}/include)
  add_library(pybind11 IMPORTED INTERFACE)
  set_property(TARGET pybind11 PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${pybind11_INCLUDE_DIR}")
endif()

#---------------------------------------------------------------------------

if(NOT ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
  set(DYLIB_LIBRARY "-ldl")
else()
  set(DYLIB_LIBRARY "")
endif()

#---------------------------------------------------------------------------



######################
# Boost C++ library
######################
#
SET(BOOST_MIN_VERSION "1.81.0")

find_package(Boost ${BOOST_MIN_VERSION} COMPONENTS
    program_options
    filesystem
    date_time
    thread
    REQUIRED)

if (NOT Boost_FOUND)
     message(FATAL_ERROR "Fatal error: Boost (version >= ${BOOST_MIN_VERSION}) required.\n"
 )
endif (NOT Boost_FOUND)

if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIR})
    add_definitions( "-DHAS_BOOST" )
endif()

#-----------------------------------------------------------------------------------------

# include_directories("${pegtl_SOURCE_DIR}/include")
include_directories("${csv_SOURCE_DIR}")
include_directories("${spdlog_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${linenoise_SOURCE_DIR}")
include_directories("${fmt_SOURCE_DIR}/include")

###########################
# Python Interface
###########################
#
find_package(Python3 COMPONENTS Interpreter)

if (BUILD_PYTHON)
message(STATUS "PYTHON")
find_package (Python3 COMPONENTS Interpreter Development)
  if (Python3_Development_FOUND)
    message(STATUS "Python Version ${PYTHON_VERSION_MAJOR} found - building the Python API.")
    message("PythonInclude ${Python3_INCLUDE_DIRS}")
    message("PythonLibs ${Python3_LIBRARY_DIRS}")

    include_directories(${Python3_INCLUDE_DIRS})
    include_directories(${pybind11_INCLUDE_DIR})
    link_directories(${PYTHON_LIBDIR})

    set(PYTHON_SOURCES
      src/py/py_poseidon.cpp
    )
  else()
    set (PYTHON_SOURCES "")
  endif(Python3_Development_FOUND)
else ()
        message(STATUS "Don't build python interface")
        set (PYTHON_SOURCES "")
endif()

#-----------------------------------------------------------------------------------------

if(USE_LLVM MATCHES ON)
  set(codegen_operators_src
    src/query/codegen/operators/aggregation.cpp
    src/query/codegen/operators/collect.cpp
    src/query/codegen/operators/create.cpp
    src/query/codegen/operators/expand.cpp
    src/query/codegen/operators/filter.cpp
    src/query/codegen/operators/foreach_rship.cpp
    src/query/codegen/operators/groupby.cpp
    src/query/codegen/operators/has_label.cpp
    src/query/codegen/operators/join.cpp
    src/query/codegen/operators/limit.cpp
    src/query/codegen/operators/project.cpp
    src/query/codegen/operators/scan.cpp
    src/query/codegen/operators/sort.cpp
    src/query/codegen/operators/join/cross_join.cpp
    src/query/codegen/operators/join/hash_join.cpp
    src/query/codegen/operators/join/nested_loop_join.cpp
    src/query/codegen/operators/join/outer_join.cpp
  )

  set(llvm_src
    ${codegen_operators_src}
    src/query/codegen/codegen.cpp
    src/query/codegen/filter_expression.cpp
    src/query/codegen/global_definitions.cpp
    src/query/codegen/jit/jit_cache.cpp
    src/query/codegen/jit/p_jit.cpp
    src/query/codegen/jit/p_context.cpp
    src/query/codegen/qcompiler.cpp
    src/query/codegen/proc/joiner.cpp
    src/query/codegen/proc/grouper.cpp
    src/query/codegen/interprete_filter_visitor.cpp
  )
else()
  set(llvm_src "")
endif()

set(src_files 
  src/dict/string_pool.cpp 
  src/dict/paged_string_pool.cpp 
  src/dict/code_table.cpp
  src/dict/dict.cpp
  src/storage/nodes.cpp
  src/storage/relationships.cpp
  src/storage/properties.cpp
  src/storage/graph_db.cpp
  src/storage/graph_db_import.cpp
  src/storage/graph_db_index.cpp
  src/storage/graph_db_gc.cpp
  src/storage/graph_db_recovery.cpp
  src/bp_file/paged_file.cpp
  src/bp_file/bufferpool.cpp
  src/bp_file/lru_list.cpp
  src/btree/index_map.cpp
  src/query/plan_op/qop.cpp
  src/query/plan_op/qop_projection.cpp
  src/query/plan_op/qop_builtins.cpp
  src/query/plan_op/qop_scans.cpp
  src/query/plan_op/qop_relationships.cpp
  src/query/plan_op/qop_aggregates.cpp
  src/query/plan_op/qop_analytics.cpp
  src/query/plan_op/qop_algorithm.cpp
  src/query/plan_op/qop_updates.cpp
  src/query/plan_op/qop_joins.cpp
  src/query/plan_op/query_batch.cpp
  src/query/plan_op/query_pipeline.cpp
  src/query/plan_op/query_builder.cpp
  src/query/expr/binary_expression.cpp
  src/query/expr/expression.cpp
  src/query/interp/expr_interpreter.cpp
  src/query/interp/qinterp.cpp
  src/query/interp/query_ctx.cpp
  src/query/utils/query_printer.cpp
  src/query/utils/thread_pool.cpp
  src/query/utils/profiling.cpp
  src/query/utils/qresult_iterator.cpp
  src/tx/transaction.cpp
  src/pool/graph_pool.cpp
  src/log/walog.cpp
  src/query/parser/query_planner.cpp
  src/query/parser/query_proc.cpp
  src/analytics/bfs.cpp
  src/analytics/shortest_path.cpp
  ${ANTLR_grammar_CXX_OUTPUTS}
)

# Using OBJECT to avoid to build object files multiple times due to inclusion
# in different libraries.
# macOS doesn't like SHARED, Linux needs it
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
add_library(poseidon_obj OBJECT ${src_files})
else()
add_library(poseidon_obj OBJECT SHARED ${src_files})
endif()

# Poseidon core library for building C++ applications
add_library(poseidon_core SHARED
  $<TARGET_OBJECTS:poseidon_obj>
)

if(USE_LLVM MATCHES ON)
  set(LLVM_LIB LLVM)
else()
  set(LLVM_LIB "")
endif()

target_link_libraries(poseidon_core
  ${LLVM_LIB}
  antlr4_cpp_runtime
#  antlr4_shared
  Boost::filesystem
  Boost::date_time
  Boost::thread
  spdlog::spdlog_header_only
  fmt::fmt
)


target_include_directories( ${PROJECT_NAME}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

if (BUILD_PYTHON)
message("Build python ${PYTHON_SOURCES}")
# Python Poseidon library for loading a module into the python3 interpreter
add_library(poseidon SHARED
  $<TARGET_OBJECTS:poseidon_obj>
  ${PYTHON_SOURCES}
)

target_link_libraries(poseidon
  ${LLVM_LIB}
  antlr4_cpp_runtime
  # antlr4_shared
  Boost::filesystem
  Boost::date_time
  Boost::thread
  ${Python3_LIBRARIES}
  spdlog::spdlog_header_only
  fmt::fmt
)

set_target_properties(poseidon PROPERTIES PREFIX "" SUFFIX ".so")
endif()

#-----------------------------------------------------------------------------------------

add_subdirectory(src/antlr4_cpp_runtime)

#-----------------------------------------------------------------------------------------

add_executable(pcli
  src/cmds/pcli.cpp
 )

 target_link_libraries(pcli
  poseidon_core
  Boost::program_options
)

add_executable(wal_dump
  src/cmds/wal_dump.cpp
)

target_link_libraries(wal_dump
  poseidon_core
  Boost::program_options
)

#-----------------------------------------------------------------------------------------
#
# Unit tests using Catch
#
enable_testing()
add_subdirectory(test)

#-----------------------------------------------------------------------------------------

# Create logs directory
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/logs")
