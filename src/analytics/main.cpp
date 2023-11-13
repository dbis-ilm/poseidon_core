#include <chrono>
#include <iostream>

#include "graph_db.hpp"
#include "graph_pool.hpp"
#include "format_converter.hpp"

#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

#ifdef USE_PMDK

#define POOL_SIZE ((unsigned long long)(1024 * 1024 * 4000ull))

struct root {
  graph_db_ptr graph;
};

#endif

std::chrono::steady_clock::time_point timer_beg;
std::chrono::steady_clock::time_point timer_end;

inline void start_timer() {
  timer_beg = std::chrono::steady_clock::now();
}

inline void stop_timer() {
  timer_end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(timer_end -
                                                                        timer_beg).count();
  spdlog::info("Elapsed Time: {} milliseconds", duration/(double)1000);
}

void create_data(graph_db_ptr graph) {
  graph->begin_transaction();

    //
    //  0 --> 1 --> 2 --> 3 --> 4
    //  |           ^
    //  |           |
    //  +---------> 6
    //              ^
    //              |
    //             5
    //

  for (int i = 0; i < 7; i++) {
    graph->add_node("Person",
                                  {{"name", boost::any(std::string("John Doe"))},
                                  {"age", boost::any(42)},
                                  {"id", boost::any(i)},
                                  {"dummy1", boost::any(std::string("Dummy"))},
                                  {"dummy2", boost::any(1.2345)}},
                                  true);
  }
  graph->add_relationship(0, 1, ":knows", {});
  graph->add_relationship(1, 2, ":knows", {});
  graph->add_relationship(2, 3, ":knows", {});
  graph->add_relationship(3, 4, ":knows", {});
  graph->add_relationship(5, 6, ":knows", {});
  graph->add_relationship(6, 2, ":knows", {});
  graph->add_relationship(0, 6, ":likes", {});
  graph->commit_transaction();
}

int main(int argc, char* argv[]) {

  std::string db_name = "csr_sf10";
  std::string snb_home = "/home/data/SNB_SF_10";
  std::string path = poseidon::gPmemPath + db_name;

#ifdef USE_PMDK
    pmem::obj::pool<root> pop;
    if (access(path.c_str(), F_OK) != 0) 
      pop = pmem::obj::pool<root>::create(path, db_name, POOL_SIZE);
    else
      pop = pmem::obj::pool<root>::open(path, db_name);
    
    auto root_ptr = pop.root();
    if (!root_ptr->graph) {
      pmem::obj::transaction::run(pop, [&] {
        root_ptr->graph = p_make_ptr<graph_db>();
      });
      load_snb_data(root_ptr->graph, snb_home);
    }
    else
      root_ptr->graph->runtime_initialize();

  auto &graph = root_ptr->graph;
#else
  auto pool = graph_pool::create(path);
  auto graph = pool->create_graph(db_name);

  load_snb_data(graph, snb_home);
#endif

  std::vector<uint64_t> row_offsets1 = {};
  std::vector<uint64_t> col_indices1 = {};
  std::vector<float> edge_values1 = {};
  auto weight_func = [](auto& r) { return 1.3; };

  std::vector<uint64_t> row_offsets2 = {};
  std::vector<uint64_t> col_indices2 = {};
  std::vector<float> edge_values2 = {};

  start_timer();
  graph->run_transaction([&]() {
    poseidon_to_csr(graph, row_offsets1, col_indices1,
                                    edge_values1, weight_func);
    return true;
  });
  stop_timer();

  auto &delta = graph->get_csr_delta();
  start_timer();
  for (auto i = 42; i < 1000000; i++) {
    delta->add_update_delta(i, {2, 3, 4}, {1.3, 1.3, 1.3});
    delta->add_append_delta(i+30000000, {5, 6, 7}, {1.3, 1.3, 1.3});
  }
  stop_timer();

  start_timer();
  update_csr_with_delta(graph, row_offsets1, col_indices1,
      edge_values1, row_offsets2, col_indices2, edge_values2);
  stop_timer();
}