#include "config.h"
#include "defs.hpp"
#include "graph_db.hpp"
#include "graph_pool.hpp"

#include "spdlog/spdlog.h"

int main(int argc, char **argv) {
    if (argc != 3)
        exit(-1);

    std::string path = argv[1];
    std::string graph_name = argv[2];

    auto pool = graph_pool::create(path, 1024*1024*100);
    auto graph = pool->create_graph(graph_name);
    spdlog::info("create pool '{}' with graph '{}'", path, graph_name);
    {
        graph->begin_transaction();
        auto n = graph->add_node("Person", {{"number", std::any(2)}});
        spdlog::info("node #{} inserted", n);
        graph->commit_transaction();
    }
    {
        graph->begin_transaction();
        graph->add_node("Person", {{"number", std::any(42)}});
        graph->add_node("Person", {{"number", std::any(43)}});
        graph->add_node("Person", {{"number", std::any(44)}});
    } 
    exit(0);
}
