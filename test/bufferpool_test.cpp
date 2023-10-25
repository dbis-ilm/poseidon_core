#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "paged_file.hpp"
#include "bufferpool.hpp"

#include <filesystem>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

void create_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj))
        std::filesystem::create_directory(path_obj);
}

void delete_dir(const std::string& path) {
    std::filesystem::path path_obj(path);
    std::filesystem::remove_all(path_obj);
}

TEST_CASE("Creating a bufferpool", "[bufferpool]") {
    create_dir("bp_test1");
    bufferpool bpool;

    auto test_file = std::make_shared<paged_file>();
    test_file->open("bp_test1/bp_records.db", 0);

    bpool.register_file(0, test_file);
    for (auto i = 0u; i < 100; i++)
        bpool.allocate_page(0ul);
    
    for (auto i = 1u; i <= 100; i++) {
        auto p = bpool.fetch_page(i);
        p->payload[0] = i+1;
        bpool.mark_dirty(i);
    }

    boost::random::mt19937 rng; 
    boost::random::uniform_int_distribution<> page_ids(1, 100);

    for (auto i = 0u; i < 10000; i++) {
        auto pid = page_ids(rng);
        auto p = bpool.fetch_page(pid);
        REQUIRE(p->payload[0] == pid+1);
    }

    bpool.flush_all();
    delete_dir("bp_test1");
}

TEST_CASE("Creating a bufferpool and fetchting data from disk", "[bufferpool]") {
    create_dir("bp_test2");

    {
        bufferpool bpool;
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bp_test2/bp_records.db", 0);
        bpool.register_file(0, test_file);

        for (auto i = 0u; i < 100; i++)
            bpool.allocate_page(0ul);
    
        for (auto i = 1u; i <= 100; i++) {
            auto p = bpool.fetch_page(i);
            p->payload[0] = i+1;
            bpool.mark_dirty(i);
        }
        bpool.flush_all();
    }

    {
        bufferpool bpool;
        auto test_file = std::make_shared<paged_file>();
        test_file->open("bp_test2/bp_records.db", 0);

        bpool.register_file(1, test_file);

        boost::random::mt19937 rng; 
        boost::random::uniform_int_distribution<> page_ids(1, 100);

        for (auto i = 0u; i < 10000; i++) {
            auto pid = page_ids(rng);
            auto p = bpool.fetch_page((1ul << 60) | pid);
            REQUIRE(p->payload[0] == pid+1);
        }
    }
    delete_dir("bp_test2");
}