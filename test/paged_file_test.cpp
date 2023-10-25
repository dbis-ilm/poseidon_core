#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include "config.h"
#include "exceptions.hpp"
#include "paged_file.hpp"

TEST_CASE("Creating a paged file", "[paged_file]") {
    remove("test.dat");
    paged_file pf;

    pf.open("test.dat");
    REQUIRE(pf.is_open());
    auto pid = pf.allocate_page();
    REQUIRE(pid == 1);
    REQUIRE(pf.num_pages() == 1);

    page p1;
    memset(p1.payload, 0xff, PAGE_SIZE);
    pf.write_page(pid, p1);

    page p2;
    REQUIRE(pf.read_page(pid, p2) == true);
    REQUIRE(p2.payload[0] == 0xff);
    REQUIRE(p2.payload[PAGE_SIZE-1] == 0xff);
    pf.close();
    remove("test.dat");
}

TEST_CASE("Creating a larger paged file", "[paged_file]") {
    remove("test2.dat");
    {
        paged_file pf;
        
        pf.open("test2.dat");
        REQUIRE(pf.is_open());
        for (auto i = 0u; i < 100; i++)
            pf.allocate_page();

        page p1;
        memset(p1.payload, 0xcc, PAGE_SIZE);
        pf.write_page(42, p1);
        pf.close();
    }

    {
        paged_file pf;
        
        pf.open("test2.dat");
        REQUIRE(pf.is_open());
        REQUIRE(pf.num_pages() == 100);
        page p2;
        REQUIRE(pf.read_page(42, p2) == true);
        auto n = 0u;
        for (auto i = 0u; i < PAGE_SIZE; i++)
            if (p2.payload[i] == 0xcc)
                n++;
        REQUIRE(n == PAGE_SIZE);
        pf.close();
    }
    remove("test2.dat");
}

TEST_CASE("Creating another larger paged file", "[paged_file]") {
    remove("test3.dat");
    {
        paged_file pf;
        
        pf.open("test3.dat");
        REQUIRE(pf.is_open());
        for (auto i = 0u; i < 100; i++)
            pf.allocate_page();

        page p1;
        memset(p1.payload, 0xcc, PAGE_SIZE);

        for (auto i = 0u; i < 100; i++) {
            p1.payload[0] = i;
            pf.write_page(i+1, p1);
        }
        pf.close();
    }

    {
        paged_file pf;
        
        pf.open("test3.dat");
        REQUIRE(pf.is_open());
        REQUIRE(pf.num_pages() == 100);
        page p2;

        for (auto i = 0u; i < 100; i++) {
            REQUIRE(pf.read_page(i+1, p2) == true);
            auto n = 1u;
            REQUIRE(p2.payload[0] == i);
            for (auto i = 1u; i < PAGE_SIZE; i++) {
                if (p2.payload[i] == 0xcc)
                    n++;
            }
            REQUIRE(n == PAGE_SIZE);
        }
        pf.close();
    }
    remove("test3.dat");
}

TEST_CASE("Creating a paged file and delete some pages", "[paged_file]") {
    remove("test4.dat");
        paged_file pf;
        
        pf.open("test4.dat");
        REQUIRE(pf.is_open());
        for (auto i = 0u; i < 100; i++)
            pf.allocate_page();

        REQUIRE(pf.free_page(42));
        REQUIRE(pf.free_page(88));

        REQUIRE(!pf.free_page(42));

        REQUIRE(!pf.is_valid(0));
        REQUIRE(pf.is_valid(1));
        REQUIRE(pf.is_valid(40));
        REQUIRE(!pf.is_valid(42));
        REQUIRE(!pf.is_valid(200));

        page pp;
        CHECK_THROWS_AS(pf.read_page(42, pp), index_out_of_range);

        REQUIRE(pf.allocate_page() == 42);
        pf.close();
        remove("test4.dat");

}

TEST_CASE("Checking last valid page", "[paged_file]") {
    remove("test5.dat");
    paged_file pf;
    
    pf.open("test5.dat");
    REQUIRE(pf.is_open());

    REQUIRE(pf.last_valid_page() == 1);
    REQUIRE(pf.last_valid_page() == 1);
    
    for (auto i = 0u; i < 5; i++)
        pf.allocate_page();

    REQUIRE(pf.last_valid_page() == 6);
    REQUIRE(pf.free_page(3));
    REQUIRE(pf.last_valid_page() == 6);
    remove("test5.dat");
}

TEST_CASE("Reusing free page slots", "[paged_file]") {
    remove("test6.dat");
    paged_file pf;
    
    pf.open("test6.dat");
    REQUIRE(pf.is_open());
    
    for (auto i = 0u; i < 5; i++)
        pf.allocate_page();
    
    for (auto i = 0u; i < 5; i++)
        pf.free_page(i+1);
    
    for (auto i = 0u; i < 5; i++)
        pf.allocate_page();

    REQUIRE(pf.num_pages() == 5);
    remove("test6.dat");
}