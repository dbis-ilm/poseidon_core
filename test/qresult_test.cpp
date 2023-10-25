/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */
#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include <catch2/catch_test_macros.hpp>
#include "qresult_iterator.hpp"
#include "query_ctx.hpp"

TEST_CASE("Testing sort of result_set - [int ASC, string ASC]", "[qresult]") {
    query_ctx ctx;
    result_set rs, expected;
    rs.append({ query_result(5), query_result("CCC") });
    rs.append({ query_result(3), query_result("BBB") });
    rs.append({ query_result(4), query_result("EEE") });
    rs.append({ query_result(3), query_result("AAA") });
    rs.append({ query_result(1), query_result("DDD") });

    rs.sort(ctx, { result_set::sort_spec(0, 2), result_set::sort_spec(1, 4) }); 

    expected.append({ query_result(1), query_result("DDD") });
    expected.append({ query_result(3), query_result("AAA") });
    expected.append({ query_result(3), query_result("BBB") });
    expected.append({ query_result(4), query_result("EEE") });
    expected.append({ query_result(5), query_result("CCC") });

    REQUIRE(rs == expected);
}

TEST_CASE("Testing sort of result_set - [string ASC, int ASC]", "[qresult]") {
    query_ctx ctx;
    result_set rs, expected;
    rs.append({ query_result("BBB"), query_result(5) });
    rs.append({ query_result("EEE"), query_result(3) });
    rs.append({ query_result("BBB"), query_result(4) });
    rs.append({ query_result("AAA"), query_result(3) });
    rs.append({ query_result("CCC"), query_result(1) });

    rs.sort(ctx, { result_set::sort_spec(0, 4), result_set::sort_spec(1, 2) }); 
    
    expected.append({ query_result("AAA"), query_result(3) });
    expected.append({ query_result("BBB"), query_result(4) });
    expected.append({ query_result("BBB"), query_result(5) });
    expected.append({ query_result("CCC"), query_result(1) });
    expected.append({ query_result("EEE"), query_result(3) });

    REQUIRE(rs == expected);
}

TEST_CASE("Testing sort of result_set - [int DESC, string ASC]", "[qresult]") {
    query_ctx ctx;
    result_set rs, expected;
    rs.append({ query_result(5), query_result("CCC") });
    rs.append({ query_result(3), query_result("BBB") });
    rs.append({ query_result(4), query_result("EEE") });
    rs.append({ query_result(3), query_result("AAA") });
    rs.append({ query_result(1), query_result("DDD") });

    rs.sort(ctx, { result_set::sort_spec(0, 2, result_set::sort_spec::Desc), result_set::sort_spec(1, 4) }); 

    expected.append({ query_result(5), query_result("CCC") });
    expected.append({ query_result(4), query_result("EEE") });
    expected.append({ query_result(3), query_result("AAA") });
    expected.append({ query_result(3), query_result("BBB") });
    expected.append({ query_result(1), query_result("DDD") });

    REQUIRE(rs == expected);
}