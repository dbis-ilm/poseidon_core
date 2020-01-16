/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */
#define CATCH_CONFIG_MAIN // This tells Catch to provide a main() - only do
                          // this in one cpp file

#include "catch.hpp"
#include "properties.hpp"


 
TEST_CASE("Testing helper functions", "[properties]") {
  REQUIRE(is_quoted_string("\"Hallo\""));
  REQUIRE(is_quoted_string("'Hallo'"));
  REQUIRE(!is_quoted_string("Hallo"));
  REQUIRE(is_int("1234"));
  REQUIRE(is_int("-1234"));
  REQUIRE(is_int("0"));
  REQUIRE(!is_int("-0"));
  REQUIRE(is_int("1"));
  REQUIRE(!is_int("01234"));
  REQUIRE(!is_int("Hallo"));
  REQUIRE(!is_int("Hallo123"));
  REQUIRE(!is_int("01"));
  REQUIRE(is_float("1.234"));
  REQUIRE(is_float("123.234"));
  REQUIRE(is_float("0.234"));
  REQUIRE(!is_float("123"));
  REQUIRE(!is_float("H123"));
  REQUIRE(!is_float("H.123"));
}

TEST_CASE("Testing p_item", "[properties]") {
    p_item pi1(22, 66.67);
    REQUIRE(pi1.get<double>() == 66.67);
    REQUIRE(pi1.key() == 22);
    REQUIRE(pi1.typecode() == p_item::p_double);

    REQUIRE(!pi1.equal(60));
    REQUIRE(!pi1.equal(70.0));
    REQUIRE(pi1.equal(66.67));

    p_item pi2(22, 44);
    REQUIRE(pi2.get<int>() == 44);
    REQUIRE(pi2.typecode() == p_item::p_int);

    std::array<p_item, 10> items;
    items[0] = pi1;
    REQUIRE(items[0].get<double>() == 66.67);
    REQUIRE(items[0].key() == 22);
}

TEST_CASE("Testing property_set", "[properties]") {
  // TODO
}
