/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef qop_builtins_hpp_
#define qop_builtins_hpp_

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "qop.hpp"

/**
 *  namespace for builtin functions used in project, filter etc.
 */
namespace builtin {

/**
 * Returns true if the node/relationship has the property specified by
 * the key. Otherwise, return false.
 */	
bool has_property(query_result &pv, const std::string &key);

/**
 * Returns true if the node/relationship has the given label specified. 
 * Otherwise, return false.
 */	
bool has_label(query_result &pv, const std::string &l);

/**
 * Returnd the label of the node/relationship.
 */
query_result get_label(const query_result &pv);

/**
 * Return the integer value of the property of a node/relationship stored in
 * projection_result res and identified by the given key.
 */
query_result int_property(const query_result &res, 
                 const std::string &key);

/**
 * Return the double value of the property of a node/relationship stored in
 * projection_result res and identified by the given key.
 */
query_result double_property(const query_result &res, 
                       const std::string &key);

/**
 * Return the string value of the property of a node/relationship stored in
 * projection_result res and identified by the given key.
 */
query_result string_property(const query_result &res, 
                            const std::string &key);

/**
 * Return the unsigned 64-bit integer value of the property of a node/relationship 
 * stored in projection_result res and identified by the given key.
 */
query_result uint64_property(const query_result &res, 
                 const std::string &key);

/**
 * Return the ptime value of the property of a node/relationship 
 * stored in projection_result res and identified by the given key.
 */
query_result ptime_property(const query_result &res, 
                 const std::string &key);

/**
 * Return the string representation of the date property of a node/relationship 
 * stored in projection_result res and identified by the given key.
 */
query_result pr_date(const query_result &pv, 
                 const std::string &key);

/**
 * Return the year of the date property of a node/relationship 
 * stored in projection_result res and identified by the given key.
 */
query_result pr_year(const query_result &pv, 
                 const std::string &key);

/**
 * Return the month of the date property of a node/relationship 
 * stored in projection_result res and identified by the given key.
 */
query_result pr_month(const query_result &pv, 
                 const std::string &key);

/**
 * Return the string representation of a node/relationship stored in
 * projection_result res.
 */
std::string string_rep(query_result &res);

/**
 * Convert the given string value into an integer.
 */
int to_int(const std::string &s);

/**
 * Convert the given date value into its string representation.
 */
std::string int_to_datestring(int v);
std::string int_to_datestring(const query_result& v);

/**
 * Convert the given date string (2019-02-12) into an int value (posix time).
 */
int datestring_to_int(const std::string &d);

/**
 * Convert the given datetime value into its string representation.
 */
std::string int_to_dtimestring(int v);

std::string int_to_dtimestring(const query_result& v);

/**
 * Convert the given date+time string (2019-01-22T19:59:59.221+0000) into an int
 * value (posix time).
 */
int dtimestring_to_int(const std::string &d);

/**
 * Return true if the value represented by pv is a null value.
 */
bool is_null(const query_result& pv);

/*
CASE:
 return !string_property(res, 0, "content").empty() ?
    string_property(res, 0, "content") :
    string_property(res, 0, "imageFile"); },

*/

} // namespace builtin

#endif