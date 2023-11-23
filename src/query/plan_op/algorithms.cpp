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

#include <limits>
#include "qop_builtins.hpp"

qr_tuple num_links(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    int in_links = 0, out_links = 0;
    auto n = qv_get_node(v.back()); 

    ctx.foreach_from_relationship_of_node(*n, [&](auto &r) { out_links++; });
    ctx.foreach_to_relationship_of_node(*n, [&](auto &r) { in_links++; });
    
    qr_tuple res(2);
    res[0] = qv_(in_links); res[1] = qv_(out_links);
    return res;
}
#if 0
// GroupBy([$3.authorid:uint64], [count($0.tweetid:uint64)], Expand(OUT, 'Author', ForeachRelationship(FROM, 'AUTHOR', Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website'))))))
// Expand(OUT, 'Author', ForeachRelationship(FROM, 'AUTHOR', Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website')))))
// Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website')))
qr_tuple oldest_tweet(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    auto n = qv_get_node(v.back()); 
    auto tweet_label = ctx.gdb_->get_code("Tweet");
    auto creation_label = ctx.gdb_->get_code("created_at");
    auto oldest_date = std::string("ZZZZZZZZZZZZ"); // std::numeric_limits<int>::max();
    node *oldest_tweet = nullptr;

    ctx.foreach_to_relationship_of_node(*n, [&](auto &rship) {
        auto& tweet = ctx.gdb_->node_by_id(rship.src_node);
        if (tweet.node_label == tweet_label) {
            auto pval = ctx.gdb_->get_property_value(tweet, creation_label);    
            auto date_str = std::string(ctx.gdb_->get_string(pval.template get<dcode_t>()));
            std::cout << "Tweet - created at: " << date_str << std::endl;
            auto dval = boost::posix_time::from_iso_extended_string(date_str);
            std::cout << "Tweet - created at: " << dval << std::endl;
            if (date_str < oldest_date) {
                oldest_date = date_str;
                oldest_tweet = &tweet;
            }
        }
    });
    qr_tuple res(1);
    res[0] = oldest_tweet;
    return res;
}
#endif
std::map<std::string, algorithm_op::tuple_algorithm_func> algorithm_op::tuple_algorithms_  = {
#if 0
  { std::string("OldestTweet"), oldest_tweet },
#endif
  { std::string("NumLinks"), num_links }
};

std::map<std::string, algorithm_op::set_algorithm_func> algorithm_op::set_algorithms_;
