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
#include "analytics.hpp"
#include "analytics/shortest_path.hpp"


/*
 Algorithm([ShortestPath, TUPLE, "knows", 0, 0], 
    CrossJoin(
        Filter($0.id == 'Ilmenau', 
            NodeScan('City')),
        Filter($0.id == 'Berlin', 
            NodeScan('City'))
    )
 )
*/
algorithm_op::tuple_list shortest_path_algorithm(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    if (v.size() < 2)
        return algorithm_op::tuple_list();

    auto start = qv_get_node(v[v.size() - 2])->id();
    auto stop = qv_get_node(v[v.size() - 1])->id();

    auto rship_label_str = std::any_cast<std::string>(args[0]);
    auto rship_label = ctx.get_code(rship_label_str);
    bool bidirectional = args.size() > 1 ? std::any_cast<int>(args[1]) == 1 : false;
    bool all_spaths = args.size() > 2 ? std::any_cast<int>(args[2]) == 1 : false;
    
    auto pv = [&](node &n, const path &p) {}; 
    auto rpred = [&](relationship &r) { return r.rship_label == rship_label; };

    if (all_spaths) {
        std::list<path_item> spaths;
        all_unweighted_shortest_paths(ctx, start, stop, bidirectional, rpred, pv, spaths);
        algorithm_op::tuple_list tlist;
        for (auto &path : spaths) {
            array_t node_ids(path.get_path());
            qr_tuple res(1);
            res[0] = qv_(node_ids);
            tlist.push_back(res);
        }
        return tlist;
    }
    else {
        path_item spath;
        bool found = unweighted_shortest_path(ctx, start, stop, bidirectional,
                                          rpred, pv, spath);
        if (found) {
            qr_tuple res(1);
            array_t node_ids(spath.get_path());
            res[0] = qv_(node_ids);
            return algorithm_op::tuple_list({res});
        }
        else
            return algorithm_op::tuple_list();
    }
}

algorithm_op::tuple_list weighted_shortest_path_algorithm(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    if (v.size() < 2)
        return algorithm_op::tuple_list();

    auto start = qv_get_node(v[v.size() - 2])->id();
    auto stop = qv_get_node(v[v.size() - 1])->id();

    auto rship_label_str = std::any_cast<std::string>(args[0]);
    auto rship_label = ctx.get_code(rship_label_str);

    auto weight_label_str = std::any_cast<std::string>(args[1]);
    // auto weight_label = ctx.get_code(weight_label_str);
    auto rweight = [&](relationship &r) { 
        auto rd = ctx.gdb_->get_rship_description(r.id());
        auto wval = get_property<double>(rd.properties, weight_label_str); 
        return wval.has_value() ? wval.value() : 0.0;
    };

    bool bidirectional = args.size() > 2 ? std::any_cast<int>(args[2]) == 1 : false;
    bool all_spaths = args.size() > 3 ? std::any_cast<int>(args[3]) == 1 : false;
    
    auto pv = [&](node &n, const path &p) {}; 
    auto rpred = [&](relationship &r) { return r.rship_label == rship_label; };

    if (all_spaths) {
        std::list<path_item> spaths;
        all_weighted_shortest_paths(ctx, start, stop, bidirectional, rpred, rweight, pv, spaths);
        algorithm_op::tuple_list tlist;
        for (auto &path : spaths) {
            array_t node_ids(path.get_path());
            qr_tuple res(1);
            res[0] = qv_(node_ids);
            tlist.push_back(res);
        }
        return tlist;
    }
    else {
        path_item spath;
        bool found = weighted_shortest_path(ctx, start, stop, bidirectional,
                                          rpred, rweight, pv, spath);
        if (found) {
            qr_tuple res(1);
            array_t node_ids(spath.get_path());
            res[0] = qv_(node_ids);
            return algorithm_op::tuple_list({res});
        }
        else
            return algorithm_op::tuple_list();
    }
}

algorithm_op::tuple_list num_links(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    int in_links = 0, out_links = 0;
    auto n = qv_get_node(v.back()); 

    ctx.foreach_from_relationship_of_node(*n, [&](auto &r) { out_links++; });
    ctx.foreach_to_relationship_of_node(*n, [&](auto &r) { in_links++; });
    
    qr_tuple res(2);
    res[0] = qv_(in_links); res[1] = qv_(out_links);
    return algorithm_op::tuple_list({res});
}

// GroupBy([$3.authorid:uint64], [count($0.tweetid:uint64)], Expand(OUT, 'Author', ForeachRelationship(FROM, 'AUTHOR', Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website'))))))
// Expand(OUT, 'Author', ForeachRelationship(FROM, 'AUTHOR', Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website')))))
// Algorithm([OldestTweet, TUPLE], Limit(10, NodeScan('Website')))
algorithm_op::tuple_list oldest_tweet(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
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
            //std::cout << "Tweet - created at: " << date_str << std::endl;
            //auto dval = boost::posix_time::from_iso_extended_string(date_str);
            //std::cout << "Tweet - created at: " << dval << std::endl;
            if (date_str < oldest_date) {
                oldest_date = date_str;
                oldest_tweet = &tweet;
            }
        }
    });
    if (oldest_tweet != nullptr) {
        qr_tuple res(1);
        res[0] = oldest_tweet;
        return algorithm_op::tuple_list({res});
    }
    else {
        return algorithm_op::tuple_list();
    }
}

std::map<std::string, algorithm_op::tuple_algorithm_func> algorithm_op::tuple_algorithms_  = {
  { std::string("OldestTweet"), oldest_tweet },
  { std::string("NumLinks"), num_links },
  { std::string("ShortestPath"), shortest_path_algorithm },
  { std::string("WeightedShortestPath"), weighted_shortest_path_algorithm }
  // { std::string("KWeightedShortestPath"), k_weighted_shortest_path },
};

std::map<std::string, algorithm_op::set_algorithm_func> algorithm_op::set_algorithms_;
