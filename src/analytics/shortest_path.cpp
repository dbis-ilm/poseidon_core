/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#include <queue>

#include "shortest_path.hpp"

#include <boost/dynamic_bitset.hpp>

constexpr double UNKNOWN_WEIGHT = std::numeric_limits<double>::max();

bool unweighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop,
        bool bidirectional, rship_predicate rpred, path_visitor visit, path_item &spath) {
    bool found = false;
    std::queue<path> frontier;
    boost::dynamic_bitset<> visited(ctx.gdb_->get_nodes()->as_vec().capacity());
    std::vector<std::size_t> distance(ctx.gdb_->get_nodes()->as_vec().capacity(), UNKNOWN);

    distance[start] = 0;
    visited.set(start);
    frontier.push({start});

    while (!frontier.empty()) {
        auto u = frontier.front();
        auto uid = u.back();    
        frontier.pop();

        auto& n = ctx.gdb_->node_by_id(uid);
        visit(n, u);
       
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            auto vid = r.to_node_id();
            if (rpred(r) && !visited[vid]) {
                visited.set(vid);
                distance[vid] = distance[uid] + 1;
                path u2(u);
                u2.push_back(vid);
                frontier.push(u2);

                if (vid == stop) {
                    found = true;
                    spath.set_path(u2);
                    spath.set_hops(distance[vid]);
                }
            }
        });

        if (bidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                auto vid = r.from_node_id();
                if (rpred(r) && !visited[vid]) {
                    visited.set(vid);
                    distance[vid] = distance[uid] + 1;
                    path u2(u);
                    u2.push_back(vid);
                    frontier.push(u2);

                    if (vid == stop) {
                        found = true;
                        spath.set_path(u2);
                        spath.set_hops(distance[vid]);
                    }
                }
            });
        }
        if (found)
            return true;
    }
    return false;
}

bool all_unweighted_shortest_paths(query_ctx& ctx, node::id_t start, node::id_t stop,
        bool bidirectional, rship_predicate rpred, path_visitor visit, std::list<path_item> &spaths) {
    bool found = false;
    std::queue<path> frontier;
    boost::dynamic_bitset<> visited(ctx.gdb_->get_nodes()->as_vec().capacity());
    std::vector<std::size_t> distance(ctx.gdb_->get_nodes()->as_vec().capacity(), UNKNOWN);

    distance[start] = 0;
    visited.set(start);
    frontier.push({start});

    while (!frontier.empty()) {
        auto u = frontier.front();
        auto uid = u.back();    
        frontier.pop();

        auto& n = ctx.gdb_->node_by_id(uid);
        visit(n, u);
       
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            auto vid = r.to_node_id();
            if (rpred(r) && (!visited[vid] || (vid == stop && distance[uid] < distance[stop]))) {
                visited.set(vid);
                distance[vid] = distance[uid] + 1;
                path u2(u);
                u2.push_back(vid);
                frontier.push(u2);

                if (vid == stop) {
                    found = true;
                    path_item spath;
                    spath.set_path(u2);
                    spath.set_hops(distance[vid]);
                    spaths.push_back(spath);
                }
            }
        });

        if (bidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                auto vid = r.from_node_id();
                if (rpred(r) && (!visited[vid] || (vid == stop && distance[uid] < distance[stop]))) {
                    visited.set(vid);
                    distance[vid] = distance[uid] + 1;
                    path u2(u);
                    u2.push_back(vid);
                    frontier.push(u2);

                    if (vid == stop) {
                        found = true;
                        path_item spath;
                        spath.set_path(u2);
                        spath.set_hops(distance[vid]);
                        spaths.push_back(spath);
                    }
                }
            });
        }
    }
    return found ? true : false;
}

bool weighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop, bool bidirectional,
                rship_predicate rpred, rship_weight weight_func, path_visitor visit, path_item &spath) {
    bool found = false;
    uint64_t num_nodes = ctx.gdb_->get_nodes()->as_vec().capacity();
    boost::dynamic_bitset<> visited(num_nodes);
    std::vector<uint64_t> parent(num_nodes, UNKNOWN - 1);
    std::vector<double> weight(num_nodes, UNKNOWN_WEIGHT);

    weight[start] = 0.0;
    parent[start] = UNKNOWN;

    // TODO Optimize search for next node with minimum weight
    for (uint64_t i = 0; i < num_nodes; i++) {
        uint64_t min_nid = UNKNOWN;
        double min_weight = UNKNOWN_WEIGHT;
        for (uint64_t nid = 0; nid < num_nodes; nid++) {
            if (!visited[nid] && weight[nid] < min_weight) {
                min_nid = nid;
                min_weight = weight[nid];
            }
        }

        if (min_nid == stop) {
            found = true;
            spath.trace_path(parent, stop);
            spath.set_weight(weight[stop]);
            return true;
        }
        else if (min_weight == UNKNOWN_WEIGHT)
            return false;

        visited.set(min_nid);

        auto& n = ctx.gdb_->node_by_id(min_nid);
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            auto vid = r.to_node_id();
            if (rpred(r)) {
                auto v_weight = weight_func(r);
                if (!visited[vid] && (weight[min_nid] + v_weight) < weight[vid]) {
                    weight[vid] = weight[min_nid] + v_weight;
                    parent[vid] = min_nid;
                }
            }
        });

        if (bidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                auto vid = r.from_node_id();
                if (rpred(r)) {
                    auto v_weight = weight_func(r);
                    if (!visited[vid] && (weight[min_nid] + v_weight) < weight[vid]) {
                        weight[vid] = weight[min_nid] + v_weight;
                        parent[vid] = min_nid;
                    }
                }
            });
        }
    }
    return false;
}

bool all_weighted_shortest_paths(query_ctx& ctx, node::id_t start, node::id_t stop, bool bidirectional,
                rship_predicate rpred, rship_weight weight_func, path_visitor visit, std::list<path_item> &spaths) {
    bool found = false;
    uint64_t num_nodes = ctx.gdb_->get_nodes()->as_vec().capacity();
    boost::dynamic_bitset<> visited(num_nodes);
    std::vector<uint64_t> parent(num_nodes, UNKNOWN - 1);
    std::vector<double> weight(num_nodes, UNKNOWN_WEIGHT);

    weight[start] = 0.0;
    parent[start] = UNKNOWN;

    // TODO Optimize search for next node with minimum weight
    for (uint64_t i = 0; i < num_nodes; i++) {
        uint64_t min_nid = UNKNOWN;
        double min_weight = UNKNOWN_WEIGHT;
        for (uint64_t nid = 0; nid < num_nodes; nid++) {
            if (!visited[nid] && weight[nid] < min_weight) {
                min_nid = nid;
                min_weight = weight[nid];
            }
        }

        if (min_nid == stop) {
            found = true;
            path_item spath;
            spath.trace_path(parent, stop);
            spath.set_weight(weight[stop]);
            spaths.push_back(spath);
        }
        else if (min_weight == UNKNOWN_WEIGHT)
            return found;

        visited.set(min_nid);

        auto& n = ctx.gdb_->node_by_id(min_nid);
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            auto vid = r.to_node_id();
            if (rpred(r)) {
                auto v_weight = weight_func(r);
                if (!visited[vid] && (weight[min_nid] + v_weight) < weight[vid]) {
                    weight[vid] = weight[min_nid] + v_weight;
                    parent[vid] = min_nid;
                }
                else if (vid == stop && (weight[min_nid] + v_weight) == weight[vid]) {
                    path_item spath;
                    spath.trace_path(parent, stop);
                    spath.set_weight(weight[vid]);
                    spaths.push_back(spath);
                }
            }
        });

        if (bidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                auto vid = r.from_node_id();
                if (rpred(r)) {
                    auto v_weight = weight_func(r);
                    if (!visited[vid] && (weight[min_nid] + v_weight) < weight[vid]) {
                        weight[vid] = weight[min_nid] + v_weight;
                        parent[vid] = min_nid;
                    }
                    else if (vid == stop && (weight[min_nid] + v_weight) == weight[stop]) {
                        path_item spath;
                        spath.trace_path(parent, stop);
                        spath.set_weight(weight[stop]);
                        spaths.push_back(spath);
                    }
                }
            });
        }
    }
    return found;
}

bool w_spath_with_del_rship(query_ctx& ctx, node::id_t start, node::id_t stop, bool bidirectional,
                rship_predicate rpred, rship_weight weight_func, path_visitor visit, path_item &spath) {
    uint64_t num_nodes = ctx.gdb_->get_nodes()->as_vec().capacity();
    boost::dynamic_bitset<> visited(num_nodes);
    std::vector<uint64_t> parent(num_nodes, UNKNOWN - 1);
    std::vector<double> weight(num_nodes, std::numeric_limits<double>::max());

    double path_weight = 0.0;
    weight[start] = 0.0;
    parent[start] = UNKNOWN;

    // TODO Optimize search for next node with minimum weight
    for (uint64_t nid = 0; nid < num_nodes; nid++) {
        uint64_t min_nid = UNKNOWN;
        double min_weight = std::numeric_limits<double>::max();
        for (uint64_t vid = 0; vid < num_nodes; vid++) {
            if (!visited[vid] && weight[vid] < min_weight) {
                min_nid = vid;
                min_weight = weight[vid];
            }
        }
        if (min_weight == std::numeric_limits<double>::max())
            return false;

        visited.set(min_nid);
        path_weight += min_weight;
        if (min_nid == stop) {
            spath.set_weight(path_weight);
            spath.trace_path(parent, stop);
            return true;
        }

        xid_t txid = current_transaction()->xid();
        auto& n = ctx.gdb_->node_by_id(min_nid);
        auto rid = n.from_rship_list;
        while (rid != UNKNOWN) {
            auto& r = ctx.gdb_->get_relationships()->get(rid);
            if (r.is_locked_by(txid)) {
                assert(r.has_dirty_versions());
                if (r.has_valid_version(txid)) {
                    auto vid = r.to_node_id();
                    auto v_weight = weight_func(r);
                    if (rpred(r) && !visited[vid] && (weight[min_nid] + v_weight < weight[vid])) {
                        weight[vid] = weight[min_nid] + v_weight;
                        parent[vid] = min_nid;
                    }
                }
            }
            else if (r.is_valid_for(txid)) {
                auto vid = r.to_node_id();
                auto v_weight = weight_func(r);
                if (rpred(r) && !visited[vid] && (weight[min_nid] + v_weight < weight[vid])) {
                    weight[vid] = weight[min_nid] + v_weight;
                    parent[vid] = min_nid;
                }
            }
            rid = r.next_src_rship;
        }

        if (bidirectional) {
            rid = n.to_rship_list;
            while (rid != UNKNOWN) {
                auto& r = ctx.gdb_->get_relationships()->get(rid);
                if (r.is_locked_by(txid)) {
                    assert(r.has_dirty_versions());
                    if (r.has_valid_version(txid)) {
                        auto vid = r.from_node_id();
                        auto v_weight = weight_func(r);
                        if (rpred(r) && !visited[vid] && (weight[min_nid] + v_weight < weight[vid])) {
                            weight[vid] = weight[min_nid] + v_weight;
                            parent[vid] = min_nid;
                        }
                    }
                }
                else if (r.is_valid_for(txid)) {
                    auto vid = r.from_node_id();
                    auto v_weight = weight_func(r);
                    if (rpred(r) && !visited[vid] && (weight[min_nid] + v_weight < weight[vid])) {
                        weight[vid] = weight[min_nid] + v_weight;
                        parent[vid] = min_nid;
                    }
                }
                rid = r.next_dest_rship;
            }
        }
    }
    return false;
}

bool k_weighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop, std::size_t k, bool bidirectional,
            rship_predicate rpred, rship_weight weight_func, path_visitor visit, std::vector<path_item> &spaths) {
    // find first shortest path
    path_item first_spitem;
    if (!weighted_shortest_path(ctx, start, stop, bidirectional, rpred, weight_func, visit, first_spitem))
        return false;
    spaths.push_back(first_spitem);
    std::vector<path_item> candidate_spitems;

    for (std::size_t i = 1; i < k; i++) {
        auto &prev_path = spaths[i - 1].get_path();
        if (prev_path.size() > 2) {
            // check path deviations
            for (std::size_t j = 0; (j < (prev_path.size() - 2)); j++) {
                node::id_t spur_nid = prev_path.at(j);
                // get Root path (from Start to Spur)
                path root_path(prev_path.begin(), prev_path.begin() + j + 1);

                std::vector<rship_description> del_rships;
                for (auto &p : spaths) {
                    path compare_path(p.get_path().begin(), p.get_path().begin() + j + 1);
                    // delete common relationships
                    if (compare_path == root_path /*&& p.get_path().size() > (j + 1)*/) {    
                        bool d = true;
                        auto src_nid = p.get_path().at(j);
                        auto des_nid = p.get_path().at(j + 1);
                        auto &src_node = ctx.gdb_->node_by_id(src_nid);

                        auto rid = src_node.from_rship_list;
                        xid_t txid = current_transaction()->xid();
                        while (rid != UNKNOWN) {
                            auto& r = ctx.gdb_->get_relationships()->get(rid);
                            if (r.is_locked_by(txid)) {
                                assert(r.has_dirty_versions());
                                if (r.has_valid_version(txid)) {
                                    auto nid = r.to_node_id();
                                    if (nid == des_nid) {
                                        del_rships.push_back(ctx.gdb_->get_rship_description(r.id()));
                                        ctx.gdb_->delete_relationship(r.id());
                                        d = false;
                                    }
                                }
                            }
                            else if (r.is_valid_for(txid)) {
                                auto nid = r.to_node_id();
                                if (nid == des_nid) {
                                    del_rships.push_back(ctx.gdb_->get_rship_description(r.id()));
                                    ctx.gdb_->delete_relationship(r.id());
                                    d = false;
                                }
                            }
                            rid = r.next_src_rship;
                        }

                        if (bidirectional /*&& d*/) {
                            rid = src_node.to_rship_list;
                            while (rid != UNKNOWN) {
                                auto& r = ctx.gdb_->get_relationships()->get(rid);
                                if (r.is_locked_by(txid)) {
                                    assert(r.has_dirty_versions());
                                    if (r.has_valid_version(txid)) {
                                        auto nid = r.from_node_id();
                                        if (nid == des_nid) {
                                            del_rships.push_back(ctx.gdb_->get_rship_description(r.id()));
                                            ctx.gdb_->delete_relationship(r.id());
                                        }
                                    }
                                }
                                else if (r.is_valid_for(txid)) {
                                    auto nid = r.from_node_id();
                                    if (nid == des_nid) {
                                        del_rships.push_back(ctx.gdb_->get_rship_description(r.id()));
                                        ctx.gdb_->delete_relationship(r.id());
                                    }
                                }
                                rid = r.next_dest_rship;
                            }
                        }
                    }
                }
                // find Spur path (from Spur to Stop)
                path_item spur_spitem;
                if (!w_spath_with_del_rship(ctx, spur_nid, stop, bidirectional, rpred, weight_func, visit, spur_spitem))
                    return false;
                // concatenate Root and Spur paths to get Candidate Shortest Path
                path candidate_path(root_path);
                copy(spur_spitem.get_path().begin() + 1, spur_spitem.get_path().end(), back_inserter(candidate_path));
                bool add = true;
                for (auto &cp : candidate_spitems) {
                    if (candidate_path == cp.get_path()) {
                        add = false;
                        break;
                    }
                }
                if (add) {
                    path_item candidate_spitem;
                    candidate_spitem.set_path(candidate_path);
                    candidate_spitems.push_back(candidate_spitem);
                }
                // restore common relationships that were deleted
                if (!del_rships.empty()) {
                    for (std::size_t del = 0; del < del_rships.size(); del++) {
                        auto rdescr = del_rships[del];
                        auto src = rdescr.from_id;
                        auto des = rdescr.to_id;
                        auto &rlabel = rdescr.label;
                        auto &rprops = rdescr.properties;
                        ctx.gdb_->add_relationship(src, des, rlabel, rprops, true);
                    }
                }
            }
        }
        if (candidate_spitems.empty())
            return false;
        // Add minimum weight candidate shortest path to spaths
        auto idx = 0;
        auto min_idx = 0;
        double min_weight = std::numeric_limits<double>::max();
        for (auto &cp : candidate_spitems) {
            if (cp.get_weight() < min_weight) {
                min_idx = idx;
                min_weight = cp.get_weight();
            }
            idx++;
        }
        spaths.push_back(candidate_spitems.at(min_idx));
        candidate_spitems.erase(candidate_spitems.begin() + min_idx);
    }
    return true;
}