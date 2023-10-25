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

#ifndef shortest_path_hpp_
#define shortest_path_hpp_

#include "graph_db.hpp"
#include "query_ctx.hpp"

/**
 * A struct containing shortest path information.
 */
struct path_item {
  path_item() : hops_(0), weight_(0.0) {}

  void trace_path(std::vector<uint64_t> &parent, uint64_t v) {
    if (parent[v] == UNKNOWN) {
      path_.push_back(v);
      return;
    }
    trace_path(parent, parent[v]);
    path_.push_back(v);
  }
  void set_path(path &p) {
    path_ = p;
  }
  void set_hops(uint64_t h) {
    hops_ = h;
  }
  void set_weight(double w) {
    weight_ = w;
  }
  const path& get_path() { return path_; }
  uint64_t get_hops() { return hops_; }
  double get_weight() { return weight_; }

private:
  path path_;
  uint64_t hops_;
  double weight_;
};

/**
 * A sequential implementation of unweighted shortest path search on the given graph. The search starts at the
 * given start node and follows all relationships satisfying the predicate rpred. For each visited
 * node, the node_visitor callback is invoked. The bidirectional flag determines whether only outgoing 
 * relationships are considered (bidirectional = false) or both outgoing and incoming relationships 
 * (bidirectional = true).
 * 
 * all_weighted_shortest_paths searches for all the shortest paths between start and stop nodes, 
 * having with equal distance
 */
bool unweighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop,
            bool bidirectional, rship_predicate rpred, path_visitor visit, path_item &spath);
bool all_unweighted_shortest_paths(query_ctx& ctx, node::id_t start, node::id_t stop,
            bool bidirectional, rship_predicate rpred, path_visitor visit, std::list<path_item> &spaths);

/**
 * weighted_shortest_path is a sequential implementation of weighted shortest path search on the given graph. 
 * The search starts at the given start node and follows all relationships satisfying the predicate rpred. 
 * The weight of a traversed relationship is calculated from the weight function. For each visited node, 
 * the node_visitor callback is invoked. 
 * The bidirectional flag determines whether only outgoing relationships are considered 
 * (bidirectional = false) or both outgoing and incoming relationships (bidirectional = true).
 * 
 * all_weighted_shortest_paths searches for all the shortest paths between start and stop nodes, 
 * having with equal weight
 */
bool weighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop, bool bidirectional,
                        rship_predicate rpred, rship_weight weight_func, path_visitor visit, path_item &spath);

bool all_weighted_shortest_paths(query_ctx& ctx, node::id_t start, node::id_t stop, bool bidirectional,
                rship_predicate rpred, rship_weight weight_func, path_visitor visit, std::list<path_item> &spaths);

/**
 * A sequential implementation of first k shortest path search on the given graph.
 */
bool k_weighted_shortest_path(query_ctx& ctx, node::id_t start, node::id_t stop, std::size_t k, bool bidirectional,
                rship_predicate rpred, rship_weight weight_func, path_visitor visit, std::vector<path_item> &spaths);

#endif