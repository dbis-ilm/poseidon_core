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
#include <boost/dynamic_bitset.hpp>

#include "bfs.hpp"

void bfs(query_ctx& ctx, node::id_t start, bool unidirectional, rship_predicate rpred, node_visitor visit) {
    std::queue<node::id_t> frontier;
    boost::dynamic_bitset<> visited(ctx.gdb_->get_nodes()->as_vec().capacity());
    frontier.push(start);

    while (!frontier.empty()) {
        auto v = frontier.front();    
        frontier.pop();
        if (visited[v])
            continue;

        visited.set(v);
        auto& n = ctx.gdb_->node_by_id(v);
        visit(n);
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            if (rpred(r)) {
                frontier.push(r.to_node_id());
            }
        });

        if (unidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                if (rpred(r)) {
                    frontier.push(r.from_node_id());
                }
            });
        }
    }
}

void path_bfs(query_ctx& ctx, node::id_t start, bool unidirectional, rship_predicate rpred, path_visitor visit) {
    std::queue<path> frontier;
    boost::dynamic_bitset<> visited(ctx.gdb_->get_nodes()->as_vec().capacity());
    frontier.push({start});

    while (!frontier.empty()) {
        auto v = frontier.front();
        auto vid = v.back();    
        frontier.pop();
        if (visited[vid])
            continue;

        visited.set(vid);
        auto& n = ctx.gdb_->node_by_id(vid);
        visit(n, v);
       
        ctx.foreach_from_relationship_of_node(n, [&](auto &r) {
            if (rpred(r)) {
                path v2(v);
                v2.push_back(r.to_node_id());
                frontier.push(v2);
            }
        });

        if (unidirectional) {
            ctx.foreach_to_relationship_of_node(n, [&](auto &r) {
                if (rpred(r)) {
                    path v2(v);
                    v2.push_back(r.from_node_id());
                    frontier.push(v2);
                }
            });
        }
    }

}
