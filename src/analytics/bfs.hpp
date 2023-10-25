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

#ifndef bfs_hpp_
#define bfs_hpp_

#include "graph_db.hpp"
#include "query_ctx.hpp"

/**
 * A sequential implementation of the breadth-first search on the given graph. The search starts at the
 * given start node and follows all edges (relationships) satisfying the predicate rpred. For each visited
 * node, the node_visitor callback is invoked. The unidirectional flag determines whether only outgoing 
 * relationships are considered (unidirectional = false) or both outgoing and incoming relationships 
 * (unidirectional = true).
 */
void bfs(query_ctx& ctx, node::id_t start, bool unidirectional, rship_predicate rpred, node_visitor visit);

void path_bfs(query_ctx& ctx, node::id_t start, bool unidirectional, rship_predicate rpred, path_visitor visit);
 
#endif