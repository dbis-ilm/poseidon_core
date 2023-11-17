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

#include "algorithms.hpp"

qr_tuple num_links(query_ctx& ctx, const qr_tuple& v, algorithm_op::param_list& args) {
    int in_links = 0, out_links = 0;
    auto n = boost::get<node *>(v.back()); 

    ctx.foreach_from_relationship_of_node(*n, [&](auto &r) { out_links++; });
    ctx.foreach_to_relationship_of_node(*n, [&](auto &r) { in_links++; });
    
    qr_tuple res(2);
    res[0] = qv_(in_links); res[1] = qv_(out_links);
    return res;
}
