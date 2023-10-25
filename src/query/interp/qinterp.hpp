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
#ifndef qinterp_hpp_
#define qinterp_hpp_

#include "query_batch.hpp"
#include "graph_db.hpp"
#include "query_ctx.hpp"

/**
 * This class implements a query interpreter which simply encapsulates
 * the execution of plan_ops plus an expression interpreter. This query
 * mode does not need LLVM for query compilation.
*/
class qinterp {
public:
    /** 
     * Constructor.
     */
    qinterp() = default;

    /**
     * Destructor.
     */
    ~qinterp() = default;

    /**
     * Starts the given query within the context of a new transaction.
     */
    void execute(query_ctx& ctx, query_batch& qbatch);
};

#endif