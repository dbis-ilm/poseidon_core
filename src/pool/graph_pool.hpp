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

#ifndef graph_pool_hpp_
#define graph_pool_hpp_

#include "defs.hpp"
#include "graph_db.hpp"

#include <unordered_map>

class graph_pool;
using graph_pool_ptr = std::unique_ptr<graph_pool>;

/**
 * graph_pool is the main entry point to a graph database. It corresponds to a
 * pmem pool and can store multiple graphs identified by their unique name.
 */
class graph_pool {
public:
    friend std::unique_ptr<graph_pool> std::make_unique<graph_pool>();
    /**
     * Create a new pmem pool on the given path with the given size.
     * This method shouldn't be used if a poolset is needed.
     */
    static graph_pool_ptr create(const std::string& path, unsigned long long pool_size = 1024*1024*40000ull);

    /**
     * Open an existing pool. If a pool was created as poolset via 'pmempool create'
     * the pool should be opened with init = true.
     */
    static graph_pool_ptr open(const std::string& path, bool init = false);

    /**
     * Destroys the given pool. Works only with single pool but not with a poolset.
     */
    static void destroy(graph_pool_ptr& p);

    /**
     * Destructor.
     */
    ~graph_pool();

    /**
     * Create a new graph with the given name.
     */
    graph_db_ptr create_graph(const std::string& name);

    /**
     * Open an existing graph with the given name. If no graph
     * exists with this name an exception is raised.
     */
    graph_db_ptr open_graph(const std::string& name);

    void drop_graph(const std::string& name);
    
    /**
     * Close the graph_pool.
     */
    void close();

private:
    /**
     * Private constructor - a graph_pool is created only via the static
     * member functions.
     */
    graph_pool();
   
    std::unordered_map<std::string, graph_db_ptr> graphs_;
    std::string path_;
};

#endif
