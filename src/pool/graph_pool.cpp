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
#include "graph_pool.hpp"

#include <filesystem>

graph_pool_ptr graph_pool::create(const std::string& path, unsigned long long pool_size) {
    auto self = std::make_unique<graph_pool>();
    self->path_ = path;
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj)) {
        std::filesystem::create_directory(path_obj);
    }
    return self;
}

graph_pool_ptr graph_pool::open(const std::string& path, bool init) {
    auto self = std::make_unique<graph_pool>();
    self->path_ = path;
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj)) {
        spdlog::info("FATAL: graph_pool '{}' doesn't exist.", path);
        abort();
    }
    return self;
}

void graph_pool::destroy(graph_pool_ptr& p) {
    for (auto& gp : p->graphs_) { 
        graph_db::destroy(gp.second);
    }
    std::filesystem::path path_obj(p->path_);
    std::filesystem::remove_all(path_obj);  
}

graph_pool::graph_pool() {   
}

graph_pool::~graph_pool() {}

graph_db_ptr graph_pool::create_graph(const std::string& name) {
    auto gptr = p_make_ptr<graph_db>(name, path_);
    graphs_.insert({ name, gptr});
    return gptr;
}

graph_db_ptr graph_pool::open_graph(const std::string& name) {
    // TODO: check whether graph directory exists
    std::filesystem::path path_obj(path_);
    path_obj /=name;
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj)) {
        spdlog::info("FATAL: graph '{}' doesn't exist in pool '{}'.", name, path_);
        throw unknown_db();
    } 
    auto gptr = p_make_ptr<graph_db>(name, path_);
    graphs_.insert({ name, gptr});
    return gptr;
}

void graph_pool::drop_graph(const std::string& name) {
    auto iter = graphs_.find(name);
    if (iter == graphs_.end())
        throw unknown_db();
    std::filesystem::path path_obj(name);
    std::filesystem::remove_all(path_obj);
    graphs_.erase(iter);
}

void graph_pool::close() {   
    for (auto& gp : graphs_) { 
        gp.second->flush();
        gp.second->purge_bufferpool();
        gp.second->close_files();
    }
}