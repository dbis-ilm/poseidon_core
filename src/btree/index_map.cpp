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

#include <boost/hana.hpp>
#include "exceptions.hpp"
#include "index_map.hpp"
#include "spdlog/spdlog.h"

index_map::index_map() {
}

index_map::~index_map() {
}

void index_map::register_index(const std::string& idx_name, index_id idx) {
    indexes_.insert({ idx_name, idx });
}

void index_map::unregister_index(const std::string& idx_name) {
    auto it = indexes_.find(idx_name);
    if (it == indexes_.end())
        throw unknown_index();
    indexes_.erase(it);
}

index_id index_map::get_index(const std::string& idx_name) {
    auto it = indexes_.find(idx_name);
    if (it == indexes_.end())
        throw unknown_index();
    return it->second;
}

index_id index_map::get_index_id(const std::string& idx_name) {
    auto it = indexes_.find(idx_name);
    if (it == indexes_.end())
        return boost::blank{};
    return it->second;  
}

bool index_map::has_index(const std::string& idx_name) {
    auto it = indexes_.find(idx_name);
    return it != indexes_.end();
}

void index_map::clear() {
    auto visitor = boost::hana::overload(
        [&](boost::blank& b) { },
        [&](pf_btree_ptr idx) { idx->close(); },
        [&](im_btree_ptr idx) { }
    );
    for (auto it = indexes_.begin(); it != indexes_.end(); it++) {
        auto idx_id = it->second;    
        boost::apply_visitor(visitor, idx_id);
    }
    indexes_.clear();       
}