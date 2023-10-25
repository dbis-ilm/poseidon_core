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
#ifndef index_map_hh_
#define index_map_hh_

#include "defs.hpp"
#include "btree.hpp"

#include <unordered_map>

class index_map {
public:
    index_map();
    ~index_map();

    void register_index(const std::string& idx_name, index_id idx);
    void unregister_index(const std::string& idx_name);
    index_id get_index(const std::string& idx_name);
    index_id get_index_id(const std::string& idx_name);
    bool has_index(const std::string& idx_name);

    std::size_t size() const { return indexes_.size(); }

    void clear();

private:
    std::unordered_map<std::string, index_id> indexes_;
};

#endif