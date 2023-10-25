/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#ifndef code_table_hpp_
#define code_table_hpp_

#include <string>
#include <limits>
#include "defs.hpp"
#include "robin_hood.h"

class paged_string_pool;

class code_table {
    friend class dict;
public:
    code_table(std::shared_ptr<paged_string_pool> pool) : pool_(pool) {}
    ~code_table() = default;
    
    dcode_t find(const std::string& s);
    dcode_t get(dcode_t id);
    dcode_t insert(const std::string& s, dcode_t id);
    
    void print() const;
    std::size_t size() const { return map_.size(); }
    void rebuild();
    void resize() {};
private:
    std::shared_ptr<paged_string_pool> pool_;   
    robin_hood::unordered_map<uint64_t, dcode_t> map_;
};

#endif /* code_table_hpp */