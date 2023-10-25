/*
 * Copyright (C) 2019-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#ifndef string_pool_hpp
#define string_pool_hpp

#include <string>
#include "defs.hpp"

class string_pool {
public:
    string_pool(uint32_t init_size = 100000, uint32_t exp_size = 10000);
    ~string_pool();
    
    const char* extract(dcode_t pos) const;
    bool equal(dcode_t pos, const std::string& s) const;
    
    dcode_t add(const std::string& str);
    
    bool scan(std::function<void(const char *s, dcode_t c)> cb);
    void print() const;
    
private:
    char *pool_;
    p<uint32_t> size_, expand_;
    p<uint32_t> last_;
};

#endif /* string_pool_hpp */
