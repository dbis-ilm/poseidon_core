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
#ifndef dict_hpp
#define dict_hpp

#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include "defs.hpp"

#include "bufferpool.hpp"
#include "paged_string_pool.hpp"

#include "code_table.hpp"

/**
 * This class implements an (updatable) string dictionary. Strings are stored in
 * a contiguous memory region (string_pool). The position of each string is this
 * memory region is used as the code which replaces the string value. To get the
 * mapping between the code and the actual string a hash table (htable) is used.
 * 
 * The string_pool is stored persistently (either in PMem or in a paged file),
 * the hash table is maintained in memory.
 */ 
class dict {
public:
    /**
     * Create a new dictionary with the initial string pool size. The prefix argument 
     * is used only for the path of a paged file.
     */
    dict(bufferpool& bpool, const std::string& prefix = "", uint32_t init_pool_size = 100000);

    /**
     * Destructor.
     */
    ~dict();
    
  /**
   * The underlying persistent hash tables need a runtime initialization if
   * stored in persistent memory.
   */
    void initialize();

  /**
   * Insert a new string and return a newly assigned code. Duplicate strings
   * are ignored and the already assigned code is returned.
   */
    dcode_t insert(const std::string& s);

  /**
   * Return the code associated with the string s. If this string does not
   * exist then 0 is returned.
   */
    dcode_t lookup_string(const std::string& s) const;

  /**
   * Return the string associated with the given code. If the code does not
   * exist an empty string is returned.
   */
    const char* lookup_code(dcode_t code) const;
    
    /**
     * Printing the content of the string pool for debugging purposes.
     */
    void print_pool() const;

    /**
     * Printing the content of the hash table for debugging purposes.
     */
    void print_table() const;

    /**
     * Resize the dictionary (string pool and hash table.)
     */
    void resize();
    
    /**
     * Return the size of the dictionary, i.e. the number of unique stored strings.
     */
    std::size_t size() const;

    std::size_t count_string_pool_size();
    
    void close_file() { 
	dict_file_->close();
    }

private:
    bufferpool& bpool_;
    std::shared_ptr<paged_file> dict_file_;
    std::shared_ptr<paged_string_pool> pool_;  // the string pool for storing the actual strings
    code_table *table_;  		             // the hash table for mapping codes to strings
    mutable std::shared_mutex m_;        // a mutex for synchronizing access to the dictionary
};

using dict_ptr = p_ptr<dict>;

#endif /* dict_hpp */
