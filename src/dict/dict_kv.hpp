/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef dict_kv_hpp_
#define dict_kv_hpp_

#include "defs.hpp"

#include "polymorphic_string.hpp"
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include <string>

using string_t = polymorphic_string;

/**
 * dict is a class for mapping strings to integer codes. Its main purpose is
 * to implement string dictionaries for dictionary compression as well as type
 * tables for mapping type names to integer codes. This class implements the
 * dictionary as persistent mapping table via PMDK.
 */
class dict {
public:
  /**
   * Construcotr.
   */
  dict();

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
  dcode_t insert(const std::string &s);

  /**
   * Return the code associated with the string s. If this string does not
   * exist then 0 is returned.
   */
  dcode_t lookup_string(const std::string &s);

  /**
   * Return the string associated with the given code. If the code does not
   * exist an empty string is returned.
   */
  const char *lookup_code(dcode_t c);

  /**
   * Return the size of the dictionary, i.e. the number of unique strings
   * stored in the dictionary.
   */
  std::size_t size();

private:
  /*
    class key_equal {
    public:
      template <typename M, typename U>
      bool operator()(const M &lhs, const U &rhs) const {
        return lhs == rhs;
      }
    };
    */
  class string_hasher {
    /* hash multiplier used by fibonacci hashing */
    static const size_t hash_multiplier = 11400714819323198485ULL;

  public:
    // using transparent_key_equal = key_equal;

    size_t operator()(const string_t &str) const {
      return hash(str.c_str(), str.size());
    }

  private:
    size_t hash(const char *str, size_t size) const {
      size_t h = 0;
      for (size_t i = 0; i < size; ++i) {
        h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
      }
      return h;
    }
  };

  using code_str_hashmap_t = pmem::obj::concurrent_hash_map<dcode_t, string_t>;
  using str_code_hashmap_t =
      pmem::obj::concurrent_hash_map<string_t, dcode_t, string_hasher>;

  pmem::obj::persistent_ptr<code_str_hashmap_t> code_str_map_;
  pmem::obj::persistent_ptr<str_code_hashmap_t> str_code_map_;
  p<dcode_t> last_code_;
};

#endif
