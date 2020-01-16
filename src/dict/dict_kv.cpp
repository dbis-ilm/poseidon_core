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

#include "dict_kv.hpp"
#include <libpmemobj++/utils.hpp>

dict::dict() {
  auto pop = pmem::obj::pool_by_vptr(this);
  last_code_ = 1;
  pmem::obj::transaction::run(pop, [&] {
    str_code_map_ = pmem::obj::make_persistent<str_code_hashmap_t>();
    code_str_map_ = pmem::obj::make_persistent<code_str_hashmap_t>();
  });
}

dict::~dict() {
  auto pop = pmem::obj::pool_by_vptr(this);
  pmem::obj::transaction::run(pop, [&] {
    pmem::obj::delete_persistent<str_code_hashmap_t>(str_code_map_);
    pmem::obj::delete_persistent<code_str_hashmap_t>(code_str_map_);
  });
}

void dict::initialize() {
  code_str_map_->runtime_initialize(true);
  str_code_map_->runtime_initialize(true);
}

dcode_t dict::insert(const std::string &s) {
  str_code_hashmap_t::const_accessor ac0;
  string_t str(s);
  if (str_code_map_->find(ac0, str)) {
    return ac0->second;
  }

  auto dcode = last_code_;
  last_code_ = last_code_ + 1;

  {
    str_code_hashmap_t::accessor ac;
    str_code_map_->insert(ac, str);
    ac->second = dcode;
    ac.release();
  }
  {
    code_str_hashmap_t::accessor ac;
    code_str_map_->insert(ac, dcode);
    ac->second = str;
    ac.release();
  }
  return dcode;
}

dcode_t dict::lookup_string(const std::string &s) {
  str_code_hashmap_t::const_accessor ac;
  if (str_code_map_->find(ac, string_t(s)))
    return ac->second;
  else
    return 0;
}

const char *dict::lookup_code(dcode_t c) {
  code_str_hashmap_t::const_accessor ac;
  if (code_str_map_->find(ac, c))
    return ac->second.c_str();
  else
    return nullptr;
}

std::size_t dict::size() { return code_str_map_->size(); }
