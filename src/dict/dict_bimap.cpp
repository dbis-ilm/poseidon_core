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

#include "dict_bimap.hpp"

dict::dict() : last_dcode_(1) {}

dcode_t dict::insert(const std::string &s) {
  std::lock_guard<std::mutex> guard(m_);

  auto iter = dmap_.right.find(s);
  if (iter != dmap_.right.end()) {
    return iter->second;
  }

  auto dcode = last_dcode_++;
  dmap_.insert(dict_bimap_type::value_type(dcode, s));
  return dcode;
}

dcode_t dict::lookup_string(const std::string &s) {
  std::lock_guard<std::mutex> guard(m_);
  auto iter = dmap_.right.find(s);
  return iter != dmap_.right.end() ? iter->second : 0;
}

const char *dict::lookup_code(dcode_t c) {
  std::lock_guard<std::mutex> guard(m_);
  auto iter = dmap_.left.find(c);
  return iter != dmap_.left.end() ? iter->second.c_str() : nullptr;
}

std::size_t dict::size() {
  std::lock_guard<std::mutex> guard(m_);
  return dmap_.size();
}
