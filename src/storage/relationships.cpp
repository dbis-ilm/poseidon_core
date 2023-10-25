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

#include "relationships.hpp"
#include "thread_pool.hpp"
#include <iostream>
#include <sstream>

#define PARALLEL_INIT

std::ostream &operator<<(std::ostream &os, const rship_description &rdescr) {
  os << ":" << rdescr.label << "[" << rdescr.id << "]{"; // OpenCypher 9.0
  bool first = true;
  for (auto &p : rdescr.properties) {
    if (!first)
      os << ", ";
    os << p.first << ": " << p.second;
    first = false;
  }
  os << "}";
  return os;
}

/* ------------------------------------------------------------------------ */

std::string rship_description::to_string() const {
  std::ostringstream os;
  os << *this;
  return os.str();
}

bool rship_description::has_property(const std::string& pname) const {
  return properties.find(pname) != properties.end();
}

bool rship_description::operator==(const rship_description& other) const {
  return id == other.id && from_id == other.from_id && to_id == other.to_id && label == other.label /* && properties == other.properties */;
}
