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
#include <iostream>
#include <sstream>

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

/* ------------------------------------------------------------------------ */

void relationship_list::runtime_initialize() {
  // make sure that all locks are released and no dirty objects exist
  for (auto &r : rships_) {
    r.txn_id = 0;
    r.dirty_list = nullptr;
  }
}

relationship::id_t relationship_list::append(relationship &&r, xid_t owner) {
  auto p = rships_.append(std::move(r));
  p.second->id_ = p.first;
  if (owner != 0) {
    /// spdlog::info("lock relationship #{} by {}", p.first, owner);
    p.second->lock(owner);
  }
  return p.first;
}

relationship::id_t relationship_list::add(relationship &&r, xid_t owner) {
  if (rships_.is_full())
    rships_.resize(1);

  auto id = rships_.first_available();
  assert(id != UNKNOWN);
  r.id_ = id;
  if (owner != 0) {
    /// spdlog::info("lock relationship #{} by {}", id, owner);
    r.lock(owner);
  }
  rships_.store_at(id, std::move(r));
  return id;
}

relationship &relationship_list::get(relationship::id_t id) {
  if (rships_.capacity() <= id)
    throw unknown_id();
  auto &r = rships_.at(id);
  return r;
}

void relationship_list::remove(relationship::id_t id) {
  if (rships_.capacity() <= id)
    throw unknown_id();
  auto &r = rships_.at(id);
  if (r.has_dirty_versions())
    delete r.dirty_list;
  rships_.erase(id);
}

relationship &
relationship_list::last_in_from_list(relationship::id_t id) {
  relationship *rship = &get(id);
  while (rship->next_src_rship != UNKNOWN) {
    rship = &get(rship->next_src_rship);
  }
  return *rship;
}

relationship &
relationship_list::last_in_to_list(relationship::id_t id) {
  relationship *rship = &get(id);
  while (rship->next_dest_rship != UNKNOWN) {
    rship = &get(rship->next_dest_rship);
  }
  return *rship;
}

void relationship_list::dump() {
  std::cout << "------- RELATIONSHIPS -------\n";
  for (auto &r : rships_) {
    std::cout << "#" << r.id() << ", " << r.rship_label << ", " << r.src_node
              << "->" << r.dest_node << ", " << r.next_src_rship << ", "
              << r.next_dest_rship << "\n";
  }
  std::cout << "-----------------------------\n";
}
