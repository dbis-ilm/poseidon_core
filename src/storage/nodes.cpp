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
#include "nodes.hpp"
#include <iostream>
#include <sstream>

#include "spdlog/spdlog.h"

template <class T>
bool output_any(std::ostream &os, const boost::any &any_value) {
  try {
    T v = boost::any_cast<T>(any_value);
    os << v;
    return true;
  } catch (boost::bad_any_cast &e) {
    return false;
  }
}

template <>
bool output_any<std::string>(std::ostream &os, const boost::any &any_value) {
  try {
    auto v = boost::any_cast<std::string>(any_value);
    os << '"' << v << '"';
    return true;
  } catch (boost::bad_any_cast &e) {
    return false;
  }
}

template <>
bool output_any<const char *>(std::ostream &os, const boost::any &any_value) {
  try {
    auto v = boost::any_cast<const char *>(any_value);
    os << '"' << v << '"';
    return true;
  } catch (boost::bad_any_cast &e) {
    return false;
  }
}
std::ostream &operator<<(std::ostream &os, const boost::any &any_value) {
  // list all types you want to try
  if (!output_any<int>(os, any_value))
    if (!output_any<double>(os, any_value))
      if (!output_any<bool>(os, any_value))
        if (!output_any<std::string>(os, any_value))
          if (!output_any<const char *>(os, any_value))
            if (!output_any<uint64_t>(os, any_value))
              os << "{unknown}"; // all cast are failed, we have a unknown type of any
  return os;
}

std::ostream &operator<<(std::ostream &os, const node_description &ndescr) {
  //  os << "(#" << ndescr.id << ":" << ndescr.label << ", {";
  os << ndescr.label << "[" << ndescr.id << "]{"; // OpenCypher 9.0
  bool first = true;
  for (auto &p : ndescr.properties) {
    if (!first)
      os << ", ";
    os << p.first << ": " << p.second;
    first = false;
  }
  //  os << "})";
  os << "}";
  return os;
}

/* ------------------------------------------------------------------------ */

std::string node_description::to_string() const {
  std::ostringstream os;
  os << *this;
  return os.str();
}

/* ------------------------------------------------------------------------ */

void node_list::runtime_initialize() {
  // make sure that all locks are released and no dirty objects exist
  for (auto &n : nodes_) {
    n.txn_id = 0;
    n.dirty_list = nullptr;
  }
}

node::id_t node_list::append(node &&n, xid_t owner) {
  auto p = nodes_.append(std::move(n));
  p.second->id_ = p.first;
  if (owner != 0) {
    /// spdlog::info("lock node #{} by {}", p.first, owner);
    p.second->lock(owner);
  }

  return p.first;
}

node::id_t node_list::add(node &&n, xid_t owner) {
  if (nodes_.is_full())
    nodes_.resize(1);

  auto id = nodes_.first_available();
  assert(id != UNKNOWN);
  n.id_ = id;
  if (owner != 0) {
    /// spdlog::info("lock node #{} by {}", id, owner);
    n.lock(owner);
  }
  nodes_.store_at(id, std::move(n));
  return id;
}

node &node_list::get(node::id_t id) {
  if (nodes_.capacity() <= id)
    throw unknown_id();
  auto &n = nodes_.at(id);
  return n;
}

void node_list::remove(node::id_t id) {
  if (nodes_.capacity() <= id)
    throw unknown_id();
  auto &n = nodes_.at(id);
  if (n.dirty_list) //Cannot use: if(n.has_dirty_versions()) because if dirty_list is empty, then resource not deleted.
    delete n.dirty_list;
  nodes_.erase(id);
}


node_list::~node_list(){
	// Since dirty_list is not a smart pointer, clear all resources used for dirty list.
	for (auto &n : nodes_) {
		if(n.dirty_list) {
			delete n.dirty_list;
			n.dirty_list = nullptr;
		}
	}
}

void node_list::dump() {
  std::cout << "----------- NODES -----------\n";
  for (const auto& n : nodes_) {
    std::cout << "#" << n.id() << ", @" << (unsigned long)&n
              << " [ tx=" << n.txn_id.load() << ", bts=" << n.bts
              << ", cts=" << n.cts << "], label=" << n.node_label << ", "
              << n.from_rship_list << ", " << n.to_rship_list << ", "
              << n.property_list;
    if (n.has_dirty_versions()) {
      // TODO: print dirty list
      std::cout << " {";
      for (const auto& dn : *n.dirty_list) {
        std::cout << "( @" << (unsigned long)&(dn->elem_)
                  << ", tx=" << dn->elem_.txn_id.load()
                  << ", btx=" << dn->elem_.bts << ", ctx=" << dn->elem_.cts
                  << ")";
      }
      std::cout << "]}";
    }
    std::cout << "\n";
  }
  std::cout << "-----------------------------\n";
}
