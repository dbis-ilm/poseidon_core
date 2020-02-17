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

#include "update.hpp"

/* ------------------------------------------------------------------------ */

void create_node::dump(std::ostream &os) const {
  os << "create_node([" << label << "]";
  if (!props.empty()) {
    os << ", {";
    bool first = true;
    for (auto &p : props) {
      if (!first)
        os << ", ";
      os << p.first << ": " << p.second;
      first = false;
    }
    os << "}";
  }
  os << ")";
}

void create_node::start(graph_db_ptr &gdb) { 
  auto &n = gdb->node_by_id(gdb->add_node(label, props));
  consume_(gdb, {&n});
}

void create_node::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto &n = gdb->node_by_id(gdb->add_node(label, props));
  auto v2 = append(v, query_result(&n));
  consume_(gdb, v2);
}

/* ------------------------------------------------------------------------ */

void create_relationship::dump(std::ostream &os) const {
  os << "create_relationship([" << label << "]";
  if (!props.empty()) {
    os << ", {";
    bool first = true;
    for (auto &p : props) {
      if (!first)
        os << ", ";
      os << p.first << ": " << p.second;
      first = false;
    }
    os << "}";
  }
  os << ")";
}

void create_relationship::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto n1 = boost::get<node *>(v[src_des_nodes_.first]);
  auto n2 = boost::get<node *>(v[src_des_nodes_.second]);
  auto rid = gdb->add_relationship(n1->id(), n2->id(), label, props);
  auto& r = gdb->rship_by_id(rid);
  auto v2 = append(v, query_result(&r));
  
  consume_(gdb, v2);
}

/* ------------------------------------------------------------------------ */

void update_node::dump(std::ostream &os) const {
  os << "update_node([ " << var_no_ << " ]";
  if (!props.empty()) {
    os << ", {";
    bool first = true;
    for (auto &p : props) {
      if (!first)
        os << ", ";
      os << p.first << ": " << p.second;
      first = false;
    }
    os << "}";
  }
  os << ")=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void update_node::process(graph_db_ptr &gdb, const qr_tuple &v) {
  // fetch the node to be updated
  auto &ge = v[var_no_];
  assert(ge.which() == 0); // TODO: raise exception
  auto n = boost::get<node *>(ge);
  // update the node
  gdb->update_node(*n, props);
  consume_(gdb, v);
}
