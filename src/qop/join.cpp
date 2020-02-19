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

#include "join.hpp"

void cross_join::dump(std::ostream &os) const { // TODO
  os << "cross_join()=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void cross_join::process_left(graph_db_ptr &gdb, const qr_tuple &v) {
  for (auto &inp : input_) {
    auto res = concat(v, inp);
    consume_(gdb, res);
  }
}

void cross_join::process_right(graph_db_ptr &gdb, const qr_tuple &v) {
  input_.push_back(v);
}

void cross_join::finish(graph_db_ptr &gdb) { qop::default_finish(gdb); }

/* ------------------------------------------------------------------------ */

void left_outerjoin::dump(std::ostream &os) const { // TODO
  os << "left_outerjoin()=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void left_outerjoin::process_left(graph_db_ptr &gdb, const qr_tuple &v) {
  auto src = boost::get<node *>(v[src_des_nodes_.first]);
  bool dangling_tuple = true;

  for (auto &inp : input_) {
    auto des = boost::get<node *>(inp[src_des_nodes_.second]);
    gdb->foreach_from_relationship_of_node((*src), [&](auto &r) {
      if (r.to_node_id() == des->id()){
        dangling_tuple = false;
        auto res = append(concat(v, inp), query_result(&r));
        consume_(gdb, res);
      }
    });
    if (dangling_tuple){
      auto res = append(concat(v, inp), query_result(std::string("NULL")));
      consume_(gdb, res);
    }
  }
}

void left_outerjoin::process_right(graph_db_ptr &gdb, const qr_tuple &v) {
  input_.push_back(v);
}

void left_outerjoin::finish(graph_db_ptr &gdb) { qop::default_finish(gdb); }