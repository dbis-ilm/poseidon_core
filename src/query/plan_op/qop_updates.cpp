/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#include "qop_updates.hpp"

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

void create_node::start(query_ctx &ctx) { 
  auto &n = ctx.gdb_->node_by_id(ctx.gdb_->add_node(label, props, true));
  consume_(ctx, {&n});
}

void create_node::process(query_ctx &ctx, const qr_tuple &v) {
  auto &n = ctx.gdb_->node_by_id(ctx.gdb_->add_node(label, props, true));
  auto v2 = append(v, query_result(&n));
  consume_(ctx, v2);
}

/* ------------------------------------------------------------------------ */

void create_relationship::dump(std::ostream &os) const {
  os << "create_relationship(" << src_des_nodes_.first;
  os << "->"; 
  os << src_des_nodes_.second << ", [" << label << "]";
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

void create_relationship::process(query_ctx &ctx, const qr_tuple &v) {
  assert(v[src_des_nodes_.first].which() == node_ptr_type);
  auto n1 = boost::get<node *>(v[src_des_nodes_.first]);
  assert(v[src_des_nodes_.second].which() == node_ptr_type);
  auto n2 = boost::get<node *>(v[src_des_nodes_.second]);
  auto rid = ctx.gdb_->add_relationship(n1->id(), n2->id(), label, props, true);
  auto& r = ctx.gdb_->rship_by_id(rid);
  auto v2 = append(v, query_result(&r));
  
  consume_(ctx, v2);
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

void update_node::process(query_ctx &ctx, const qr_tuple &v) {
  // fetch the node to be updated
  auto &ge = v[var_no_];
  assert(ge.which() == node_ptr_type); // TODO: raise exception
  auto n = boost::get<node *>(ge);
  // update the node
  ctx.gdb_->update_node(*n, props);
  consume_(ctx, v);
}

/* ------------------------------------------------------------------------ */

void detach_node::dump(std::ostream &os) const {
  os << "detach_node([" "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void detach_node::process(query_ctx &ctx, const qr_tuple &v) {
  qr_tuple res = v;
  node * n = nullptr;
  if (pos_ == std::numeric_limits<std::size_t>::max()) {
    n = boost::get<node *>(v.back());
    res[(v.size() - 1)] = query_result(null_t(-1));
  }
  else {
    n = boost::get<node *>(v[pos_]);
    res[pos_] = query_result(null_t(-1));
  }

  std::list<relationship::id_t> rships;
  ctx.foreach_from_relationship_of_node(*n, [&](relationship &r) {
    rships.push_back(r.id()); });
  ctx.foreach_to_relationship_of_node(*n, [&](relationship &r) {
    rships.push_back(r.id()); });

  for (auto rid : rships)
    ctx.gdb_->delete_relationship(rid);

  // for(auto rel : rels_)
  //   res[rel] = query_result(null_t(-1));

  ctx.gdb_->delete_node(n->id());

  consume_(ctx, res);
}

/* ------------------------------------------------------------------------ */

void remove_node::dump(std::ostream &os) const {
  os << "remove_node([" "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void remove_node::process(query_ctx &ctx, const qr_tuple &v) {
  qr_tuple res = v;
  node * n = nullptr;
  if (pos_ == std::numeric_limits<std::size_t>::max()) {
    n = boost::get<node *>(v.back());
    res[(v.size() - 1)] = query_result(null_t(-1));
  }
  else {
    n = boost::get<node *>(v[pos_]);
    res[pos_] = query_result(null_t(-1));
  }

  ctx.gdb_->delete_node(n->id());
  consume_(ctx, res);
}

/* ------------------------------------------------------------------------ */

void remove_relationship::dump(std::ostream &os) const {
  os << "remove_relationship([" "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void remove_relationship::process(query_ctx &ctx, const qr_tuple &v) {
  qr_tuple res = v;
  relationship * r = nullptr;
  if (pos_ == std::numeric_limits<std::size_t>::max()) {
    r = boost::get<relationship *>(v.back());
    res[(v.size() - 1)] = query_result(null_t(-1));
  }
  else {
    r = boost::get<relationship *>(v[pos_]);
    res[pos_] = query_result(null_t(-1));
  }

  ctx.gdb_->delete_relationship(r->id());
  consume_(ctx, res);
}