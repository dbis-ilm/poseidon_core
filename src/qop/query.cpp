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

#include "query.hpp"
#include "join.hpp"
// #include "lua_poseidon.hpp"
#include "update.hpp"

namespace ph = std::placeholders;

query &query::append_op(qop_ptr op, qop::consume_func cf) {
  if (!plan_head_)
    plan_head_ = op;
  else
    plan_tail_->connect(op, cf);
  plan_tail_ = op;

  return *this;
}

query &query::append_op(qop_ptr op, qop::consume_func cf, qop::finish_func ff) {
  if (!plan_head_)
    plan_head_ = op;
  else
    plan_tail_->connect(op, cf, ff);
  plan_tail_ = op;

  return *this;
}

query &query::all_nodes(const std::string &label) {
  plan_head_ = plan_tail_ = std::make_shared<scan_nodes>(label);
  return *this;
}

query &query::nodes_where(const std::string &label, const std::string &key,
                          std::function<bool(const p_item &)> pred) {
  plan_head_ = plan_tail_ = std::make_shared<scan_nodes>(label);
  auto op = std::make_shared<is_property>(key, pred);
  return append_op(op,
                   std::bind(&is_property::process, op.get(), ph::_1, ph::_2));
}

query &query::to_relationships(const std::string &label) {
  auto op = std::make_shared<foreach_to_relationship>(label);
  return append_op(op, std::bind(&foreach_to_relationship::process, op.get(),
                                 ph::_1, ph::_2));
}

query &query::to_relationships(std::pair<int, int> range,
                               const std::string &label) {
  auto op = std::make_shared<foreach_variable_to_relationship>(
      label, range.first, range.second);
  return append_op(op, std::bind(&foreach_variable_to_relationship::process,
                                 op.get(), ph::_1, ph::_2));
}

query &query::from_relationships(const std::string &label) {
  auto op = std::make_shared<foreach_from_relationship>(label);
  return append_op(op, std::bind(&foreach_from_relationship::process, op.get(),
                                 ph::_1, ph::_2));
}

query &query::from_relationships(std::pair<int, int> range,
                                 const std::string &label) {
  auto op = std::make_shared<foreach_variable_from_relationship>(
      label, range.first, range.second);
  return append_op(op, std::bind(&foreach_variable_from_relationship::process,
                                 op.get(), ph::_1, ph::_2));
}

query &query::property(const std::string &key,
                       std::function<bool(const p_item &)> pred) {
  auto op = std::make_shared<is_property>(key, pred);
  return append_op(op,
                   std::bind(&is_property::process, op.get(), ph::_1, ph::_2));
}

query &query::to_node(const std::string &label) {
  auto op = std::make_shared<get_to_node>();
  append_op(op, std::bind(&get_to_node::process, op.get(), ph::_1, ph::_2));
  if (!label.empty()) {
    auto op2 = std::make_shared<node_has_label>(label);
    return append_op(
        op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  }
  return *this;
}

query &query::from_node(const std::string &label) {
  auto op = std::make_shared<get_from_node>();
  append_op(op, std::bind(&get_from_node::process, op.get(), ph::_1, ph::_2));
  if (!label.empty()) {
    auto op2 = std::make_shared<node_has_label>(label);
    return append_op(
        op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  }
  return *this;
}

query &query::has_label(const std::string &label) {
  auto op = std::make_shared<node_has_label>(label);
  return append_op(
      op, std::bind(&node_has_label::process, op.get(), ph::_1, ph::_2));
}

query &query::limit(std::size_t n) {
  auto op = std::make_shared<limit_result>(n);
  return append_op(op,
                   std::bind(&limit_result::process, op.get(), ph::_1, ph::_2));
}

query &query::print() {
  auto op = std::make_shared<printer>();
  return append_op(op, std::bind(&printer::process, op.get(), ph::_1, ph::_2));
}

query &query::collect(result_set &rs) {
  auto op = std::make_shared<collect_result>(rs);
  return append_op(
      op, std::bind(&collect_result::process, op.get(), ph::_1, ph::_2),
      std::bind(&collect_result::finish, op.get(), ph::_1));
}

query &query::project(const projection::expr_list &exprs) {
  auto op = std::make_shared<projection>(exprs);
  return append_op(op,
                   std::bind(&projection::process, op.get(), ph::_1, ph::_2));
}

query &
query::orderby(std::function<bool(const qr_tuple &, const qr_tuple &)> cmp) {
  auto op = std::make_shared<order_by>(cmp);
  return append_op(op, std::bind(&order_by::process, op.get(), ph::_1, ph::_2),
                   std::bind(&order_by::finish, op.get(), ph::_1));
}

query &query::crossjoin(query &other) {
  auto op = std::make_shared<cross_join>();
  other.append_op(
      op, std::bind(&cross_join::process_right, op.get(), ph::_1, ph::_2));
  return append_op(
      op, std::bind(&cross_join::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&cross_join::finish, op.get(), ph::_1));
}

query &query::outerjoin(std::pair<int, int> src_des, query &other) {
  auto op = std::make_shared<left_outerjoin>(src_des);
  other.append_op(
      op, std::bind(&left_outerjoin::process_right, op.get(), ph::_1, ph::_2));
  return append_op(
      op, std::bind(&left_outerjoin::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&left_outerjoin::finish, op.get(), ph::_1));
}

/*
query &query::call_lua(const std::string &proc_name,
                       const std::vector<std::size_t> &params) {
  auto op = std::make_shared<call_lua_procedure>(graph_db_, proc_name, params);
  return append_op(
      op, std::bind(&call_lua_procedure::process, op.get(), ph::_1, ph::_2));
}
*/

query &query::create(const std::string &label, const properties_t &props) {
  auto op = std::make_shared<create_node>(label, props);
  return append_op(op,
                   std::bind(&create_node::process, op.get(), ph::_1, ph::_2));
}

query &query::create_rship(std::pair<int, int> src_des, const std::string &label,
                           const properties_t &props) {
  auto op = std::make_shared<create_relationship>(label, props, src_des);
  return append_op(
      op, std::bind(&create_relationship::process, op.get(), ph::_1, ph::_2));
}

query &query::update(std::size_t var, properties_t &props) {
  auto op = std::make_shared<update_node>(var, props);
  return append_op(op,
                   std::bind(&update_node::process, op.get(), ph::_1, ph::_2));
}

void query::start() { plan_head_->start(graph_db_); }

void query::dump(std::ostream &os) { plan_head_->dump(os); }

void query::start(std::initializer_list<query *> queries) {
  for (auto &q : queries) {
    q->start();
  }
}

void query_set::start() {
  for (auto &q : queries_) {
    q.start();
  }
}

void query_set::dump(std::ostream &os) {
  for (auto &q : queries_) {
    q.dump(os);
  }
}
