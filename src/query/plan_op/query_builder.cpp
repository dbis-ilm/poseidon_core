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

#include "query_builder.hpp"
#include "qop_joins.hpp"
#include "qop_scans.hpp"
#include "qop_relationships.hpp"
#include "qop_updates.hpp"
#include "qop_aggregates.hpp"
#include "query_printer.hpp"
#include <memory>

namespace ph = std::placeholders;

/*
query_builder::query_builder(query_ctx& ctx, qop_ptr qop) : ctx_(ctx) {
  qpipeline_.plan_head_ = qop;
  // initialize plan_tail_
  qpipeline_.plan_tail_ = qop;
  while (qpipeline_.plan_tail_->has_subscriber())
    qpipeline_.plan_tail_ = qpipeline_.plan_tail_->subscriber();
}
*/

query_builder &query_builder::operator=(const query_builder &q) {
  ctx_ = q.ctx_;
  qpipeline_ = q.qpipeline_;
  return *this;
}

query_builder &query_builder::all_nodes(const std::string &label) {  
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<scan_nodes>(label);
  return *this;
}

query_builder &query_builder::all_nodes(std::map<std::size_t, std::vector<std::size_t>> &range_map, const std::string &label) {
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<scan_nodes>(label, range_map);
  return *this;
}

query_builder &query_builder::nodes_where(const std::string &label, const std::string &key,
                          std::function<bool(const p_item &)> pred) {
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<scan_nodes>(label);
  auto op = std::make_shared<is_property>(key, pred);
  qpipeline_.append_op(op, std::bind(&is_property::process, op.get(), ph::_1, ph::_2));
  return *this;
}

query_builder &query_builder::nodes_where(const std::vector<std::string> &labels, const std::string &key,
                          std::function<bool(const p_item &)> pred) {
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<scan_nodes>(labels);
  auto op = std::make_shared<is_property>(key, pred);
  qpipeline_.append_op(op, std::bind(&is_property::process, op.get(), ph::_1, ph::_2));
  return *this;
}

query_builder &query_builder::nodes_where_indexed(const std::string &label, const std::string &prop, uint64_t val) {
  auto idx = ctx_.gdb_->get_index(label, prop);
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<index_scan>(idx, val);
  return *this;
}

query_builder &query_builder::nodes_where_indexed(const std::vector<std::string> &labels,
                                  const std::string &prop, uint64_t val) {
  std::list<index_id> idxs;
  for (auto &label : labels) {
    auto idx = ctx_.gdb_->get_index(label, prop);
    idxs.push_back(idx);
  }
  qpipeline_.plan_head_ = qpipeline_.plan_tail_ = std::make_shared<index_scan>(idxs, val);
  return *this;
}

query_builder &query_builder::to_relationships(const std::string &label, int pos) {
  auto op = std::make_shared<foreach_to_relationship>(label, pos);
  qpipeline_.append_op(op, std::bind(&foreach_to_relationship::process, op.get(),
                                 ph::_1, ph::_2));
  return *this;
}

query_builder &query_builder::to_relationships(std::pair<int, int> range,
                               const std::string &label, int pos) {
  auto op = std::make_shared<foreach_variable_to_relationship>(
      label, range.first, range.second, pos);
  qpipeline_.append_op(op, std::bind(&foreach_variable_to_relationship::process,
                                 op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::from_relationships(const std::string &label, int pos) {
  auto op = std::make_shared<foreach_from_relationship>(label, pos);
  qpipeline_.append_op(op, std::bind(&foreach_from_relationship::process, op.get(),
                                 ph::_1, ph::_2));
  return *this;                               

}

query_builder &query_builder::from_relationships(std::pair<int, int> range,
                                 const std::string &label, int pos) {
  auto op = std::make_shared<foreach_variable_from_relationship>(
      label, range.first, range.second, pos);
  qpipeline_.append_op(op, std::bind(&foreach_variable_from_relationship::process,
                                 op.get(), ph::_1, ph::_2));
  return *this;                               

}

query_builder &query_builder::all_relationships(const std::string &label, int pos) {
  auto op = std::make_shared<foreach_all_relationship>(label, pos);
  qpipeline_.append_op(op, std::bind(&foreach_all_relationship::process, op.get(),
                                 ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::all_relationships(std::pair<int, int> range,
                                 const std::string &label, int pos) {
  auto op = std::make_shared<foreach_variable_all_relationship>(
      label, range.first, range.second, pos);
  qpipeline_.append_op(op, std::bind(&foreach_variable_all_relationship::process,
                                 op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::property(const std::string &key,
                       std::function<bool(const p_item &)> pred) {
  auto op = std::make_shared<is_property>(key, pred);
  qpipeline_.append_op(op,
                   std::bind(&is_property::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::filter(const expr &ex) {
  auto op = std::make_shared<filter_tuple>(ex);
  qpipeline_.append_op(op,
                   std::bind(&filter_tuple::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::to_node(const std::string &label) {
  auto op = std::make_shared<get_to_node>();
  qpipeline_.append_op(op, std::bind(&get_to_node::process, op.get(), ph::_1, ph::_2));
  if (!label.empty()) {
    auto op2 = std::make_shared<node_has_label>(label);
    qpipeline_.append_op(
        op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  }
  return *this;
}

query_builder &query_builder::to_node(const std::vector<std::string> &labels) {
  auto op = std::make_shared<get_to_node>();
  qpipeline_.append_op(op, std::bind(&get_to_node::process, op.get(), ph::_1, ph::_2));
  auto op2 = std::make_shared<node_has_label>(labels);
  qpipeline_.append_op(
      op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::from_node(const std::string &label) {
  auto op = std::make_shared<get_from_node>();
  qpipeline_.append_op(op, std::bind(&get_from_node::process, op.get(), ph::_1, ph::_2));
  if (!label.empty()) {
    auto op2 = std::make_shared<node_has_label>(label);
    qpipeline_.append_op(
        op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  }
  return *this;
}

query_builder &query_builder::from_node(const std::vector<std::string> &labels) {
  auto op = std::make_shared<get_from_node>();
  qpipeline_.append_op(op, std::bind(&get_from_node::process, op.get(), ph::_1, ph::_2));
  auto op2 = std::make_shared<node_has_label>(labels);
  qpipeline_.append_op(
      op2, std::bind(&node_has_label::process, op2.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::has_label(const std::string &label) {
  auto op = std::make_shared<node_has_label>(label);
  qpipeline_.append_op(
      op, std::bind(&node_has_label::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::has_label(const std::vector<std::string> &labels) {
  auto op = std::make_shared<node_has_label>(labels);
  qpipeline_.append_op(
      op, std::bind(&node_has_label::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::limit(std::size_t n) {
  auto op = std::make_shared<limit_result>(n);
  qpipeline_.append_op(op,
                   std::bind(&limit_result::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::print() {
  auto op = std::make_shared<printer>();
  qpipeline_.append_op(op, std::bind(&printer::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::collect(result_set &rs) {
  auto op = std::make_shared<collect_result>(rs);
  qpipeline_.append_op(
      op, std::bind(&collect_result::process, op.get(), ph::_1, ph::_2),
      std::bind(&collect_result::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::finish() {
  auto op = std::make_shared<end_pipeline>();
  qpipeline_.append_op(op, std::bind(&end_pipeline::process, op.get()));
  return *this;                               
}

query_builder &query_builder::project(const projection::expr_list &exprs) {
  auto op = std::make_shared<projection>(exprs);
  qpipeline_.append_op(op,
                   std::bind(&projection::process, op.get(), ph::_1, ph::_2));  
  return *this;                               
}

query_builder &query_builder::orderby(std::function<bool(const qr_tuple &, const qr_tuple &)> cmp) {
  auto op = std::make_shared<order_by>(cmp);
  qpipeline_.append_op(op, std::bind(&order_by::process, op.get(), ph::_1, ph::_2),
                   std::bind(&order_by::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::groupby(const std::vector<group_by::group>& grps, const std::vector<group_by::expr>& exprs) {
  auto op = std::make_shared<group_by>(grps, exprs, ctx_.get_dictionary());
  qpipeline_.append_op(op, std::bind(&group_by::process, op.get(), ph::_1, ph::_2),
                   std::bind(&group_by::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::aggr(const std::vector<aggregate::expr>& exprs) {
  auto op = std::make_shared<aggregate>(exprs, ctx_.get_dictionary());
  qpipeline_.append_op(op, std::bind(&aggregate::process, op.get(), ph::_1, ph::_2),
                   std::bind(&aggregate::finish, op.get(), ph::_1));

  return *this;                               
}

query_builder &
query_builder::distinct() {
  auto op = std::make_shared<distinct_tuples>();
  qpipeline_.append_op(op, std::bind(&distinct_tuples::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &
query_builder::where_qr_tuple(std::function<bool(const qr_tuple &)> pred) {
  auto op = std::make_shared<filter_tuple>(pred);
  qpipeline_.append_op(op,
                   std::bind(&filter_tuple::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}


query_builder &query_builder::union_all(query_pipeline &other) {
  auto op = std::make_shared<union_all_op>();
  other.append_op(
      op, std::bind(&union_all_op::process_right, op.get(), ph::_1, ph::_2));
  qpipeline_.append_op(
      op, std::bind(&union_all_op::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&union_all_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::union_all(std::initializer_list<query_pipeline *> queries) {
  auto op = std::make_shared<union_all_op>();
  for (auto &q : queries)
    q->append_op(
        op, std::bind(&union_all_op::process_right, op.get(), ph::_1, ph::_2));
  qpipeline_.append_op(
      op, std::bind(&union_all_op::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&union_all_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::count() {
  std::vector<aggregate::expr> exprs;
  exprs.push_back(aggregate::expr(aggregate::expr::f_count, 0, "", int_type));
  auto op = std::make_shared<aggregate>(exprs, ctx_.get_dictionary());
  qpipeline_.append_op(op,
                   std::bind(&aggregate::process, op.get(), ph::_1, ph::_2),
                   std::bind(&aggregate::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::cross_join(query_pipeline &other) {
  auto op = std::make_shared<cross_join_op>(other.plan_head());
  other.append_op(
      op, std::bind(&cross_join_op::process_right, op.get(), ph::_1, ph::_2));
  qpipeline_.append_op(
      op, std::bind(&cross_join_op::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&cross_join_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::nested_loop_join(std::pair<int, int> left_right, query_pipeline &other) {
  auto op = std::make_shared<nested_loop_join_op>(left_right, other.plan_head());
  other.append_op(
      op, std::bind(&nested_loop_join_op::process_right, op.get(), ph::_1, ph::_2));
  qpipeline_.append_op(
      op, std::bind(&nested_loop_join_op::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&nested_loop_join_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::hash_join(std::pair<int, int> left_right, query_pipeline &other) {
  auto op = std::make_shared<hash_join_op>(left_right, other.plan_head());
  other.append_op(
      op, std::bind(&hash_join_op::build_phase, op.get(), ph::_1, ph::_2));
  qpipeline_.append_op(
      op, std::bind(&hash_join_op::probe_phase, op.get(), ph::_1, ph::_2),
      std::bind(&hash_join_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::left_outer_join(query_pipeline &other, std::function<bool(const qr_tuple &, const qr_tuple &)> pred) {
  auto op = std::make_shared<left_outer_join_op>(pred);
  other.append_op(
      op, std::bind(&left_outer_join_op::process_right, op.get(), ph::_1, ph::_2),
      std::bind(&left_outer_join_op::finish, op.get(), ph::_1));
  qpipeline_.append_op(
      op, std::bind(&left_outer_join_op::process_left, op.get(), ph::_1, ph::_2),
      std::bind(&left_outer_join_op::finish, op.get(), ph::_1));
  return *this;                               
}

query_builder &query_builder::algo_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
                      rship_predicate rpred, bool bidirectional, bool all_spaths) {
  auto op = std::make_shared<shortest_path_opr>(start_stop, rpred, bidirectional, all_spaths);
  qpipeline_.append_op(op,
                   std::bind(&shortest_path_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::algo_weighted_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
            rship_predicate rpred, rship_weight weight, bool bidirectional, bool all_spaths) {
  auto op = std::make_shared<weighted_shortest_path_opr>(start_stop, rpred, weight,
                                                          bidirectional, all_spaths);
  qpipeline_.append_op(op,
                   std::bind(&weighted_shortest_path_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

#ifdef USE_GUNROCK
query_builder &query_builder::gunrock_bfs(std::size_t start, bool bidirectional) {
  auto op = std::make_shared<gunrock_bfs_opr>(start, bidirectional);
  qpipeline_.append_op(op,
                   std::bind(&gunrock_bfs_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::gunrock_sssp(std::size_t start, rship_weight weight, bool bidirectional) {
  auto op = std::make_shared<gunrock_sssp_opr>(start, weight, bidirectional);
  qpipeline_.append_op(op,
                   std::bind(&gunrock_sssp_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::gunrock_pr(bool bidirectional) {
  auto op = std::make_shared<gunrock_pr_opr>(bidirectional);
  qpipeline_.append_op(op,
                   std::bind(&gunrock_pr_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}
#endif

query_builder &query_builder::algo_k_weighted_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
      std::size_t k, rship_predicate rpred, rship_weight weight, bool bidirectional) {
  auto op = std::make_shared<k_weighted_shortest_path_opr>(start_stop, k, rpred, weight, bidirectional);
  qpipeline_.append_op(op,
                   std::bind(&k_weighted_shortest_path_opr::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::csr(rship_weight weight, bool bidirectional, std::size_t pos) {
  auto op = std::make_shared<csr_data>(weight, bidirectional, pos);
  qpipeline_.append_op(op,
                   std::bind(&csr_data::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::create(const std::string &label, const properties_t &props) {
  auto op = std::make_shared<create_node>(label, props);
  qpipeline_.append_op(op,
                   std::bind(&create_node::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::create_rship(std::pair<int, int> src_des, const std::string &label,
                           const properties_t &props) {
  auto op = std::make_shared<create_relationship>(label, props, src_des);
  qpipeline_.append_op(
      op, std::bind(&create_relationship::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::update(std::size_t var, properties_t &props) {
  auto op = std::make_shared<update_node>(var, props);
  qpipeline_.append_op(op,
                   std::bind(&update_node::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::delete_detach(const std::size_t pos) {
  auto op = std::make_shared<detach_node>(pos);
  qpipeline_.append_op(op,
                   std::bind(&detach_node::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::delete_node(const std::size_t pos) {
  auto op = std::make_shared<remove_node>(pos);
  qpipeline_.append_op(op,
                   std::bind(&remove_node::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}

query_builder &query_builder::delete_rship(const std::size_t pos) {
  auto op = std::make_shared<remove_relationship>(pos);
  qpipeline_.append_op(op,
                   std::bind(&remove_relationship::process, op.get(), ph::_1, ph::_2));
  return *this;                               
}