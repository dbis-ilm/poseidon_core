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

#include "query_pipeline.hpp"
#include "query_printer.hpp"

query_pipeline::query_pipeline(qop_ptr qop) {
  plan_head_ = qop;
  // initialize plan_tail_
  plan_tail_ = qop;
  while (plan_tail_->has_subscriber())
    plan_tail_ = plan_tail_->subscriber();
}

query_pipeline &query_pipeline::operator=(const query_pipeline &q) {
  plan_head_ = q.plan_head_;
  plan_tail_ = q.plan_tail_;
  return *this;
}

query_pipeline &query_pipeline::append_op(qop_ptr op, qop::consume_func cf) {
  if (!plan_head_)
    plan_head_ = op;
  else
    plan_tail_->connect(op, cf);
  plan_tail_ = op;

  return *this;
}

query_pipeline &query_pipeline::append_op(qop_ptr op, qop::consume_func cf, qop::finish_func ff) {
  if (!plan_head_)
    plan_head_ = op;
  else
    plan_tail_->connect(op, cf, ff);
  plan_tail_ = op;

  return *this;
}


void query_pipeline::start(query_ctx& ctx) { 
  assert(ctx.gdb_->get_dictionary());
  plan_head_->start(ctx); 
}

void query_pipeline::print_plan(std::ostream& os) {
    os << ">>---------------------------------------------------------------------->>\n";
    auto qop_tree = build_qop_tree(plan_head_);
    qop_tree.first->print(os);
    print_plan_helper(os, qop_tree.first, "");
    os << "<<----------------------------------------------------------------------<<\n";
}
