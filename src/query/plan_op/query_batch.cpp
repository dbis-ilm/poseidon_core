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

#include "query_batch.hpp"
#include "query_printer.hpp"

void query_batch::start(query_ctx& ctx) {
  for (auto &q : queries_) {
    q.start(ctx);
  }
}

void query_batch::append_printer() {
  // TODO: find the last operator
  auto qop = queries_.at(0).plan_tail_;
  auto op = std::make_shared<printer>();
  qop->connect(op, std::bind(&printer::process, op.get(), ph::_1, ph::_2),
    std::bind(&printer::finish, op.get(), ph::_1));
}

void query_batch::append_collect(result_set& rs) {
  // TODO: find the last operator
  auto qop = queries_.at(0).plan_tail_;
  auto op = std::make_shared<collect_result>(rs);
  qop->connect(op, std::bind(&collect_result::process, op.get(), ph::_1, ph::_2), 
    std::bind(&collect_result::finish, op.get(), ph::_1));;
}

void query_batch::print_plan(std::ostream& os) {
    std::vector<qop_node_ptr> trees;
    for (auto &q : queries_) {
        auto qop_tree = build_qop_tree(q.plan_head_);
        trees.push_back(qop_tree.first);
    }
    std::list<qop_node_ptr> bin_ops;
    for (auto& t : trees) {
      collect_binary_ops(t, bin_ops);
    }
    // merge trees
    for (auto i = 1u; i < trees.size(); i++) {
      // std::cout << "try to merge: #0 + #" << i << "...\n";
        merge_qop_trees(trees[0], trees[i], bin_ops);
    }
    // os << "##----------------------------------------------------------------------\n";
    trees[0]->print(os);
    print_plan_helper(os, trees[0], "");
    // os << "##----------------------------------------------------------------------\n";
}

void query_batch::accept(qop_visitor& visitor) {
    for (auto &q : queries_) {  
      q.plan_head()->accept(visitor);
    }
}