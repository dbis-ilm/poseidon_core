/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#ifndef query_printer_hpp_
#define query_printer_hpp_

#include <iostream>
#include <memory>
#include <vector>

/**
 * A helper class for representing the tree of query operators (instances of qop).
 * NOTE: The root of this tree is the final operator producing the results but
 * not the scan.
 */
struct qop_node;
using qop_node_ptr = std::shared_ptr<qop_node>;

struct qop_node {
    qop_ptr qop_; // pointer to the actual query operator
    std::vector<qop_node_ptr> children_; // child nodes (publisher)

    qop_node(qop_ptr p) : qop_(p) {}

    bool is_binary() const { return qop_->is_binary(); }

    void print(std::ostream& os = std::cout) {
        qop_->dump(os); 
        os << std::endl;
    }
};

void print_plan_helper(std::ostream& os, qop_node_ptr root, const std::string& prefix);
void merge_qop_trees(qop_node_ptr master, qop_node_ptr tree, std::list<qop_node_ptr>& bin_ops);
void collect_binary_ops(qop_node_ptr tree, std::list<qop_node_ptr>& ops);

/**
 * Recursively constructs a tree of qop_nodes representing a query plan for printing.
 * This is an upside-down tree where the scan operators are the leaf nodes.
 * The function returns a pair of (root node, current node).
 */
std::pair<qop_node_ptr, qop_node_ptr> build_qop_tree(qop_ptr root);

#endif