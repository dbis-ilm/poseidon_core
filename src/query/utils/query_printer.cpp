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
#include "query_builder.hpp"
#include "query_printer.hpp"

std::pair<qop_node_ptr, qop_node_ptr> build_qop_tree(qop_ptr root) {
   // std::cout << "build_qop_tree: "; root->dump(std::cout); std::cout << std::endl;
    if (!root->has_subscriber()) {
        auto troot = std::make_shared<qop_node>(root);
        return std::make_pair(troot, troot);
    }
    auto np = build_qop_tree(root->subscriber());
    auto rn = std::make_shared<qop_node>(root);
    np.second->children_.push_back(rn);
    return std::make_pair(np.first, rn);
}

void collect_binary_ops(qop_node_ptr tree, std::list<qop_node_ptr>& ops) {
    while (tree != nullptr) {
        if (tree->is_binary()) { 
            auto it = std::find_if(ops.begin(), ops.end(), [&](auto o) { return o->qop_ == tree->qop_;});
            if (it == ops.end()) {
                // add to front
                ops.push_front(tree);
            }
        }
        assert(tree->children_.size() <= 1);
        tree = tree->children_.empty() ? nullptr : tree->children_.front();
    }
}

/*
qop_node_ptr find_op_in_tree(qop_node_ptr tree, qop_node_ptr op) {
    auto n = tree;
    while (n != nullptr) {
        if (n->qop_ == op->qop_)
            return n;
        if (n->children_.size() > 1) std::cout << "AAAAAAAAAAAAAA\n";
        n = n->children_.empty() ? nullptr : n->children_[0];
    }
    return nullptr;
}
*/
qop_node_ptr find_op_in_tree(qop_node_ptr n, qop_node_ptr op) {
    if (n == nullptr)
        return nullptr;
    if (n->qop_ == op->qop_)
        return n;
    if (!n->children_.empty()) {
        auto n2 = find_op_in_tree(n->children_[0], op);
        if (n2 != nullptr)
            return n2;
        else if (n->children_.size() == 2)
            return find_op_in_tree(n->children_[1], op);
    }
    return nullptr;
}

std::pair<qop_node_ptr, qop_node_ptr> 
find_common_binary_qop(qop_node_ptr master, qop_node_ptr tree, std::list<qop_node_ptr>& bin_ops) {
    for (auto op : bin_ops) {
        auto m = find_op_in_tree(master, op);
        auto t = find_op_in_tree(tree, op);
 
        if (m != nullptr && t != nullptr) {
            return std::make_pair(m, t);
        }
    }
    return std::make_pair<qop_node_ptr, qop_node_ptr>(nullptr, nullptr);
}

void merge_qop_trees(qop_node_ptr master, qop_node_ptr tree, std::list<qop_node_ptr>& bin_op_list) {
    // 1. find the first binary operator in tree
    std::pair<qop_node_ptr, qop_node_ptr> bin_op = find_common_binary_qop(master, tree, bin_op_list);
    if (! bin_op.first || ! bin_op.second) {
        std::cout << "Ooops, no common bin op!" << std::endl;
        return;
    }
    // 2. merge the trees
    /*
    std::cout << "merge: \n";
    bin_op.first->print();
    bin_op.second->children_[0]->print();
    std::cout << "----\n";
    */
    assert(bin_op.second->children_.size() == 1);
    bin_op.first->children_.push_back(bin_op.second->children_[0]);

    // 3. remove the bin_op from the list
    bin_op_list.remove(bin_op.first);

    // print_plan_helper(std::cout, bin_op.first, "");

}
// --------------------------------------------------------------------------------------

void print_plan_helper(std::ostream& os, qop_node_ptr root, const std::string& prefix) {
    bool hasFirst = (root->children_.size() >= 1);
    bool hasSecond = (root->children_.size() == 2);

    if (!hasFirst && !hasSecond)
        return;

    os << prefix;
    os << ((hasFirst  && hasSecond) ? "├── " : "");
    os << ((!hasFirst && hasSecond) ? "└── " : "");

    if (hasSecond) {
        auto& rchild = root->children_[1];
        bool printStrand = (hasFirst && hasSecond && (rchild->children_.size() > 0));
        std::string newPrefix = prefix + (printStrand ? "│   " : "    ");
        rchild->print(os);
        print_plan_helper(os, rchild, newPrefix);
    }

    if (hasFirst) {
        os << (hasSecond ? prefix : "") << "└── ";
        root->children_[0]->print(os);
        print_plan_helper(os, root->children_[0], prefix + "    ");
    }
}

