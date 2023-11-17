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

#ifndef qop_visitor_hpp_
#define qop_visitor_hpp_

#include <memory>

struct scan_nodes;
struct index_scan;
struct foreach_from_relationship;
struct foreach_variable_from_relationship;
struct foreach_all_from_relationship;
struct foreach_variable_all_from_relationship;
struct foreach_variable_to_relationship;
struct foreach_relationship;
struct expand;
struct is_property;
struct node_has_label;
struct get_from_node;
struct get_to_node;
struct printer;
struct limit_result;
struct order_by;
struct aggregate;
struct group_by;
struct distinct_tuples;
struct filter_tuple;
struct union_all_op;
struct shortest_path_opr;
struct weighted_shortest_path_opr;
struct k_weighted_shortest_path_opr;
struct collect_result;
struct end_pipeline;
struct projection;
struct cross_join_op;
struct nested_loop_join_op;
struct hash_join_op;
struct left_outer_join_op;
struct create_node;
struct create_relationship;
struct update_node;
struct detach_node;
struct remove_node;
struct remove_relationship;
struct algorithm_op;

class qop_visitor {
public:
    virtual ~qop_visitor() = default;

    virtual void visit(std::shared_ptr<scan_nodes> op) {}
    virtual void visit(std::shared_ptr<index_scan> op) {}
    virtual void visit(std::shared_ptr<foreach_relationship> op) {}
    virtual void visit(std::shared_ptr<is_property> op) {}
    virtual void visit(std::shared_ptr<node_has_label> op) {}
    virtual void visit(std::shared_ptr<expand> op) {}
    virtual void visit(std::shared_ptr<printer> op) {}
    virtual void visit(std::shared_ptr<limit_result> op) {}
    virtual void visit(std::shared_ptr<order_by> op) {}
    virtual void visit(std::shared_ptr<aggregate> op) {}
    virtual void visit(std::shared_ptr<group_by> op) {}
    virtual void visit(std::shared_ptr<distinct_tuples> op) {}
    virtual void visit(std::shared_ptr<filter_tuple> op) {}
    virtual void visit(std::shared_ptr<union_all_op> op) {}
    virtual void visit(std::shared_ptr<shortest_path_opr> op) {}
    virtual void visit(std::shared_ptr<weighted_shortest_path_opr> op) {}
    virtual void visit(std::shared_ptr<k_weighted_shortest_path_opr> op) {}
    virtual void visit(std::shared_ptr<collect_result> op) {}
    virtual void visit(std::shared_ptr<end_pipeline> op) {}
    virtual void visit(std::shared_ptr<projection> op) {}
    virtual void visit(std::shared_ptr<cross_join_op> op) {}
    virtual void visit(std::shared_ptr<nested_loop_join_op> op) {}
    virtual void visit(std::shared_ptr<hash_join_op> op) {}
    virtual void visit(std::shared_ptr<left_outer_join_op> op) {}
    virtual void visit(std::shared_ptr<create_node> op) {}
    virtual void visit(std::shared_ptr<create_relationship> op) {}
    virtual void visit(std::shared_ptr<update_node> op) {}
    virtual void visit(std::shared_ptr<detach_node> op) {}
    virtual void visit(std::shared_ptr<remove_node> op) {}
    virtual void visit(std::shared_ptr<remove_relationship> op) {}
    virtual void visit(std::shared_ptr<algorithm_op> op) {}
};

#endif