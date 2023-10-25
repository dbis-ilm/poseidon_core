#ifndef POSEIDON_CORE_INTERPRETER_HPP
#define POSEIDON_CORE_INTERPRETER_HPP

#include "expression.hpp"
#include "query.hpp"
#include "interprete_filter_visitor.h"


class interprete_visitor : public op_visitor {
    graph_db_ptr gdb_;
    query query_;
    arg_builder args_;
    interprete_filter_visitor ifv_;
    result_set *rs_;
    projection::expr_list pexpr_;

    std::vector<std::size_t> grp_pos_;

    std::vector<query> queries_;
public:
    interprete_visitor(graph_db_ptr gdb, arg_builder & args, result_set *rs);

    void visit(std::shared_ptr<scan_op> op) override;

    void visit(std::shared_ptr<foreach_rship_op> op) override;

    void visit(std::shared_ptr<project> op) override;

    void visit(std::shared_ptr<expand_op> op) override;

    void visit(std::shared_ptr<filter_op> op) override;

    void visit(std::shared_ptr<collect_op> op) override;

    void visit(std::shared_ptr<join_op> op) override;

    void visit(std::shared_ptr<sort_op> op) override;

    void visit(std::shared_ptr<limit_op> op) override;

    void visit(std::shared_ptr<end_op> op) override;

    void visit(std::shared_ptr<create_op> op) override;

    void visit(std::shared_ptr<group_op> op) override;

    void visit(std::shared_ptr<aggr_op> op) override;

    void visit(std::shared_ptr<connected_op> op) override;

    void visit(std::shared_ptr<append_op> op) override;

    void visit(std::shared_ptr<store_op> op) override;

    void start();

    result_set &get_results() { return *rs_; }
};


#endif //POSEIDON_CORE_INTERPRETER_HPP
