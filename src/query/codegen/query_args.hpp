#ifndef POSEIDON_CORE_ARG_EXTRACTOR_HPP
#define POSEIDON_CORE_ARG_EXTRACTOR_HPP

#include "qop_visitor.hpp"
#include "binary_expression.hpp"

class arg_extractor : public qop_visitor {
    arg_builder ab_;
    unsigned arg_cnt_;

    std::vector<base_joiner*> joiner_list_;

public:
    arg_extractor() : arg_cnt_(1) {}

    virtual void visit(std::shared_ptr<scan_nodes> op) override {
       auto s_op = std::dynamic_pointer_cast<scan_nodes>(op);
       if(s_op->labels.size() == 0) {
           ab_.arg(arg_cnt_++, s_op->label);
       } else {
           for(auto & l : s_op->labels) {
               ab_.arg(arg_cnt_++, l);
           }
       }
    }

    virtual void visit(std::shared_ptr<foreach_relationship> op) override {
       auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(op);
       ab_.arg(arg_cnt_++, fe_op->label);
    }

    virtual void visit(std::shared_ptr<expand> op) override {
        auto exp_op = std::dynamic_pointer_cast<expand>(op);
        if(!exp_op->label.empty())
                ab_.arg(arg_cnt_, exp_op->label);
        arg_cnt_++;
    }

    virtual void visit(std::shared_ptr<node_has_label> op) override {
        auto hl_op = std::dynamic_pointer_cast<node_has_label>(op);
        ab_.arg(arg_cnt_++, hl_op->label);
    }

    virtual void visit(std::shared_ptr<collect_result> op) override {
        auto cop = std::dynamic_pointer_cast<collect_result>(op);
        ab_.arg(arg_cnt_++, &cop->results_);
    }

    virtual void visit(std::shared_ptr<filter_tuple> op) override {
        auto ft = std::dynamic_pointer_cast<filter_tuple>(op);
        
        if(ft->ex_->ftype_ == FOP_TYPE::OP) {
            auto bexpr = std::dynamic_pointer_cast<eq_predicate>(ft->ex_);
            
            auto iexpr = std::dynamic_pointer_cast<number_token>(bexpr->right_);
            ab_.arg(arg_cnt_++, iexpr->ivalue_);
        }
    }

    virtual void visit(std::shared_ptr<end_pipeline> op) override {
        auto cop = std::dynamic_pointer_cast<end_pipeline>(op);

        switch(cop->other_) {
            case qop_type::cross_join: {
                auto cj = new cross_joiner();
                joiner_list_.push_back(cj);
                break;
            }
            case qop_type::hash_join: {
                break;
            }
            case qop_type::left_join: {
                break;
            }
            case qop_type::nest_loop_join: {
                auto nlj = new nested_loop_joiner(cop->other_idx_);
                joiner_list_.push_back(nlj);
                break;
            }
        }

        auto j = joiner_list_.back();
        ab_.arg(arg_cnt_++, j);
    }

    virtual void visit(std::shared_ptr<cross_join_op> op) override {
        auto last_joiner = joiner_list_.back();
        ab_.arg(arg_cnt_++, last_joiner);
    }

    arg_builder & get_args() { return ab_; }

};
#endif //POSEIDON_CORE_QID_GENERATOR_HPP
