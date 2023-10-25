/*
 * Copyright (C) 2019-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include <stack>

#include "properties.hpp"
#include "expr_interpreter.hpp"
#include "func_call_expr.hpp"

std::ostream& operator<<(std::ostream& os, const query_result& qr) {
    os << "[" << qr.which() << "]";
    return os;
}

// ----------- query_result::int -----------
inline bool int_not_equal(const query_result& r1, const query_result& r2) {
    return boost::get<int>(r1) != boost::get<int>(r2);
}

inline bool int_greater_than(const query_result& r1, const query_result& r2) {
    return boost::get<int>(r1) > boost::get<int>(r2);
}

inline bool int_less_than(const query_result& r1, const query_result& r2) {
    return boost::get<int>(r1) < boost::get<int>(r2);
}

inline bool uint64_less_than(const query_result& r1, const query_result& r2) {
    return boost::get<uint64_t>(r1) < boost::get<uint64_t>(r2);
}

inline bool uint64_greater_than(const query_result& r1, const query_result& r2) {
    return boost::get<uint64_t>(r1) > boost::get<uint64_t>(r2);
}

// ----------- query_result::double -----------
inline bool double_not_equal(const query_result& r1, const query_result& r2) {
    return boost::get<double>(r1) != boost::get<double>(r2);
}

inline bool double_greater_than(const query_result& r1, const query_result& r2) {
    return boost::get<double>(r1) > boost::get<double>(r2);
}

inline bool double_less_than(const query_result& r1, const query_result& r2) {
    return boost::get<double>(r1) < boost::get<double>(r2);
}

// ----------- query_result::string -----------
inline bool string_not_equal(const query_result& r1, const query_result& r2) {
    return boost::get<std::string>(r1) != boost::get<std::string>(r2);
}

inline bool string_equal(const query_result& r1, const query_result& r2) {
    return boost::get<std::string>(r1) == boost::get<std::string>(r2);
}

inline bool string_greater_than(const query_result& r1, const query_result& r2) {
    return boost::get<std::string>(r1) > boost::get<std::string>(r2);
}

inline bool string_less_than(const query_result& r1, const query_result& r2) {
    return boost::get<std::string>(r1) < boost::get<std::string>(r2);
}

// ----------- query_result -----------
bool equal(const query_result& qr1, const query_result& qr2) {
    // std::cout << "equal: " << qr1.which() << "-" << qr2.which() << std::endl;
    if (qr1.which() == qr2.which()) {
        switch (qr1.which()) {
            case node_ptr_type: // node *
                return boost::get<node*>(qr1) == boost::get<node*>(qr2);
            case rship_ptr_type: // relationship *
                return boost::get<relationship*>(qr1) == boost::get<relationship*>(qr2);
                break;
            case int_type: // int
                return boost::get<int>(qr1) == boost::get<int>(qr2);
            case double_type: // double
                return boost::get<double>(qr1) == boost::get<double>(qr2);
            case string_type: // std::string
                return string_equal(qr1, qr2);
            case uint64_type: // uint64_t
                return boost::get<uint64_t>(qr1) == boost::get<uint64_t>(qr2);
            default:
                break;
        }
    } 
    else if (qr1.which() == int_type && qr2.which() == uint64_type)
        return (uint64_t)boost::get<int>(qr1) == boost::get<uint64_t>(qr2);
    else if (qr1.which() == uint64_type && qr2.which() == int_type)
        return boost::get<uint64_t>(qr1) == (uint64_t)boost::get<int>(qr2);

    return false;
}

bool less_than(const query_result& qr1, const query_result& qr2) {
    if (qr1.which() == qr2.which()) {
        switch (qr1.which()) {
            case node_ptr_type: // node *
            case rship_ptr_type: // relationship *
                break;
            case int_type: // int
                return int_less_than(qr1, qr2);
            case double_type: // double
                return double_less_than(qr1, qr2);
            case string_type: // std::string
                return string_less_than(qr1, qr2);
            case uint64_type: // uint64_t
                return uint64_less_than(qr1, qr2);
            default:
                break;
        }
    } 
    else if (qr1.which() == int_type && qr2.which() == uint64_type)
        return (uint64_t)boost::get<int>(qr1) < boost::get<uint64_t>(qr2);
    else if (qr1.which() == uint64_type && qr2.which() ==int_type)
        return boost::get<uint64_t>(qr1) < (uint64_t)boost::get<int>(qr2);
    return false;
}

bool less_or_equal(const query_result& qr1, const query_result& qr2) {
    return less_than(qr1, qr2) || equal(qr1, qr2);
}

bool greater_than(const query_result& qr1, const query_result& qr2) {
    if (qr1.which() == qr2.which()) {
        switch (qr1.which()) {
            case node_ptr_type: // node *
            case rship_ptr_type: // relationship *
                break;
            case int_type: // int
                return int_greater_than(qr1, qr2);
            case double_type: // double
                return double_greater_than(qr1, qr2);
            case string_type: // std::string
                return string_greater_than(qr1, qr2);
            case uint64_type: // uint64_t
                return uint64_greater_than(qr1, qr2);
            default:
                break;
        }
    } 
    else if (qr1.which() == int_type && qr2.which() == uint64_type)
        return (uint64_t)boost::get<int>(qr1) > boost::get<uint64_t>(qr2);
    else if (qr1.which() == uint64_type && qr2.which() == int_type)
        return boost::get<uint64_t>(qr1) > (uint64_t)boost::get<int>(qr2);
    return false;
}

bool greater_or_equal(const query_result& qr1, const query_result& qr2) {
    return greater_than(qr1, qr2) || equal(qr1, qr2);
}

query_result pop(std::stack<query_result>& st) {
    auto v = st.top();
    st.pop();
    return v;
}

struct filter_visitor : public expression_visitor {
public:
    filter_visitor(query_ctx& ctx, const qr_tuple& tup) : ctx_(ctx), tup_(tup) {
        /* 
        auto n = boost::get<node *>(tup[0]);
        std::cout << "visitor:node_id = " << n->id() << std::endl;
        */
     }
    ~filter_visitor() = default;

    bool result() {  
        auto v = pop(stack_); 
        /*
        if (boost::get<int>(v)) {
        auto n = boost::get<node *>(tup_[0]);
            std::cout << "filter_visitor satisfied for = " << n->id() << std::endl;
        }
        */
        return boost::get<int>(v) != 0; 
    }

    virtual void visit(int rank, std::shared_ptr<number_token> op) override {
        // std::cout << "visit number_token: " << op->dump() << std::endl;
        stack_.push(op->ftype_ == FOP_TYPE::INT ? query_result(op->ivalue_) : query_result(op->dvalue_));
    }

    virtual void visit(int rank, std::shared_ptr<key_token> op) override {
        // std::cout << "visit key_token: " << op->qr_id_ << ", " << op->key_ << " : " << tup_.size() << std::endl;
        // TODO: we should replace string key_ by its dcode_t in prepare_expr_visitor
        auto inp = tup_[op->qr_id_];
        p_item res;
        switch (inp.which()) {
            case node_ptr_type: // node *
            {
                auto nptr = boost::get<node *>(inp);
                // if key_ is empty then the node is requested ($i:node)
                if (op->key_.empty()) {
                    // std::cout << "id = " << nptr->id() << std::endl;
                    stack_.push(query_result(nptr));
                }
                else {
                    // otherwise the property value ($i.prop:dtype) which is handled later
                    res = ctx_.gdb_->get_property_value(*nptr, op->key_);
                }
                break;
            }
            case rship_ptr_type: // relationship *
            {
                auto rptr = boost::get<relationship *>(inp);
                // if key_ is empty then the relationship is requested
                if (op->key_.empty())
                    stack_.push(query_result(rptr));
                else
                    // otherwise the property value ($i.prop:dtype) which is handled later
                    res = ctx_.gdb_->get_property_value(*rptr, op->key_);
                break;
            }
            default:
                // std::cout << "visit key_token ==> " << inp.which() << std::endl;
                // Ooops!!
                break;
        }
        switch (res.typecode()) {
            case p_item::p_int:
                stack_.push(query_result(res.get<int>()));
                break;
            case p_item::p_double:
                stack_.push(query_result(res.get<double>()));
                break;
            case p_item::p_uint64:
                stack_.push(query_result(res.get<uint64_t>()));
                break;
            case p_item::p_dcode:
                {
                    auto str = ctx_.gdb_->get_dictionary()->lookup_code(res.get<dcode_t>());
                    stack_.push(query_result(std::string(str)));
                }
                break;
            case p_item::p_unused:
                // node* or relationship*
                break;
            default:
                // spdlog::info("cannot push for #{} : inp={}, res={}", op->qr_id_, inp.which(), res.typecode());
                break;
        }            
        // std::cout << "PUSH: " << res << std::endl;
    }

    virtual void visit(int rank, std::shared_ptr<str_token> op) override {
        stack_.push(query_result(op->str_));
    }

    virtual void visit(int rank, std::shared_ptr<time_token> op) override {
        std::cout << "visit time_token" << std::endl;        
    }

    virtual void visit(int rank, std::shared_ptr<fct_call> op) override {
        std::cout << "visit fct_call" << std::endl;         
    }

    virtual void visit(int rank, std::shared_ptr<func_call> op) override {
        // std::cout << "visit func_call: " << op->func_name_ << " : " << op->param_list_.size() << std::endl;
        if (op->param_list_.size() == 1) {
            auto arg = pop(stack_);
            auto res = op->func1_ptr_(ctx_, arg);
            stack_.push(res);
        }
        else if (op->param_list_.size() == 2) {
            auto arg2 = pop(stack_);
            auto arg1 = pop(stack_);
            auto res = op->func2_ptr_(ctx_, arg1, arg2);
            stack_.push(res);
        }      
    }

    virtual void visit(int rank, std::shared_ptr<eq_predicate> op) override {
        // std::cout << "visit eq_predicate: ==" << std::endl;       
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = equal(v1, v2);
            // std::cout << "visit eq_predicate: ->" << res << std::endl;       
            stack_.push(query_result(res ? 1 : 0));
        }
    }
    
    virtual void visit(int rank, std::shared_ptr<le_predicate> op) override {
        // std::cout << "visit le_predicate: <=" << std::endl;       
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = less_or_equal(v1, v2);
            stack_.push(query_result(res ? 1 : 0));
        }    
    }

    virtual void visit(int rank, std::shared_ptr<lt_predicate> op) override {
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            // std::cout << "visit lt_predicate: <" << v1 << ", " << v2 << std::endl;       
            bool res = less_than(v1, v2);
            stack_.push(query_result(res ? 1 : 0));
        }    
    }

    virtual void visit(int rank, std::shared_ptr<ge_predicate> op) override {
        // std::cout << "visit ge_predicate: >=" << std::endl;              
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = greater_or_equal(v1, v2);
            stack_.push(query_result(res ? 1 : 0));
        }    
    }

    virtual void visit(int rank, std::shared_ptr<gt_predicate> op) override {
        // std::cout << "visit gt_predicate: >" << std::endl; 
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = greater_than(v1, v2);
            stack_.push(query_result(res ? 1 : 0));
        }
    }

    virtual void visit(int rank, std::shared_ptr<and_predicate> op) override {
        if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = boost::get<int>(v1) && boost::get<int>(v2);
            stack_.push(query_result(res ? 1 : 0));
        }
    }

    virtual void visit(int rank, std::shared_ptr<or_predicate> op) override {
         if (valid_operands()) {
            auto v2 = pop(stack_);
            auto v1 = pop(stack_);
            bool res = boost::get<int>(v1) || boost::get<int>(v2);
            stack_.push(query_result(res ? 1 : 0));
        }       
    }

    virtual void visit(int rank, std::shared_ptr<call_predicate> op) override {}

private:
    bool valid_operands() {
        if (stack_.size() < 2) {
            // std::cout << "stack_size = " << stack_.size() << std::endl;
            stack_.pop();
            stack_.push(query_result(0));
            return false; 
        }
        return true;
    }

    query_ctx ctx_;
    qr_tuple tup_;
    std::stack<query_result> stack_;
};

bool interpret_expression(query_ctx& ctx, expr& ex, const qr_tuple& tup) {
    filter_visitor vis(ctx, tup);
    // std::cout << "interpret_expression: " << ex->dump() << std::endl;
    ex->accept(0, vis);
    return vis.result();
}
