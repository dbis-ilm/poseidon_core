#ifndef func_call_expr_hpp_
#define func_call_expr_hpp_

#include "expression.hpp"
#include "qresult_iterator.hpp"
#include "query_ctx.hpp"

struct func_call : public expression, std::enable_shared_from_this<func_call> {
    std::string func_name_;
    std::vector<expr> param_list_;
    std::function<query_result(query_ctx&, query_result&)> func1_ptr_;
    std::function<query_result(query_ctx&, query_result&, query_result&)> func2_ptr_;

    func_call(const std::string& fn, const std::vector<expr>& pl) : func_name_(fn), param_list_(pl), func1_ptr_(nullptr), func2_ptr_(nullptr) {}

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr Fct(const std::string& fname, const std::vector<expr>& params) { 
    return std::make_shared<func_call>(fname, params); 
}

#endif