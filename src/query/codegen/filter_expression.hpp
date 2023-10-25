#ifndef ART_FILTER_EXPRESSION_INLINE_HPP
#define ART_FILTER_EXPRESSION_INLINE_HPP

#include <string>
#include <memory>
#include <iostream>
#include "jit/p_context.hpp"
#include "filter_visitor.hpp"
#include "jit/p_context.hpp"
#include "expression.hpp"
#include "binary_expression.hpp"
#include "codegen.hpp"

/**
 * The visitor structure for the filter expression code generation
 */
struct fep_visitor_inline : public expression_visitor {

    // the actual tuple for evaluation
    //Value *roi_;
    std::vector<codegen_inline_visitor::QR_VALUE> &qr_regs_;

    // the type of the tuple
    Value *type_;

    // a filter operator counter used to identify the operator at evaluation
    int opd_cnt = 0;

    // register values for the current pitem 
    Value *cur_item_arr;
    Value *cur_item;
    Value *cur_pset;

    Value *arg_;

    // evaluation values
    Value *unknown_id_;
    Value *plist_id_;
    Value *loop_cnt_;
    Value *max_cnt_;
    Value *pitem_;

    fep_visitor_inline(PContext *ctx, Function *parent, std::vector<codegen_inline_visitor::QR_VALUE> &qr, BasicBlock *next, BasicBlock *end, Value *item_arr, Value *item, Value *pset,
            Value *unknown_id, Value *plist_id, Value *loop_cnt, Value *max_cnt, Value *pitem,
            Value *arg);

    void visit(int rank, std::shared_ptr<number_token> num) override;

    void visit(int rank, std::shared_ptr<key_token> key) override;

    void visit(int rank, std::shared_ptr<str_token> str) override;

    void visit(int rank, std::shared_ptr<time_token> time) override;

    void visit(int rank, std::shared_ptr<fct_call> fct) override;

    void visit(int rank, std::shared_ptr<func_call> fct) override;

   void visit(int rank, std::shared_ptr<eq_predicate> eq) override;

    void visit(int rank, std::shared_ptr<le_predicate> eq) override;

    void visit(int rank, std::shared_ptr<lt_predicate> eq) override;

    void visit(int rank, std::shared_ptr<ge_predicate> eq) override;

    void visit(int rank, std::shared_ptr<gt_predicate> eq) override;

    void visit(int rank, std::shared_ptr<and_predicate> andpr) override;

    void visit(int rank, std::shared_ptr<or_predicate> orpr) override;

    void visit(int rank, std::shared_ptr<call_predicate> call) override;

    Value *alloc(std::string name, Type *type, Value *val = nullptr);

    BasicBlock *add_bb(std::string name);

    PContext *ctx_;
    Function *fct_;

    Value *qr_node_;
    BasicBlock *next_;
    BasicBlock *end_;
    std::vector<expr> expr_stack;

    std::map<std::string, Value *> alloc_stack;
    std::map<int, Value *> gen_vals_;
    std::map<std::string, BasicBlock *> bbs_;

    std::vector<AllocaInst *> keys;

    struct filter_callee {
        Value* callee_;
        FunctionType* type_;
    };

    std::map<int, filter_callee> fct_callees_;

};


#endif //ART_FILTER_EXPRESSION_HPP
