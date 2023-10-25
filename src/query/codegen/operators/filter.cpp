#include "codegen.hpp"
#include "filter_expression.hpp"

/**
 * Generates code for the filter operator
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<filter_tuple> op) {
    // create all relevant basic blocks for the operator processing
    BasicBlock *entry = BasicBlock::Create(ctx.getContext(), "filter_entry", main_function);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "filter_consume", main_function);
    BasicBlock *false_pred = BasicBlock::Create(ctx.getContext(), "filter_false", main_function);

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    // link with the previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(entry);
    ctx.getBuilder().SetInsertPoint(entry);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling)
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    // extract the argument from the argument desc
    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto arg = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, opid})));
    
    // obtain the last query result
    //auto pres = reg_query_results.back().reg_val;
    
    // generate code for each filter expression
    auto vis = fep_visitor_inline(&ctx, main_function, reg_query_results, consume, 
            false_pred, cur_item_arr, cur_item, cur_pset, 
            rhs_alloca, plist_id, 
            loop_cnt, max_cnt, pitem, arg);
    op->ex_->accept(0, vis);


    ctx.getBuilder().SetInsertPoint(consume);
    {
        if(profiling) {
            t_end = ctx.getBuilder().CreateCall(fadd_now, {});
            ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
        }
        // next operator
        prev_bb = consume;
    }

    ctx.getBuilder().SetInsertPoint(false_pred);
    {
        //return to main loop
        ctx.getBuilder().CreateBr(main_return);
    }
}