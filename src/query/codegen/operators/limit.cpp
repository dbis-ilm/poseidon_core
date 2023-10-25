#include "codegen.hpp"

/**
 * Generates code for the limit operator
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<limit_result> op) {
    BasicBlock *limit = BasicBlock::Create(ctx.getContext(), "limit_entry", main_function);
    BasicBlock *limit_reached = BasicBlock::Create(ctx.getContext(), "limit_entry", main_function);
    BasicBlock *limit_cont = BasicBlock::Create(ctx.getContext(), "limit_entry", main_function);

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    // link with previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(limit);
    ctx.getBuilder().SetInsertPoint(limit);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    // get the given limit
    auto res_limit = ConstantInt::get(ctx.int64Ty, op->num_);

    // load the value from the global result counter
    auto cur_cnt = ctx.getBuilder().CreateLoad(res_counter);

    // check if the limit is reached
    auto reached = ctx.getBuilder().CreateICmpEQ(cur_cnt, res_limit);
    ctx.getBuilder().CreateCondBr(reached, limit_reached, limit_cont);

    // if the limit is reached branch to the global end block
    ctx.getBuilder().SetInsertPoint(limit_reached);

    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }

    ctx.getBuilder().CreateBr(scan_nodes_end);


    // increment the global result counter
    ctx.getBuilder().SetInsertPoint(limit_cont);
    auto incr = ctx.getBuilder().CreateAdd(cur_cnt, ctx.LLVM_ONE);
    ctx.getBuilder().CreateStore(incr, res_counter);
    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }
    prev_bb = limit_cont;

}