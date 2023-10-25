#include "codegen.hpp"

/**
 * Generates code for the collection
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<collect_result> op) {
    cur_size = op->operator_id_;
    
    // obtain all relevant FunctionCallees
    auto mat_reg = ctx.extern_func("mat_reg_value");
    auto collect_regs = ctx.extern_func("collect_tuple");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");
    auto endnotify = ctx.extern_func("end_notify");
    auto atp = ctx.extern_func("append_to_tuple");

    Value *gdb_ptr;
    Value *rs_arg;
    Function *collect_fct;

    if(pipelined_finish) {
        collect_fct = main_finish;
        //rs_arg = main_finish->args().begin()+2;
    } else {
        collect_fct = main_function;
        gdb_ptr = gdb;
    }
    
    // link with previous operator
    BasicBlock *entry = BasicBlock::Create(ctx.getContext(), "collect_entry", collect_fct);
    pre_tuple_mat = BasicBlock::Create(ctx.getModule().getContext(), "pre_tuple_mat", collect_fct);
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(entry);
    ctx.getBuilder().SetInsertPoint(entry);

    // extract result_set from args
    auto args = collect_fct->args().begin()+1;
    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    rs_arg = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(args, {ctx.LLVM_ZERO, opid}));

    if(pipelined_finish) {
        gdb_ptr = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(collect_fct->args().begin(), 0));
    }

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    auto print_on_collect = 0;
    auto print_tuple = ConstantInt::get(ctx.boolTy, print_on_collect);

    // create a single materialization call for each register value
    // each register will be materialized into thread local storage
    for(auto & res : reg_query_results) {
        // value type 7 => boolean, already transformed into integer
        if(res.type == 10) {
            ctx.getBuilder().CreateCall(atp, {res.reg_val});
        } else {
            auto type_id = (res.type == 7 ? 2 : res.type);
            auto type = ConstantInt::get(ctx.int64Ty, type_id);
            auto cv_reg = ctx.getBuilder().CreateBitCast(res.reg_val, ctx.int64PtrTy);
            ctx.getBuilder().CreateCall(mat_reg, {gdb_ptr, cv_reg, type});
        }
    }

    ctx.getBuilder().CreateBr(pre_tuple_mat);

    ctx.getBuilder().SetInsertPoint(pre_tuple_mat);
    // materialize the complete thread local storage into a global list
    ctx.getBuilder().CreateCall(collect_regs, {gdb_ptr, rs_arg, print_tuple});

    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }
    
    ctx.getBuilder().CreateBr(main_return);

    // complete the finish call
    ctx.getBuilder().SetInsertPoint(df_finish_bb);
    //ctx.getBuilder().CreateCall(endnotify, {rs});
    ctx.getBuilder().CreateRetVoid();

    // complete init->entry
    //if(!pipelined) {    
    //    ctx.getBuilder().SetInsertPoint(inits);
    //    ctx.getBuilder().CreateBr(this->entry);}
}