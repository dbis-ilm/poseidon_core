#include "codegen.hpp"

/**
 * Generate code for the sort operator
 * For now it execute the given sorting predicate
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<order_by> op) {
    Function *df_finish = Function::Create(ctx.finishFctTy, Function::ExternalLinkage, "order_by",
                                           ctx.getModule());

    BasicBlock *sort_entry = BasicBlock::Create(ctx.getContext(), "entry", main_finish);

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");
    ctx.getBuilder().SetInsertPoint(df_finish_bb);
    ctx.getBuilder().CreateBr(sort_entry);
    ctx.getBuilder().SetInsertPoint(sort_entry);

    
    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    auto res = df_finish->args().begin();

    auto sort_fc_raw = ConstantInt::get(ctx.int64Ty, (int64_t)op->sort);
    auto sort_fc_ptr = ctx.getBuilder().CreateIntToPtr(sort_fc_raw, ctx.int64PtrTy);
    auto sort_fc = ctx.getBuilder().CreateBitCast(sort_fc_ptr, FunctionType::get(ctx.voidTy, {ctx.int64PtrTy}, false)->getPointerTo());
    
    auto args = main_finish->args().begin()+1;
    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto rs_arg = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(args, {ctx.LLVM_ZERO, opid}));
    
    auto fct_ty = FunctionType::get(ctx.voidTy, {ctx.int64PtrTy}, false);

    ctx.getBuilder().CreateCall(fct_ty, sort_fc, {rs_arg});
    /*
    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }*/
    df_finish_bb = sort_entry;
    //ctx.getBuilder().CreateRetVoid();
}
