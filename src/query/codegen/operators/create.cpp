#include "codegen.hpp"

/**
 * Generates code for the create operation
 */
/*
void codegen_inline_visitor::visit(std::shared_ptr<create_op> op) {
    op->name_ = "";
    BasicBlock *bb;
    bool access = false;
    // if the create is a access path -> init function
    if(!main_function) {
        op->name_ = "query_start";
        access = true;
        Function *df_finish = Function::Create(ctx.finishFctTy, Function::ExternalLinkage, "default_finish_"+qid_,
                                               ctx.getModule());

        BasicBlock *df_finish_bb = BasicBlock::Create(ctx.getContext(), "entry", df_finish);
        ctx.getBuilder().SetInsertPoint(df_finish_bb);
        ctx.getBuilder().CreateRetVoid();

        main_function = Function::Create(ctx.startFctTy, Function::ExternalLinkage, op->name_, ctx.getModule());
        bb = BasicBlock::Create(ctx.getModule().getContext(), "entry", main_function);
        main_return = BasicBlock::Create(ctx.getModule().getContext(), "end", main_function);
        init_function(bb);
    } else { // else link with previous operator
        bb = BasicBlock::Create(ctx.getModule().getContext(), "entry_create", main_function);
        ctx.getBuilder().SetInsertPoint(prev_bb);
        ctx.getBuilder().CreateBr(bb);
        ctx.getBuilder().SetInsertPoint(bb);

    }
    auto consume = BasicBlock::Create(ctx.getModule().getContext(), "entry", main_function);

    gdb = main_function->args().begin();
    ty = main_function->args().begin() + 5;
    rs = main_function->args().begin() + 6;
    finish = main_function->args().begin() + 8;
    offset = main_function->args().begin() + 9;
    queryArgs = main_function->args().begin() + 10;

    //create node by calling external function
    if(op->produced_type_ == 0) {
        auto create_node = ctx.extern_func("create_node");
        auto opid = ConstantInt::get(ctx.int64Ty, op->op_id_);
        auto sec_opid = ConstantInt::get(ctx.int64Ty, op->op_id_+1);
        auto label = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, opid})), ctx.int8PtrTy);
        auto prop_ptr = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, sec_opid}));
        auto node = ctx.getBuilder().CreateCall(create_node, {gdb, label, prop_ptr});
        reg_query_results.push_back({node, 0});
        ctx.getBuilder().CreateBr(consume);
    } else { // create rship
        auto create_rship = ctx.extern_func("create_rship");
        auto opid = ConstantInt::get(ctx.int64Ty, op->op_id_);
        auto sec_opid = ConstantInt::get(ctx.int64Ty, op->op_id_+1);
        auto label = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, opid})), ctx.int8PtrTy);
        auto prop_ptr = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, sec_opid}));

        auto n1 = reg_query_results[op->src_des_.first].reg_val;
        auto n2 = reg_query_results[op->src_des_.second].reg_val;

        auto rship = ctx.getBuilder().CreateCall(create_rship, {gdb, label, n1, n2, prop_ptr});
        reg_query_results.push_back({rship, 1});

        ctx.getBuilder().CreateBr(consume);
    }


    ctx.getBuilder().SetInsertPoint(consume);
    prev_bb = consume;

    if(access) {
        ctx.getBuilder().SetInsertPoint(main_return);
        ctx.getBuilder().CreateRetVoid();
    }
}*/