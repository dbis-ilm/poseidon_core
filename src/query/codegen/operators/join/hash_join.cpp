#include "codegen.hpp"

void codegen_inline_visitor::visit(std::shared_ptr<hash_join_op> op) {
    pipelined = true;
    jids.push_back(query_id_inline);
    
    std::vector<int> types;
    get_rhs_type(op->get_rhs(), types);

    BasicBlock *join_lhs_entry = BasicBlock::Create(ctx.getContext(), "entry_join_lhs", main_function);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "consume", main_function);
    BasicBlock *concat_qrl = BasicBlock::Create(ctx.getContext(), "concat_qrl", main_function);
    BasicBlock *end = BasicBlock::Create(ctx.getContext(), "end", main_function);
    BasicBlock *hash_join = BasicBlock::Create(ctx.getContext(), "hash_join_probe", main_function);
    BasicBlock *hj_body = BasicBlock::Create(ctx.getContext(), "hj_body", main_function);
    BasicBlock *hj_joinable = BasicBlock::Create(ctx.getContext(), "hj_joinable", main_function);

    auto get_join_tp_at = ctx.extern_func("get_join_tp_at");
    auto node_reg = ctx.extern_func("get_node_res_at");
    auto rship_reg = ctx.extern_func("get_rship_res_at");
    auto rship_by_id = ctx.extern_func("rship_by_id");
    auto get_size = ctx.extern_func("get_mat_res_size");
    auto insert_join_id_input = ctx.extern_func("insert_join_id_input");
    auto get_join_id_at = ctx.extern_func("get_join_id_at");
    auto print_int = ctx.extern_func("print_int");
    auto int_to_reg = ctx.extern_func("int_to_reg");

    // link with previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(join_lhs_entry);
    ctx.getBuilder().SetInsertPoint(join_lhs_entry);

    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto qctx_f = main_function->args().begin() + 1;

    auto joiner_arg = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    Value *tp;
    auto jid = ConstantInt::get(ctx.int64Ty, query_id_inline);

    ctx.getBuilder().CreateBr(hash_join);

    ctx.getBuilder().SetInsertPoint(hash_join);
    
    // get lhs tuple at id -> direct register value
    auto lhs = reg_query_results.at(op->left_right_nodes_.first).reg_val;
    // extract the lhs id with GEP instruction
    auto lhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 1));

    // calculate the bucket id 
    auto bucket_size = ConstantInt::get(ctx.int64Ty, 10);
    auto bucket_id = ctx.getBuilder().CreateSRem(lhs_id, bucket_size);

    auto get_hj_input_size = ctx.extern_func("get_hj_input_size");
    auto get_hj_input_id = ctx.extern_func("get_hj_input_id");
    auto get_query_result = ctx.extern_func("get_query_result");

    auto loop_body = BasicBlock::Create(ctx.getContext(), "hj_head", main_function);

    auto input_size = ctx.getBuilder().CreateCall(get_hj_input_size, {joiner_arg, jid, bucket_id});
    auto cur_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, cur_idx);
    ctx.getBuilder().CreateBr(loop_body);

    ctx.getBuilder().SetInsertPoint(loop_body);
    auto idx = ctx.getBuilder().CreateLoad(cur_idx);
    auto hj_lt = ctx.getBuilder().CreateICmpULT(idx, input_size);
    ctx.getBuilder().CreateCondBr(hj_lt, hj_body, end);

    ctx.getBuilder().SetInsertPoint(hj_body);
    auto id = ctx.getBuilder().CreateCall(get_hj_input_id, {joiner_arg, jid, bucket_id, idx});
    auto incr_hj_id = ctx.getBuilder().CreateAdd(idx, ctx.LLVM_ONE);
    ctx.getBuilder().CreateStore(incr_hj_id, cur_idx);
    auto hj_eq = ctx.getBuilder().CreateICmpEQ(lhs_id, id);
    ctx.getBuilder().CreateCondBr(hj_eq, hj_joinable, loop_body);

    ctx.getBuilder().SetInsertPoint(hj_joinable);
    tp = ctx.getBuilder().CreateCall(get_query_result, {joiner_arg, jid, bucket_id, idx});
    ctx.getBuilder().CreateBr(concat_qrl);

    ctx.getBuilder().SetInsertPoint(concat_qrl);

    for(auto i = 0u; i < types.size(); i++) {
        auto idx = ConstantInt::get(ctx.int64Ty, i);
        if(types.at(i) == 0) {
            auto n = ctx.getBuilder().CreateCall(node_reg, {tp, idx});
            reg_query_results.push_back({n, 0});
        } else if(types.at(i) == 1) {
            auto r = ctx.getBuilder().CreateCall(rship_reg, {tp, idx});
            reg_query_results.push_back({r, 1});
        } else if(types.at(i) == 2) {
            auto i = ctx.getBuilder().CreateCall(int_to_reg, {tp, idx});
            auto i_alloc = insert_alloca(ctx.int64Ty);
            ctx.getBuilder().CreateStore(i, i_alloc);
            reg_query_results.push_back({i_alloc, 2});
        } 
    }

    ctx.getBuilder().CreateBr(consume);

    ctx.getBuilder().SetInsertPoint(end);
    ctx.getBuilder().CreateBr(main_return);

    ctx.getBuilder().SetInsertPoint(consume);
    prev_bb = consume;
    main_return = loop_body;
}