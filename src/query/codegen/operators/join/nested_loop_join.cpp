#include "codegen.hpp"

void codegen_inline_visitor::visit(std::shared_ptr<nested_loop_join_op> op) {
    pipelined = true;
    jids.push_back(query_id_inline);
    
    std::vector<int> types;
    get_rhs_type(op->get_rhs(), types);

    BasicBlock *join_lhs_entry = BasicBlock::Create(ctx.getContext(), "entry_join_lhs", main_function);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "consume", main_function);
    BasicBlock *concat_qrl = BasicBlock::Create(ctx.getContext(), "concat_qrl", main_function);
    BasicBlock *end = BasicBlock::Create(ctx.getContext(), "end", main_function);

    BasicBlock *nested_loop = BasicBlock::Create(ctx.getContext(), "nested_loop", main_function);
    BasicBlock *nested_joinable = BasicBlock::Create(ctx.getContext(), "nested_loop_joinable", main_function);
    BasicBlock *incr_loop = BasicBlock::Create(ctx.getContext(), "incr_loop", main_function);
    

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
    link_operator(join_lhs_entry);

    auto cur_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ConstantInt::get(ctx.int64Ty, 0), cur_idx);

    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto qctx_f = main_function->args().begin() + 1;

    auto joiner_arg = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    Value *tp;
    auto jid = ConstantInt::get(ctx.int64Ty, query_id_inline);
    auto ma = ctx.getBuilder().CreateCall(get_size, {joiner_arg, jid});
    ctx.getBuilder().CreateStore(ma, max_idx);
    query_id_inline++;

    auto rhs_id_alloc = insert_alloca(ctx.int64Ty);

    auto loop_body = ctx.while_loop_condition(main_function, cur_idx, max_idx, PContext::WHILE_COND::LT, end,
                                    [&](BasicBlock *body, BasicBlock *epilog) {
                                        auto pos = ctx.getBuilder().CreateLoad(cur_idx);
                                        auto rhs_id = ctx.getBuilder().CreateCall(get_join_id_at, {joiner_arg, jid, pos});
                                        
                                        tp = ctx.getBuilder().CreateCall(get_join_tp_at, {joiner_arg, jid, pos});
                                        ctx.getBuilder().CreateStore(rhs_id, rhs_id_alloc);
                                        ctx.getBuilder().CreateBr(nested_loop); 
                                    }); 

    ctx.getBuilder().SetInsertPoint(nested_loop);
        
    // get lhs tuple at id -> direct register value
    auto lhs = reg_query_results.at(op->left_right_nodes_.first).reg_val;
    // extract the lhs id with GEP instruction
    auto lhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 1));
    
    auto rhs_id = ctx.getBuilder().CreateLoad(rhs_id_alloc);
    auto id_eq = ctx.getBuilder().CreateICmpEQ(lhs_id, rhs_id);
    ctx.getBuilder().CreateCondBr(id_eq, nested_joinable, incr_loop);
    
    ctx.getBuilder().SetInsertPoint(nested_joinable);
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

    auto ip = ctx.getBuilder().CreateLoad(cur_idx);
    auto i_add = ctx.getBuilder().CreateAdd(ip, ctx.LLVM_ONE);
    ctx.getBuilder().CreateStore(i_add, cur_idx);

    ctx.getBuilder().CreateBr(consume);
    
    ctx.getBuilder().SetInsertPoint(incr_loop);
    auto i = ctx.getBuilder().CreateLoad(cur_idx);
    ctx.getBuilder().CreateStore(ctx.getBuilder().CreateAdd(i, ctx.LLVM_ONE), cur_idx);
    ctx.getBuilder().CreateBr(loop_body);

    ctx.getBuilder().SetInsertPoint(consume);

    ctx.getBuilder().SetInsertPoint(end);
    ctx.getBuilder().CreateBr(main_return);
    
    main_return = loop_body;
    prev_bb = consume;
}