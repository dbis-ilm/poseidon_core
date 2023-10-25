#include "codegen.hpp"

void codegen_inline_visitor::visit(std::shared_ptr<left_outer_join_op> op) {

}

#if 0
void codegen_inline_visitor::visit(std::shared_ptr<left_outerjoin_on_node> op) {
    pipelined = true;
    jids.push_back(query_id_inline);
    
    std::vector<int> types;
    get_rhs_type(op->get_rhs(), types);

    BasicBlock *join_lhs_entry = BasicBlock::Create(ctx.getContext(), "entry_join_lhs", main_function);
    BasicBlock *left_outer = BasicBlock::Create(ctx.getContext(), "left_outer", main_function);
    BasicBlock *nested_loop = BasicBlock::Create(ctx.getContext(), "nested_loop", main_function);
    BasicBlock *nested_joinable = BasicBlock::Create(ctx.getContext(), "nested_loop_joinable", main_function);

    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "consume", main_function);
    BasicBlock *end = BasicBlock::Create(ctx.getContext(), "end", main_function);
    BasicBlock *hash_join = BasicBlock::Create(ctx.getContext(), "hash_join_probe", main_function);
    BasicBlock *hj_body = BasicBlock::Create(ctx.getContext(), "hj_body", main_function);
    BasicBlock *hj_joinable = BasicBlock::Create(ctx.getContext(), "hj_joinable", main_function);
    BasicBlock *for_each_next = BasicBlock::Create(ctx.getContext(), "for_each_next_rship", main_function);
    BasicBlock *concat_qrl = BasicBlock::Create(ctx.getContext(), "concat_qrl", main_function);
    BasicBlock *incr_loop = BasicBlock::Create(ctx.getContext(), "incr_loop", main_function);
    BasicBlock *undang = BasicBlock::Create(ctx.getContext(), "undang", main_function);
    BasicBlock *undang_collect = BasicBlock::Create(ctx.getContext(), "undang_collect", main_function);
    BasicBlock *return_handle = BasicBlock::Create(ctx.getContext(), "handle_ret", main_function);
    BasicBlock *pre_loop = BasicBlock::Create(ctx.getContext(), "pre_loop", main_function);
    BasicBlock *pre_loop_init = BasicBlock::Create(ctx.getContext(), "pre_loop_init", main_function);
    
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

    dangling = insert_alloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ONE, dangling);
    cur_idx = insert_alloca(ctx.int64Ty);
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

    // iterate through the join list
    auto loop_body = ctx.while_loop_condition(main_function, cur_idx, max_idx, PContext::WHILE_COND::LT, end,
        [&](BasicBlock *body, BasicBlock *epilog) {
            // get current index for join vector

            auto idx = ctx.getBuilder().CreateLoad(cur_idx);
            tp = ctx.getBuilder().CreateCall(get_join_tp_at, {joiner_arg, jid, idx});

            // process left join
            ctx.getBuilder().CreateBr(left_outer);
    });
    
    ctx.getBuilder().SetInsertPoint(left_outer);
    
    
    // get lhs tuple at id -> direct register value
    auto lhs = reg_query_results.at(op->left_right_nodes_.first).reg_val;

    // rhs is materialized, call extern function to obtain tuple at idx
    auto idx = ConstantInt::get(ctx.int64Ty, op->left_right_nodes_.second);
    auto rhs = ctx.getBuilder().CreateCall(node_reg, {tp, idx});

    // get the appropriate fields to compare, rship ids
    auto rhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rhs, 1));
    auto node_rship_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 2));

    // create space for storage
    auto ra = insert_alloca(ctx.rshipPtrTy);
    auto ridx = insert_alloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(node_rship_id, ridx);       
    
    //rhs_alloca = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.UNKNOWN_ID, rhs_alloca);
    
    // iterate through relationship list of lhs node
    auto loop_rship = ctx.while_loop_condition(main_function, ridx, rhs_alloca, 
        PContext::WHILE_COND::UEQ, incr_loop,
            [&](BasicBlock *, BasicBlock *) {
                auto cur_id = ctx.getBuilder().CreateLoad(ridx);

                // get the current rship
                auto rship = ctx.getBuilder().CreateCall(rship_by_id, {gdb, cur_id});
                ctx.getBuilder().CreateStore(rship, ra);
                
                //ctx.getBuilder().CreateStore(ctx.getBuilder().CreateLoad(rship), rship_join);
                //reg_query_results.push_back({rship_join, 1});

                // get the dest node of the rship
                auto to_node = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rship, 3));
                
                // check if the dest node is the rhs node
                auto concat_cond = ctx.getBuilder().CreateICmpEQ(rhs_id, to_node);

                // handle dangling node
                ctx.getBuilder().CreateCondBr(concat_cond, undang, for_each_next);
            });
    

    // get next rship from list
    ctx.getBuilder().SetInsertPoint(for_each_next);
    {
        auto rship = ctx.getBuilder().CreateLoad(ra);
        auto next_rship = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rship, 5));
        ctx.getBuilder().CreateStore(next_rship, ridx);
        ctx.getBuilder().CreateBr(loop_rship);
    }
    
    // handle danling flag
    ctx.getBuilder().SetInsertPoint(undang);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, dangling);
    ctx.getBuilder().CreateBr(concat_qrl);
    
    // handling backflow
    ctx.getBuilder().SetInsertPoint(return_handle);
    auto lh = ctx.getBuilder().CreateLoad(ridx);
    auto is_reached = ctx.getBuilder().CreateICmpEQ(lh, ctx.UNKNOWN_ID);
    ctx.getBuilder().CreateCondBr(is_reached, loop_body, for_each_next);

    ctx.getBuilder().SetInsertPoint(incr_loop);
    {
        auto i = ctx.getBuilder().CreateLoad(cur_idx);
        ctx.getBuilder().CreateStore(ctx.getBuilder().CreateAdd(i, ctx.LLVM_ONE), cur_idx);
        ctx.getBuilder().CreateBr(loop_body);
    }

    // merge the lhs and rhs    
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

    reg_query_results.push_back({dangling, 7});
    

    ctx.getBuilder().CreateBr(consume);
//    ctx.getBuilder().SetInsertPoint(consume);

    ctx.getBuilder().SetInsertPoint(end);
    //auto dang = ctx.getBuilder().CreateLoad(dangling);
    //auto is_dangling = ctx.getBuilder().CreateICmpEQ(dang, ctx.LLVM_ONE);
    //ctx.getBuilder().CreateCondBr(is_dangling, concat_qrl, main_return);
    ctx.getBuilder().CreateBr(main_return);

    main_return = return_handle;
    prev_bb = consume;
}
#endif

