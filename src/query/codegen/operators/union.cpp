#include "codegen.hpp"

void codegen_inline_visitor::visit(std::shared_ptr<union_all_op> op) {
    pipelined = true;
    jids.push_back(query_id_inline);
    
    BasicBlock *entry = BasicBlock::Create(ctx.getContext(), "entry_union", main_function);
    BasicBlock *end_rhs = BasicBlock::Create(ctx.getContext(), "end_rhs", main_function);
    BasicBlock *demat = BasicBlock::Create(ctx.getContext(), "demat", main_function);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "consume", main_function);

    auto get_join_tp_at = ctx.extern_func("get_join_tp_at");
    auto node_reg = ctx.extern_func("get_node_res_at");
    auto rship_reg = ctx.extern_func("get_rship_res_at");
    auto get_size = ctx.extern_func("get_mat_res_size");
    auto int_to_reg = ctx.extern_func("int_to_reg");

    // link with previous operator
    link_operator(entry);

    // push rhs
    // loop through rhs list
    auto cur_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, cur_idx);

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

    auto rhs_finished = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, rhs_finished);

    auto loop_body = ctx.while_loop_condition(main_function, cur_idx, max_idx, PContext::WHILE_COND::LT, end_rhs,
                                            [&](BasicBlock *body, BasicBlock *epilog) {
                                                // get current index for join vector

                                                auto idx = ctx.getBuilder().CreateLoad(cur_idx);
                                                tp = ctx.getBuilder().CreateCall(get_join_tp_at, {joiner_arg, jid, idx});

                                                ctx.getBuilder().CreateBr(demat);
                                            });        

    ctx.getBuilder().SetInsertPoint(end_rhs);
    ctx.getBuilder().CreateStore(ctx.LLVM_ONE, rhs_finished);
    ctx.getBuilder().CreateBr(main_return);

    ctx.getBuilder().SetInsertPoint(demat);
    int i = 0;
    auto lhs_query_results = reg_query_results;
    //reg_query_results.clear(); 
    for(auto & reg : lhs_query_results) {
        break;
        auto idx = ConstantInt::get(ctx.int64Ty, i);
        switch(reg.type) {
            case 0: {
                auto n = ctx.getBuilder().CreateCall(node_reg, {tp, idx});
                reg_query_results.push_back({n, 0});
                break;
            }
            case 1: {
                auto r = ctx.getBuilder().CreateCall(rship_reg, {tp, idx});
                reg_query_results.push_back({r, 1});                
                break;
            }
            case 2: {
                break;
            }
            default: break;
        }
        i++;
    }
    ctx.getBuilder().CreateBr(consume);

    ctx.getBuilder().SetInsertPoint(end_rhs);
    // push lhs

//    ctx.getBuilder().SetInsertPoint(end);
//    ctx.getBuilder().CreateBr(main_return);

//    ctx.getBuilder().SetInsertPoint(consume);
    
    prev_bb = consume;
    main_return = entry;
}