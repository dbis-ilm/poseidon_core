#include "codegen.hpp"

/**
 * Generate code for the join operator
 */
/*
void codegen_inline_visitor::visit(std::shared_ptr<join_op> op) {
    pipelined = true;
    jids.push_back(query_id_inline);
    
    // obtain types of rhs
    std::vector<int> types;
    get_rhs_type(op->inputs_[1], types);

    op->name_ = "";

    // create all basic blocks for the join processing
    BasicBlock *join_lhs_entry = BasicBlock::Create(ctx.getContext(), "entry_join_lhs", main_function);
    BasicBlock *left_outer = BasicBlock::Create(ctx.getContext(), "left_outer", main_function);
    BasicBlock *nested_loop = BasicBlock::Create(ctx.getContext(), "nested_loop", main_function);
    BasicBlock *nested_joinable = BasicBlock::Create(ctx.getContext(), "nested_loop_joinable", main_function);

    BasicBlock *hash_join = BasicBlock::Create(ctx.getContext(), "hash_join_probe", main_function);
    BasicBlock *hj_body = BasicBlock::Create(ctx.getContext(), "hj_body", main_function);
    BasicBlock *hj_joinable = BasicBlock::Create(ctx.getContext(), "hj_joinable", main_function);
    BasicBlock *for_each_next = BasicBlock::Create(ctx.getContext(), "for_each_next_rship", main_function);
    BasicBlock *concat_qrl = BasicBlock::Create(ctx.getContext(), "concat_qrl", main_function);
    BasicBlock *incr_loop = BasicBlock::Create(ctx.getContext(), "incr_loop", main_function);
    BasicBlock *undang = BasicBlock::Create(ctx.getContext(), "undang", main_function);
    BasicBlock *undang_collect = BasicBlock::Create(ctx.getContext(), "undang_collect", main_function);
    BasicBlock *return_handle = BasicBlock::Create(ctx.getContext(), "handle_ret", main_function);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "consume", main_function);
    BasicBlock *pre_loop = BasicBlock::Create(ctx.getContext(), "pre_loop", main_function);
    BasicBlock *pre_loop_init = BasicBlock::Create(ctx.getContext(), "pre_loop_init", main_function);
    BasicBlock *end = BasicBlock::Create(ctx.getContext(), "end", main_function);


    // obtain all FunctionCallees
    auto get_join_tp_at = ctx.extern_func("get_join_tp_at");
    auto node_reg = ctx.extern_func("get_node_res_at");
    auto rship_reg = ctx.extern_func("get_rship_res_at");
    auto rship_by_id = ctx.extern_func("rship_by_id");
    auto get_size = ctx.extern_func("get_mat_res_size");
    auto insert_join_id_input = ctx.extern_func("insert_join_id_input");
    auto get_join_id_at = ctx.extern_func("get_join_id_at");
    auto print_int = ctx.extern_func("print_int");

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    auto int_to_reg = ctx.extern_func("int_to_reg");

    // link with previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(join_lhs_entry);
    ctx.getBuilder().SetInsertPoint(join_lhs_entry);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});
    cur_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);

    auto opid = ConstantInt::get(ctx.int64Ty, op->op_id_);
    auto qctx_f = main_function->args().begin() + 1;

    auto joiner_arg = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    // init idx and dangling flag
    ctx.getBuilder().CreateStore(ctx.LLVM_ONE, dangling);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, cur_idx);

    Value *tp = nullptr;
    
    // get the join identifier and the appropriate rhs tuple list
    auto jid = ConstantInt::get(ctx.int64Ty, query_id_inline);
    auto ma = ctx.getBuilder().CreateCall(get_size, {joiner_arg, jid});
    ctx.getBuilder().CreateStore(ma, max_idx);
    query_id_inline++;

    //Value *rhs_id;
    //auto rhs_id_alloc = insert_alloca(ctx.int64Ty);
    auto rhs_id_alloc = insert_alloca(ctx.int64Ty);
    auto rship_join = insert_alloca(ctx.rshipTy);
    
    BasicBlock *loop_body = nullptr;
    
    if(op->jop_ == JOIN_OP::NESTED_LOOP) {
        // outer loop
            loop_body = ctx.while_loop_condition(main_function, cur_idx, max_idx, PContext::WHILE_COND::LT, end,
                                            [&](BasicBlock *body, BasicBlock *epilog) {
                                                auto pos = ctx.getBuilder().CreateLoad(cur_idx);
                                                auto rhs_id = ctx.getBuilder().CreateCall(get_join_id_at, {joiner_arg, jid, pos});
                                                
                                                tp = ctx.getBuilder().CreateCall(get_join_tp_at, {joiner_arg, jid, pos});
                                                ctx.getBuilder().CreateStore(rhs_id, rhs_id_alloc);
                                                ctx.getBuilder().CreateBr(nested_loop); 
                                            });  
    } else if(op->jop_ == JOIN_OP::HASH_JOIN) {
        ctx.getBuilder().CreateBr(hash_join);
    } else {
        // iterate through the join list
        loop_body = ctx.while_loop_condition(main_function, cur_idx, max_idx, PContext::WHILE_COND::LT, end,
                                                [&](BasicBlock *body, BasicBlock *epilog) {
                                                    // get current index for join vector

                                                    auto idx = ctx.getBuilder().CreateLoad(cur_idx);
                                                    tp = ctx.getBuilder().CreateCall(get_join_tp_at, {joiner_arg, jid, idx});

                                                    if(op->jop_ == JOIN_OP::CROSS) {
                                                        // process cross join
                                                        ctx.getBuilder().CreateBr(concat_qrl);
                                                    } else if(op->jop_ == JOIN_OP::LEFT_OUTER) {
                                                        // process left join
                                                        ctx.getBuilder().CreateBr(left_outer);
                                                    }
                                                });        
    }

    BasicBlock *loop_rship = nullptr;
    if(op->jop_ == JOIN_OP::LEFT_OUTER) {
        // for left outer join
        ctx.getBuilder().SetInsertPoint(left_outer);

        // get lhs tuple at id -> direct register value
        auto lhs = reg_query_results.at(op->join_pos_.first).reg_val;

        // rhs is materialized, call extern function to obtain tuple at idx
        auto idx = ConstantInt::get(ctx.int64Ty, op->join_pos_.second);
        auto rhs = ctx.getBuilder().CreateCall(node_reg, {tp, idx});

        // get the appropriate fields to compare, rship ids
        auto rhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rhs, 1));
        auto node_rship_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 2));

        ctx.getBuilder().CreateStore(node_rship_id, cross_join_idx.back());

        // create space for storage
        auto ra = insert_alloca(ctx.rshipPtrTy);
        auto ridx = insert_alloca(ctx.int64Ty);
        ctx.getBuilder().CreateStore(node_rship_id, ridx);       
        
        // iterate through relationship list of lhs node
        loop_rship = ctx.while_loop_condition(main_function, ridx, rhs_alloca, PContext::WHILE_COND::UEQ, incr_loop,
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

        ctx.getBuilder().SetInsertPoint(pre_loop);
        auto pos = ctx.getBuilder().CreateAdd(ctx.getBuilder().CreateLoad(cur_idx), ctx.LLVM_ONE);
        auto max = ctx.getBuilder().CreateLoad(max_idx);
        auto reached_loop_end = ctx.getBuilder().CreateICmpSLT(pos, max);
        ctx.getBuilder().CreateCondBr(reached_loop_end, pre_loop_init, loop_body);

        ctx.getBuilder().SetInsertPoint(pre_loop_init);
        ctx.getBuilder().CreateStore(ctx.LLVM_ONE, dangling);
        ctx.getBuilder().CreateBr(loop_body);

    } else if(op->jop_ == JOIN_OP::NESTED_LOOP) {
        // for nested loop join
        ctx.getBuilder().SetInsertPoint(nested_loop);
        
        // get lhs tuple at id -> direct register value
        auto lhs = reg_query_results.at(op->join_pos_.first).reg_val;
        // extract the lhs id with GEP instruction
        auto lhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 1));
        
        auto rhs_id = ctx.getBuilder().CreateLoad(rhs_id_alloc);
        auto id_eq = ctx.getBuilder().CreateICmpEQ(lhs_id, rhs_id);
        ctx.getBuilder().CreateCondBr(id_eq, nested_joinable, incr_loop);
        
        ctx.getBuilder().SetInsertPoint(nested_joinable);
        ctx.getBuilder().CreateBr(concat_qrl);

    } else if(op->jop_ == JOIN_OP::HASH_JOIN) {
        ctx.getBuilder().SetInsertPoint(hash_join);
        
        // get lhs tuple at id -> direct register value
        auto lhs = reg_query_results.at(op->join_pos_.first).reg_val;
        // extract the lhs id with GEP instruction
        auto lhs_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(lhs, 1));

        // calculate the bucket id 
        auto bucket_size = ConstantInt::get(ctx.int64Ty, 10);
        auto bucket_id = ctx.getBuilder().CreateSRem(lhs_id, bucket_size);

        auto get_hj_input_size = ctx.extern_func("get_hj_input_size");
        auto get_hj_input_id = ctx.extern_func("get_hj_input_id");
        auto get_query_result = ctx.extern_func("get_query_result");

        loop_body = BasicBlock::Create(ctx.getContext(), "hj_head", main_function);

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
    }

    // increment the outer loop counter for next query result
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

    // add the dangling flag to the tuple result
    if(op->jop_ == JOIN_OP::LEFT_OUTER)
        reg_query_results.push_back({dangling, 7});

    if(op->jop_ == JOIN_OP::CROSS || op->jop_ == JOIN_OP::NESTED_LOOP) {
        auto i = ctx.getBuilder().CreateLoad(cur_idx);
        auto i_add = ctx.getBuilder().CreateAdd(i, ctx.LLVM_ONE);
        ctx.getBuilder().CreateStore(i_add, cur_idx);
    }

    ctx.getBuilder().CreateBr(consume);

    ctx.getBuilder().SetInsertPoint(consume);

    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->op_id_), t_start, t_end});
    }
    prev_bb = consume;

    ctx.getBuilder().SetInsertPoint(end);
    {
        if(op->jop_ == JOIN_OP::LEFT_OUTER) {
            auto dang = ctx.getBuilder().CreateLoad(dangling);
            auto is_dangling = ctx.getBuilder().CreateICmpEQ(dang, ctx.LLVM_ONE);

            ctx.getBuilder().CreateCondBr(is_dangling, undang_collect, loop_body);

            ctx.getBuilder().SetInsertPoint(undang_collect);
            //ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, dangling);
            //ctx.getBuilder().CreateStore(Constant::getNullValue(ctx.rshipTy), rship_join);
            ctx.getBuilder().CreateBr(concat_qrl);
        } else {
            ctx.getBuilder().CreateBr(main_return);
        }
    }

    if(op->jop_ == JOIN_OP::LEFT_OUTER)
        main_return = return_handle;
    else 
        main_return = loop_body;
}*/

/**
 * Generates code for the materialization of the rhs of the join
 */
void codegen_inline_visitor::visit(std::shared_ptr<end_pipeline> op) {
    //auto join_insert_left = ctx.extern_func("join_insert_left");

    auto obtain_mat_tuple = ctx.extern_func("obtain_mat_tuple");
    auto mat_node = ctx.extern_func("mat_node");
    auto mat_rship = ctx.extern_func("mat_rship");
    auto collect_tuple_join = ctx.extern_func("collect_tuple_join");
    auto collect_tuple_hash_join = ctx.extern_func("collect_tuple_hash_join");
    auto insert_join_id_input = ctx.extern_func("insert_join_id_input");
    auto insert_join_bucket_input = ctx.extern_func("insert_join_bucket_input");

    auto mat_reg = ctx.extern_func("mat_reg_value");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    BasicBlock *bb = BasicBlock::Create(ctx.getContext(), "entry_join_rhs", main_function);

    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(bb);
    ctx.getBuilder().SetInsertPoint(bb);

    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto qctx_f = main_function->args().begin() + 1;

    auto joiner_arg = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    // collect materialized tuple -> joiner rhs_input @ join id
    auto jid = ConstantInt::get(ctx.int64Ty, 0);
    
    Value *remainder = nullptr;
/*

    if(op->join_op_ == JOIN_OP::NESTED_LOOP) {
        auto n = reg_query_results[op->qr_pos_].reg_val;
        auto id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(n, 1));
        ctx.getBuilder().CreateCall(insert_join_id_input, {joiner_arg, jid, id});
    } else if(op->join_op_ == JOIN_OP::HASH_JOIN) {
        auto n = reg_query_results[op->qr_pos_].reg_val;
        auto id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(n, 1));
        auto num_bucket = ConstantInt::get(ctx.int64Ty, 10);
        remainder = ctx.getBuilder().CreateSRem(id, num_bucket);
        ctx.getBuilder().CreateCall(insert_join_bucket_input, {joiner_arg, jid, remainder, id});
    }
*/
    // obtain tuple object for materialization -> joiner mat_tuple
    auto tp = ctx.getBuilder().CreateCall(obtain_mat_tuple, {});


    // materialize each result from registers
    for(auto & res : reg_query_results) {
        if(res.type == 0) {
            ctx.getBuilder().CreateCall(mat_node, {res.reg_val, tp});
        } else if(res.type == 1) {
            ctx.getBuilder().CreateCall(mat_rship, {res.reg_val, tp});
        } else if(res.type == 2) {
            auto cv_reg = ctx.getBuilder().CreateBitCast(res.reg_val, ctx.int64PtrTy);
            ctx.getBuilder().CreateCall(mat_reg, {gdb, cv_reg, ConstantInt::get(ctx.int64Ty, 92)});
        } else if(res.type == 3) {
            auto cv_reg = ctx.getBuilder().CreateBitCast(res.reg_val, ctx.int64PtrTy);
            ctx.getBuilder().CreateCall(mat_reg, {gdb, cv_reg, ConstantInt::get(ctx.int64Ty, 93)});
        }
    }

 //   if(op->join_op_ == JOIN_OP::HASH_JOIN) {
 //       ctx.getBuilder().CreateCall(collect_tuple_hash_join, {joiner_arg, jid, remainder, tp});
 //   } else {
    ctx.getBuilder().CreateCall(collect_tuple_join, {joiner_arg, jid, tp});
 //   }
    

    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }

    ctx.getBuilder().CreateBr(main_return);

    ctx.getBuilder().SetInsertPoint(df_finish_bb);
    ctx.getBuilder().CreateRetVoid();
}