#include "codegen.hpp"

/**
 * Generates code for the foreach relationship operator.
 */
void codegen_inline_visitor::visit(std::shared_ptr<foreach_relationship> op) {
    // obtain the relationship field ids from the direction
    int node_rship_idx = 2;
    int next_rship_idx = 4;
    switch (op->dir_) {
        case RSHIP_DIR::FROM:
            node_rship_idx = 2;
            next_rship_idx = 4;
            break;
        case RSHIP_DIR::TO:
            node_rship_idx = 3;
            next_rship_idx = 5;
    }

    // obtain the FunctionCallees for the operator processing
    auto rship_by_id = ctx.extern_func("rship_by_id");
    //FunctionCallee gdb_get_rships = ctx.extern_func("gdb_get_rships");
    //FunctionCallee ohop_count = ctx.extern_func("count_potential_o_hop");
    //FunctionCallee get_rship_queue = ctx.extern_func("retrieve_fev_queue");
    //FunctionCallee insert_to_queue = ctx.extern_func("insert_fev_rship");

    auto retrieve_rships = ctx.extern_func("foreach_from_variable_rship");
    auto get_next_rship = ctx.extern_func("get_next_rship_fev");
    auto fev_list_end = ctx.extern_func("fev_list_end");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    // create the basic blocks for the operator processing
    BasicBlock *fe_entry = BasicBlock::Create(ctx.getContext(), "fe_entry", cur_pipeline);
    BasicBlock *foreach_rship_end = BasicBlock::Create(ctx.getModule().getContext(), "foreach_rel_end", cur_pipeline);
    BasicBlock *nextBB = BasicBlock::Create(ctx.getModule().getContext(), "next_rship", cur_pipeline);
    BasicBlock *consumeBB = BasicBlock::Create(ctx.getModule().getContext(), "consume_rship", cur_pipeline);

    // basic blocks for the fev operator
    BasicBlock *loop_head = BasicBlock::Create(ctx.getContext(), "fev_loop_head", cur_pipeline);
    //BasicBlock *loop_body = BasicBlock::Create(ctx.getContext(), "fev_loop_body", cur_pipeline);
    //BasicBlock *loop_next = BasicBlock::Create(ctx.getContext(), "fev_loop_next", main_function);

    // link the entry point with the block of the previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(fe_entry);
    ctx.getBuilder().SetInsertPoint(fe_entry);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    Value *node = nullptr;

    // get the previous tuple result
    if(op->npos == std::numeric_limits<int>::max())
        node = reg_query_results.back().reg_val;
    else
        node = reg_query_results.at(op->npos).reg_val;

    auto UNKNOWN_REL_ID = ConstantInt::get(ctx.int64Ty,
                                           std::numeric_limits<int64_t>::max()); // TODO: UNKNOWN VALUE = std::numeric_limits<int64_t>::max()
    auto nr = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(UNKNOWN_REL_ID, nr);

    GlobalVariable *label = ctx.getBuilder().CreateGlobalString(StringRef(op->label), "rship_label");

    auto strPtr = ctx.getBuilder().CreateBitCast(label, ctx.int8PtrTy);
    auto stra = ctx.getBuilder().CreateAlloca(ctx.int8PtrTy);
    ctx.getBuilder().CreateStore(strPtr, stra);

    //auto label_code = ConstantInt::get(ctx.int32Ty, APInt(32, ctx.get_dcode(op->label_)));

    // extract label from arguments and obtain label code
    auto qctx = cur_pipeline->args().begin();
    auto g = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx, 0));
    auto args = cur_pipeline->args().begin()+1;
    auto label_code = ctx.extract_arg_label(op->operator_id_, g, args);

    // extract the first relationship id from the node
    auto node_rship_id = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(node, node_rship_idx));
    auto lhs = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(node_rship_id, lhs);
    
    // register value for the current relationship
    Value *rship;

    // generate code depending on variable traversion or single
/*    if(op->is_variable()) {
        // for variable traversion
        // transform min and max hop into constants
        auto min_hop = ConstantInt::get(ctx.int64Ty, op->hops_.first);
        auto max_hop = ConstantInt::get(ctx.int64Ty, op->hops_.second);

        // retrieve all relationships and store it into a intermediate vector (externally)
        ctx.getBuilder().CreateCall(retrieve_rships, {gdb, label_code, node, min_hop, max_hop});        

        // simple loop in order to traverse the previous filled vector
        ctx.getBuilder().CreateBr(loop_head);
        // the loop header
        ctx.getBuilder().SetInsertPoint(loop_head);
        // check if the end of the vector is reached (externally) and branch to body or end
        auto is_end = ctx.getBuilder().CreateCall(fev_list_end, {});
        ctx.getBuilder().CreateCondBr(is_end, foreach_rship_end, loop_body);

        // in the body, simply obtain the relationship (externally) and move the address to a register value
        ctx.getBuilder().SetInsertPoint(loop_body);
        rship = ctx.getBuilder().CreateCall(get_next_rship, {});

        // branch to the next operator
        ctx.getBuilder().CreateBr(consumeBB);

    } else {*/
        // iterate through the relationship list of the node
    auto loop_body = ctx.while_loop_condition(cur_pipeline, lhs, nr, PContext::WHILE_COND::LT, foreach_rship_end,
                                            [&](BasicBlock *, BasicBlock *) {
                                                if(profiling)
                                                    t_start = ctx.getBuilder().CreateCall(fadd_now, {});

                                                // extract relationship id from alloca
                                                auto lhsi = ctx.getBuilder().CreateLoad(lhs);

                                                // obtain the relationship through an extern function call
                                                rship = ctx.getBuilder().CreateCall(rship_by_id, {g, lhsi});

                                                // check for the given label
                                                auto consume_cond = ctx.rship_cmp_label(rship, label_code);

                                                // branch if relationship label is equal to the given label
                                                ctx.getBuilder().CreateCondBr(consume_cond, consumeBB, nextBB);
                                            });

    // obtain the next relationship 
    ctx.getBuilder().SetInsertPoint(nextBB);
    {
        auto next_rship = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rship, next_rship_idx));
        ctx.getBuilder().CreateStore(next_rship, lhs);
        ctx.getBuilder().CreateBr(loop_body);
    }
//    }

    // consume the given relationship
    ctx.getBuilder().SetInsertPoint(consumeBB);
    {
        if(profiling) {
            t_end = ctx.getBuilder().CreateCall(fadd_now, {});
            ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
        }

        reg_query_results.push_back({rship, 1});
        prev_bb = consumeBB;
    }

    // backflow after complete iteration to the previous operator
    ctx.getBuilder().SetInsertPoint(foreach_rship_end);
    {
        // return to main loop of scan
        ctx.getBuilder().CreateBr(main_return);
    }

    /*if(op->is_variable()) {
        main_return = loop_head;
    } else {*/
        // set backflow to the iteration loop
    main_return = nextBB;
    //}

}