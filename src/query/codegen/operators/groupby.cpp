#include "codegen.hpp"

/**
 * Generates code for the group_by operation
 */
void codegen_inline_visitor::visit(std::shared_ptr<group_by> op) {
#if not_yet
    if(pipelined_finish) {
        //main_function = Function::Create(ctx.startFctTy, Function::ExternalLinkage, query_id_str, ctx.getModule());
        main_finish   = Function::Create(ctx.finishFctTy, Function::ExternalLinkage, "finish_"+query_id_str, ctx.getModule());
        
        //pipelines.push_back(std::string(main_function->getName()));
        pipelines.push_back(std::string(main_finish->getName()));
    }

    auto get_node_grpkey = ctx.extern_func("get_node_grpkey");
    auto get_rship_grpkey = ctx.extern_func("get_rship_grpkey");
    auto get_int_grpkey = ctx.extern_func("get_int_grpkey");
    auto get_string_grpkey = ctx.extern_func("get_string_grpkey");
    auto get_time_grpkey = ctx.extern_func("get_time_grpkey");
    auto add_to_group = ctx.extern_func("add_to_group");

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    BasicBlock *group_entry = BasicBlock::Create(ctx.getContext(), "group_entry", cur_pipeline);

    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(group_entry);
    ctx.getBuilder().SetInsertPoint(group_entry);

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    // materialize tuple
    FunctionCallee mat_reg = ctx.extern_func("mat_reg_value");
    auto qctx = cur_pipeline->args().begin();
    auto qarg = cur_pipeline->args().begin() + 1;
    auto g = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx, 0));

    auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
    auto arg_pos = ConstantInt::get(ctx.int64Ty, 3);
    //auto qargs = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx, 3));
    
    auto grp_p = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qarg, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    for(auto & res : reg_query_results) {
        // value type 7 => boolean, already transformed into integer
        auto type_id = (res.type == 7 ? 2 : res.type == 0 ? 90 : res.type == 1 ? 91 : res.type);
        auto type = ConstantInt::get(ctx.int64Ty, type_id);
        auto cv_reg = ctx.getBuilder().CreateBitCast(res.reg_val, ctx.int64PtrTy);
        
        ctx.getBuilder().CreateCall(mat_reg, {g, cv_reg, type});
        
    }

    std::vector<std::pair<int,int>> pos_type;
    int i = 0;
    for(auto pos : op->grpkey_pos_) {
        auto qr = reg_query_results[pos];
        auto pos_val = ConstantInt::get(ctx.int64Ty, pos);
        switch(qr.type) {
            case 0: {
                ctx.getBuilder().CreateCall(get_node_grpkey, {qr.reg_val, pos_val});
                
                break;
            } 
            case 1: {
                ctx.getBuilder().CreateCall(get_rship_grpkey, {qr.reg_val, pos_val});
                break;
            } 
            case 2: {
                ctx.getBuilder().CreateCall(get_int_grpkey, {ctx.getBuilder().CreateLoad(qr.reg_val), pos_val});
                break;
            } 
            case 3: {
                
                break;
            }
            case 4: {
                ctx.getBuilder().CreateCall(get_string_grpkey, {qr.reg_val, pos_val});
                break;
            }
            case 6: {
                ctx.getBuilder().CreateCall(get_time_grpkey, {qr.reg_val, pos_val});
                break;
            } 
            break;
        }
        pos_type.push_back({i++, qr.type});
    }

    ctx.getBuilder().CreateCall(add_to_group, {grp_p});
    
    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }

    ctx.getBuilder().CreateBr(main_return);

    //modify the finish processing -> add to last basic block of pipeline
    BasicBlock *group_finish = BasicBlock::Create(ctx.getContext(), "group_finish", main_finish);
    ctx.getBuilder().SetInsertPoint(df_finish_bb);

    if(pipelined_finish)
        //ctx.getBuilder().CreateBr(main_return);
        ctx.getBuilder().CreateRetVoid();
    else
        ctx.getBuilder().CreateBr(group_finish);
    
    prev_bb = group_entry;
    
    ctx.getBuilder().SetInsertPoint(group_finish);
    auto qctx_f = main_finish->args().begin() + 1;
    auto g_f = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx_f, 0));
    auto grp_pf = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);

    auto finish_group_by = ctx.extern_func("finish_group_by");
    auto rs_arg = main_finish->args().begin()+2;
    ctx.getBuilder().CreateCall(finish_group_by, {grp_pf, rs_arg});

    auto int_to_reg = ctx.extern_func("int_to_reg");
    auto str_to_reg = ctx.extern_func("str_to_reg");
    auto node_to_reg = ctx.extern_func("node_to_reg");
    auto rship_to_reg = ctx.extern_func("rship_to_reg");
    auto time_to_reg = ctx.extern_func("time_to_reg");
    auto get_grp_rs_count = ctx.extern_func("get_grp_rs_count");
    auto grp_demat_at = ctx.extern_func("grp_demat_at");

    auto demat_results = ctx.getBuilder().CreateCall(get_grp_rs_count, {grp_pf});
    auto cur_pos = insert_alloca(ctx.int64Ty);
    //auto cur_pos = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, cur_pos);
    
    BasicBlock *group_loop_head = BasicBlock::Create(ctx.getContext(), "group_loop_head", main_finish);
    BasicBlock *group_loop_body = BasicBlock::Create(ctx.getContext(), "group_loop_body", main_finish);
    BasicBlock *group_loop_exit = BasicBlock::Create(ctx.getContext(), "group_loop_exit", main_finish);

    // demat all grouped intermediate result again to registers
    ctx.getBuilder().CreateBr(group_loop_head);

    ctx.getBuilder().SetInsertPoint(group_loop_head);
    auto cur_idx = ctx.getBuilder().CreateLoad(cur_pos);
    auto cmp_head = ctx.getBuilder().CreateICmpULT(cur_idx, demat_results);
    ctx.getBuilder().CreateCondBr(cmp_head, group_loop_body, group_loop_exit);

    ctx.getBuilder().SetInsertPoint(group_loop_body);
    auto tuple = ctx.getBuilder().CreateCall(grp_demat_at, {grp_pf, cur_idx});
    auto new_idx = ctx.getBuilder().CreateAdd(cur_idx, ctx.LLVM_ONE);
    ctx.getBuilder().CreateStore(new_idx, cur_pos);

    std::vector<QR_VALUE> new_query_res;
    for(auto & pt : pos_type) {
        auto pos = ConstantInt::get(ctx.int64Ty, pt.first);
        Value *demat = nullptr;
        switch(pt.second) {
            case 0: {
                auto n = ctx.getBuilder().CreateCall(node_to_reg, {tuple, pos});
                new_query_res.push_back({n, pt.second});
                continue;
            }
            case 1: {
                auto r = ctx.getBuilder().CreateCall(rship_to_reg, {tuple, pos});
                new_query_res.push_back({r, pt.second});
                continue;
            }
            case 2: {
                auto i = ctx.getBuilder().CreateCall(int_to_reg, {tuple, pos});
                demat = insert_alloca(ctx.int64Ty);
                //demat = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
                ctx.getBuilder().CreateStore(i, demat);
                break;
            }
            case 3: continue;
            case 4: {
                auto str = ctx.getBuilder().CreateCall(str_to_reg, {tuple, pos});
                demat = insert_alloca(ctx.int64Ty);
                //demat = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
                ctx.getBuilder().CreateStore(str, demat);
                break;
            }
            case 6: {
                auto t = ctx.getBuilder().CreateCall(time_to_reg, {tuple, pos});
                demat = insert_alloca(ctx.int64Ty);
                //demat = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
                ctx.getBuilder().CreateStore(t, demat);
                break;
            }
        }
        new_query_res.push_back({demat, pt.second});
    }
    reg_query_results = new_query_res;
    //ctx.getBuilder().CreateBr(group_loop_head);

    ctx.getBuilder().SetInsertPoint(group_loop_exit);
  //  if(pipelined_finish)
  //      ctx.getBuilder().CreateBr(main_return);
    df_finish_bb = group_loop_exit;
    main_return = group_loop_head;
    prev_bb = group_loop_body;

    pipelined_finish = true;
#endif
}