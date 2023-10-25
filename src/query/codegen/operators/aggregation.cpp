#include "codegen.hpp"

/**
 * Generates code for aggr operations
 */

/*
void codegen_inline_visitor::visit(std::shared_ptr<aggr_op> op) {
    op->name_ = "";
    auto init_grp_aggr = ctx.extern_func("init_grp_aggr");
    auto grp_cnt = ctx.extern_func("get_group_cnt");
    auto grp_total_cnt = ctx.extern_func("get_total_group_cnt");
    auto get_group_sum_int = ctx.extern_func("get_group_sum_int"); 
    auto get_group_sum_double = ctx.extern_func("get_group_sum_double");
    auto get_group_sum_uint = ctx.extern_func("get_group_sum_uint");  

    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    //modify the finish processing -> add to last basic block of pipeline
    BasicBlock *aggr_finish = BasicBlock::Create(ctx.getContext(), "aggr_finish", main_finish);
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(aggr_finish);
    ctx.getBuilder().SetInsertPoint(aggr_finish);


    auto opid = ConstantInt::get(ctx.int64Ty, op->op_id_);
    auto qctx_f = main_finish->args().begin() + 1;
    auto g_f = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx_f, 0));

    auto grp_pf = ctx.getBuilder().CreateBitCast(
        ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(qctx_f, {ctx.LLVM_ZERO, opid})),
        ctx.int8PtrTy);
    
    Value* t_start = nullptr;
    Value* t_end = nullptr;

    if(profiling) 
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    ctx.getBuilder().CreateCall(init_grp_aggr, {grp_pf});
    for(auto &aggr: op->aggrs_) {
        if(aggr.first.compare("count") == 0) {
            auto cnt_alloc = insert_alloca(ctx.int64Ty);
            //auto cnt_alloc =  ctx.getBuilder().CreateAlloca(ctx.int64Ty);
            auto cnt = ctx.getBuilder().CreateCall(grp_cnt, {grp_pf});
            ctx.getBuilder().CreateStore(cnt, cnt_alloc);
            reg_query_results.push_back({cnt_alloc, 2});
        } else if(aggr.first.compare("pcount") == 0) {
            //auto pcnt_alloc = insert_alloca(ctx.doubleTy);
            auto pcnt_alloc = ctx.getBuilder().CreateAlloca(ctx.doubleTy);
            auto cnt = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateCall(grp_cnt, {grp_pf}), ctx.doubleTy);
            auto total_cnt = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateCall(grp_total_cnt, {grp_pf}), ctx.doubleTy);
            auto cnt_div = ctx.getBuilder().CreateFDiv(cnt, total_cnt);
            auto pcount = ctx.getBuilder().CreateFMul(cnt_div, ConstantFP::get(ctx.getContext(), APFloat(100.0)));
            ctx.getBuilder().CreateStore(pcount, pcnt_alloc);
            reg_query_results.push_back({ctx.getBuilder().CreateBitCast(pcnt_alloc, ctx.int64PtrTy), 3});
        } else if(aggr.first.compare("sum") == 0) {
            auto pos = ConstantInt::get(ctx.int64Ty, aggr.second);
            switch(reg_query_results[aggr.second].type) {
                case 2: {
                    auto sum_alloc = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
                    //auto sum_alloc = insert_alloca(ctx.int64Ty);
                    auto sum = ctx.getBuilder().CreateCall(get_group_sum_int, {grp_pf, pos});
                    ctx.getBuilder().CreateStore(sum, sum_alloc);
                    reg_query_results.push_back({sum_alloc, 2});
                    break;
                }
                case 3: {
                    auto sum_alloc = insert_alloca(ctx.doubleTy);
                    auto sum = ctx.getBuilder().CreateCall(get_group_sum_double, {grp_pf, pos});
                    ctx.getBuilder().CreateStore(sum, sum_alloc);
                    reg_query_results.push_back({sum_alloc, 3});
                    break;
                }
                case 5: {
                    auto sum_alloc = insert_alloca(ctx.int64Ty);
                    auto sum = ctx.getBuilder().CreateCall(get_group_sum_uint, {grp_pf, pos});
                    ctx.getBuilder().CreateStore(sum, sum_alloc);
                    reg_query_results.push_back({sum_alloc, 5});
                    break;
                }
            }

        } else if(aggr.first.compare("avg") == 0) {
            auto pos = ConstantInt::get(ctx.int64Ty, aggr.second);
            switch(reg_query_results[aggr.second].type) {
                case 2: {
                    auto avg_alloc = ctx.getBuilder().CreateAlloca(ctx.doubleTy);
                    //auto avg_alloc = insert_alloca(ctx.doubleTy);
                    auto cnt = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateCall(grp_cnt, {grp_pf}), ctx.doubleTy);
                    auto sum = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateCall(get_group_sum_int, {grp_pf, pos}), ctx.doubleTy);
                    auto avg = ctx.getBuilder().CreateFDiv(sum, cnt);
                    ctx.getBuilder().CreateStore(avg, avg_alloc);
                    reg_query_results.push_back({avg_alloc, 3});
                    break;
                }
                case 3: {
                    auto avg_alloc = insert_alloca(ctx.doubleTy);
                    auto cnt = ctx.getBuilder().CreateBitCast(ctx.getBuilder().CreateCall(grp_cnt, {grp_pf}), ctx.doubleTy);
                    auto sum = ctx.getBuilder().CreateCall(get_group_sum_double, {grp_pf, pos});
                    auto avg = ctx.getBuilder().CreateUDiv(sum, cnt);
                    ctx.getBuilder().CreateStore(avg, avg_alloc);
                    reg_query_results.push_back({avg_alloc, 3});
                    break;
                }
                case 5: {
                    auto avg_alloc = insert_alloca(ctx.int64Ty);
                    auto cnt = ctx.getBuilder().CreateCall(grp_cnt, {grp_pf});
                    auto sum = ctx.getBuilder().CreateCall(get_group_sum_uint, {grp_pf, pos});
                    auto avg = ctx.getBuilder().CreateUDiv(sum, cnt);
                    ctx.getBuilder().CreateStore(avg, avg_alloc);
                    reg_query_results.push_back({avg_alloc, 5});
                    break;
                }
            }
        }
    }
    
    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->op_id_), t_start, t_end});
    }

    prev_bb = aggr_finish;
    //df_finish = aggr_finish;
    //main_function = main_finish;
    cur_pipeline = main_finish;
}*/