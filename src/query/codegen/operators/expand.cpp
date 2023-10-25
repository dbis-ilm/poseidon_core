#include "codegen.hpp"

/**
 * Generates code for the expand operator
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<expand> op) {
    // get the id of the appropriate expand operation (to or from rship)
    unsigned exp_id;
    switch (op->dir_) {
        case EXPAND::OUT:
            exp_id = 3;
            break;
        case EXPAND::IN:
            exp_id = 2;
            break;
    }

    // create the basic blocks of the expand operator
    BasicBlock *entry = BasicBlock::Create(ctx.getContext(), "expand_entry", cur_pipeline);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "expand_consume", cur_pipeline);
    BasicBlock *null = BasicBlock::Create(ctx.getContext(), "expand_false", cur_pipeline);
    BasicBlock *check_label = BasicBlock::Create(ctx.getContext(), "check_label", cur_pipeline);

    Value *label_code = nullptr;
    Value* t_start = nullptr;
    Value* t_end = nullptr;

    // get the relevant FunctionCallee
    auto get_node_by_id = ctx.extern_func("node_by_id");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    // link with the previous operator
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(entry);
    ctx.getBuilder().SetInsertPoint(entry);

    if(profiling)
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});

    // obtain the last query result from register -> relationship
    auto rship = reg_query_results.back().reg_val;

    // extract the node id from the relationship
    auto exp_rship = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(rship, exp_id));

    auto qctx = cur_pipeline->args().begin();
    auto g = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(qctx, 0));


    //get node by id
    auto node = ctx.getBuilder().CreateCall(get_node_by_id, {g, exp_rship});
    qr.push_back(node);

    // test if node is null
    auto null_node = Constant::getNullValue(ctx.int8PtrTy);
    auto cmp_node = ctx.getBuilder().CreateBitCast(node, ctx.int8PtrTy);
    auto cmp = ctx.getBuilder().CreateICmpEQ(null_node, cmp_node);

    // branch to different positions, when node label is given
    if(op->label.empty())
        ctx.getBuilder().CreateCondBr(cmp, null, consume);
    else {
        //label_code = ctx.extract_arg_label(op->op_id_, gdb, queryArgs);
        auto args = cur_pipeline->args().begin()+1;
        label_code = ctx.extract_arg_label(op->operator_id_, g, args);
        ctx.getBuilder().CreateCondBr(cmp, null, check_label);
    }

    // node label is given
    if(!op->label.empty())
    {
        ctx.getBuilder().SetInsertPoint(check_label);
        {
            //auto label_code = ConstantInt::get(ctx.int32Ty, APInt(32, ctx.get_dcode(op->label_)));
            // compare labels 
            auto cond = ctx.node_cmp_label(node, label_code);

            // branch to appropriate block
            ctx.getBuilder().CreateCondBr(cond, consume, null);
        }
    } else if(!op->label.empty()) {        
        FunctionCallee dict_lookup_label = ctx.extern_func("dict_lookup_label");
        ctx.getBuilder().SetInsertPoint(check_label);
        auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
            
        std::vector<Value*> label_codes;
        std::vector<BasicBlock*> multi_label_conds;
        auto i = 0u;
        Value *id = opid;
        for(auto & label : op->label) {
            auto str = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, id}));
            label_codes.push_back(ctx.getBuilder().
                                     CreateCall(dict_lookup_label, 
                                        {gdb, ctx.getBuilder().CreateBitCast(str, ctx.int8PtrTy)}
                                        ));
            id = ctx.getBuilder().CreateAdd(id, ctx.LLVM_ONE);
            multi_label_conds.push_back(BasicBlock::Create(ctx.getModule().getContext(), "condition_"+label, main_function));
        }

        ctx.getBuilder().CreateBr(multi_label_conds.front());

        i = 0;
        for(auto cond : multi_label_conds) {
            ctx.getBuilder().SetInsertPoint(cond);
            auto label_cmp = ctx.node_cmp_label(node, label_codes[i++]);
            if(i == multi_label_conds.size()) {
                ctx.getBuilder().CreateCondBr(label_cmp, consume, null);
            } else {
                ctx.getBuilder().CreateCondBr(label_cmp, consume, multi_label_conds[i]);
            }

        }

    }

    // add result to register list 
    ctx.getBuilder().SetInsertPoint(consume);
    {
        if(profiling) {
            t_end = ctx.getBuilder().CreateCall(fadd_now, {});
            ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
        }
        reg_query_results.push_back({node, 0});

        prev_bb = consume;
        // next operator
    }

    ctx.getBuilder().SetInsertPoint(null);
    {
        ctx.getBuilder().CreateBr(main_return);
    }
}