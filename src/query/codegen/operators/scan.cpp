
#include "codegen.hpp"

int qcnt = 0;

/*
* Generates code for a scan operator.
*/
void codegen_inline_visitor::visit(std::shared_ptr<scan_nodes> op) {
    // clear register mapping of previous access path
    reg_query_results.clear();
    // process the appropriate query length
    query_length(op);

    // create unique function name for the JIT instance
    int cnt = 0;

    std::string name = qid_+std::to_string(qcnt++);
    std::string finish_name = "finish_"+name;
    //}

    query_id_str = name;
    main_function = Function::Create(ctx.startFctTy, Function::ExternalLinkage, name, ctx.getModule());
    main_finish   = Function::Create(ctx.finishFctTy, Function::ExternalLinkage, finish_name, ctx.getModule());

    pipelines.push_back(std::string(main_function->getName()));
    pipelines.push_back(std::string(main_finish->getName()));

    cur_pipeline = main_function;
    // obtain all relevant function callees for the scan operator
    //FunctionCallee dict_lookup_label = ctx.extern_func("dict_lookup_label");
    auto get_valid_node = ctx.extern_func("get_valid_node");
    auto gdb_get_nodes = ctx.extern_func("gdb_get_nodes");
    auto gdb_get_node_from_it = ctx.extern_func("get_node_from_it");
    auto get_begin = ctx.extern_func("get_vec_begin");
    auto get_next = ctx.extern_func("get_vec_next");
    auto is_end = ctx.extern_func("vec_end_reached");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    /*
    * When a previous access path already exists, push the new entry block as the first block into the
    * function block list and link it as the finish block
    */
    BasicBlock *bb;
    BasicBlock *next_op;

    inits = BasicBlock::Create(ctx.getModule().getContext(), "inits_entry", main_function);
    bb = BasicBlock::Create(ctx.getModule().getContext(), "entry", main_function);

    entry = bb;

    // initialize all relevant functions
    init_function(inits);

    scan_nodes_end = BasicBlock::Create(ctx.getModule().getContext(), "scan_nodes_end", main_function);
    BasicBlock *consumeBB = BasicBlock::Create(ctx.getModule().getContext(), "consume_node", main_function);

    df_finish_bb = BasicBlock::Create(ctx.getContext(), "finish_entry", main_finish);

    // obtain the query context & function arguments

    query_context = main_function->args().begin();
    queryArgs = main_function->args().begin() + 1;

    BasicBlock *curBB;

    //if(access)
    //else 
    extract_query_context(query_context);
    ctx.getBuilder().CreateBr(bb);

    ctx.getBuilder().SetInsertPoint(bb);
    
    
    // register for the current node
    Value *node;

    Value *t_start;
    Value *t_end;

    // scan all
 //   if(!op->indexed_) {
    // SCAN NODES
    // 1 get nodes vector from graph db
    auto nodes_vec = ctx.getBuilder().CreateCall(gdb_get_nodes, {gdb});

    // 3 obtain iterator from nodes vector
    auto node_begin_it = ctx.getBuilder().CreateCall(get_begin, {nodes_vec, first, last});

    // 2 lookup single label in dict
    if(op->labels.empty()) {
        auto label_code = ctx.extract_arg_label(op->operator_id_, gdb, queryArgs);

        // 4 iterate through node list
        curBB = ctx.while_loop(main_function, get_begin, get_next, is_end, node_begin_it, nodes_vec,
                                        scan_nodes_end, [&](BasicBlock *curBB, BasicBlock *epilog) {
                    if(profiling)
                        t_start = ctx.getBuilder().CreateCall(fadd_now, {});
                    // 5 obtain node from iterator
                    auto node_raw = ctx.getBuilder().CreateCall(gdb_get_node_from_it, {node_begin_it});

                    // 6 obtain valid node
                    node = ctx.getBuilder().CreateCall(get_valid_node, {gdb, node_raw, tx_ptr});

                    // 7 compare labels
                    auto cond = ctx.node_cmp_label(node, label_code);

                    // 8 if equal -> consume
                    ctx.getBuilder().CreateCondBr(cond, consumeBB, epilog);
                }, consumeBB);
    } else {
        FunctionCallee dict_lookup_label = ctx.extern_func("dict_lookup_label");
        auto opid = ConstantInt::get(ctx.int64Ty, op->operator_id_);
        
        std::vector<Value*> label_codes;
        std::vector<BasicBlock*> multi_label_conds;
        auto i = 0u;
        Value *id = opid;
        for(auto & label : op->labels) {
            auto str = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, id}));
            label_codes.push_back(ctx.getBuilder().
                                    CreateCall(dict_lookup_label, 
                                    {gdb, ctx.getBuilder().CreateBitCast(str, ctx.int8PtrTy)}
                                    ));
            id = ctx.getBuilder().CreateAdd(id, ctx.LLVM_ONE);
            multi_label_conds.push_back(BasicBlock::Create(ctx.getModule().getContext(), "condition_"+label, main_function));
        }

        BasicBlock *loop_epilog;
        curBB = ctx.while_loop(main_function, get_begin, get_next, is_end, node_begin_it, nodes_vec,
                                        scan_nodes_end, [&](BasicBlock *curBB, BasicBlock *epilog) {
                    loop_epilog = epilog;
                    // 5 obtain node from iterator
                    auto node_raw = ctx.getBuilder().CreateCall(gdb_get_node_from_it, {node_begin_it});

                    // 6 obtain valid node
                    node = ctx.getBuilder().CreateCall(get_valid_node, {gdb, node_raw, tx_ptr});

                    ctx.getBuilder().CreateBr(multi_label_conds.front());
                }, consumeBB);


        i = 0;
        for(auto cond : multi_label_conds) {
            ctx.getBuilder().SetInsertPoint(cond);
            auto label_cmp = ctx.node_cmp_label(node, label_codes[i++]);
            if(i == multi_label_conds.size()) {
                ctx.getBuilder().CreateCondBr(label_cmp, consumeBB, loop_epilog);
            } else {
                ctx.getBuilder().CreateCondBr(label_cmp, consumeBB, multi_label_conds[i]);
            }

        }

    }


/*    } else { // index scan

        // for index scan call extern gdb function and branch to consumer
        auto get_index_node = ctx.extern_func("index_get_node");

        // get label and property key from parameter descriptor
        auto opid = ConstantInt::get(ctx.int64Ty, op->op_id_);
        auto prop_opid = ConstantInt::get(ctx.int64Ty, op->op_id_+1);
        auto val_opid = ConstantInt::get(ctx.int64Ty, op->op_id_+2);
        auto label = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, opid}));
        auto prop = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, prop_opid}));
        auto val = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateInBoundsGEP(queryArgs, {ctx.LLVM_ZERO, val_opid})));

        // call extern function to obtain node
        node = ctx.getBuilder().CreateCall(get_index_node, {gdb,
                                                            ctx.getBuilder().CreateBitCast(label, ctx.int8PtrTy),
                                                            ctx.getBuilder().CreateBitCast(prop, ctx.int8PtrTy), val});

        // check if node ptr is null
        auto nullptr_cmp = ctx.getBuilder().CreateIsNotNull(node);

    	// consume or goto finish
        ctx.getBuilder().CreateCondBr(nullptr_cmp, consumeBB, scan_nodes_end);
        curBB = scan_nodes_end;
    }
*/
    // branch to the next oeprator
    ctx.getBuilder().SetInsertPoint(consumeBB);
    {
        if(profiling) {
            t_end = ctx.getBuilder().CreateCall(fadd_now, {});
            ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
        }
        // add current node to register
        reg_query_results.push_back({node, 0});
        main_return = curBB;
        // next operator fills this basic block and adds terminator
    }

    // branch to the finish function
    ctx.getBuilder().SetInsertPoint(scan_nodes_end);
    {   
        global_end = scan_nodes_end;
        //ctx.getBuilder().CreateCall(ctx.finishFctTy, finish, {rs});
        ctx.getBuilder().CreateRetVoid();
        //ctx.getBuilder().CreateBr(df_finish_bb);
    }

    // set appropriate backflow
    prev_bb = consumeBB;
    ctx.gen_funcs[name] = main_function;
}

void codegen_inline_visitor::visit(std::shared_ptr<index_scan> op) {
    
}