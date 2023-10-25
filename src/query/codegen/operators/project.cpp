#include "codegen.hpp"

/**
 * Generates code for a projection operator
 */ 
void codegen_inline_visitor::visit(std::shared_ptr<projection> op) {
    // obtain FunctionCallee for apply_pexpr
    //auto apply_pexpr = ctx.extern_func("apply_pexpr");
    auto apply_pexpr_node = ctx.extern_func("apply_pexpr_node");
    auto apply_pexpr_rship = ctx.extern_func("apply_pexpr_rship");
    auto node_has_property = ctx.extern_func("node_has_property");
    auto rship_has_property = ctx.extern_func("rship_has_property");
    auto apply_has_property = ctx.extern_func("apply_has_property");
    auto fadd_now = ctx.extern_func("get_now");
    auto fadd_time_diff = ctx.extern_func("add_time_diff");

    // create entry block and link with the previous operator
    BasicBlock *entry = BasicBlock::Create(ctx.getContext(), "project_entry", main_function);
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(entry);
    ctx.getBuilder().SetInsertPoint(entry);
    std::map<std::size_t, Value*> vmap;

    Value* t_start = nullptr;
    Value* t_end = nullptr;

    auto i = 1;

    if(profiling)
        t_start = ctx.getBuilder().CreateCall(fadd_now, {});
        
    // add new projection variables to registers
    std::vector<QR_VALUE> nqrv;
    for(auto & pe : op->prexpr_) {
        if(pe.prt == projection_expr::PROJECTION_TYPE::PROPERTY_PR) {
            auto r = reg_query_results[pe.id];
            if(pe.type == result_type::boolean) {
                nqrv.push_back(reg_query_results[pe.id]);
            } else {
                // get register value of tuple for projection
                auto qrp = ctx.getBuilder().CreateBitCast(r.reg_val, ctx.int64PtrTy);

                // get the appropriate type of the tuple
                auto type = ConstantInt::get(ctx.int64Ty, APInt(64, static_cast<int>(pe.type)));

                // apply the projection in an external function
                if(r.type == 0) {
                    ctx.getBuilder().CreateCall(apply_pexpr_node, {gdb, project_keys[i-1], type, qrp, pv[i]});
                } else {
                    ctx.getBuilder().CreateCall(apply_pexpr_rship, {gdb, project_keys[i-1], type, qrp, pv[i]});
                }

                // add new register value to global list
                nqrv.push_back({pv[i], static_cast<int>(pe.type)});

            }
        } else if(pe.prt == projection_expr::PROJECTION_TYPE::FORWARD_PR) {
            auto r = reg_query_results[pe.id];
            nqrv.push_back({r.reg_val, r.type});
            i++;
            continue;
        } else if(pe.prt == projection_expr::PROJECTION_TYPE::FUNCTIONAL_VAL) {
            auto fc_raw = ConstantInt::get(ctx.int64Ty, (int64_t)pe.udf_function);
            auto fc_ptr = ctx.getBuilder().CreateIntToPtr(fc_raw, ctx.int64PtrTy);
            auto fc_ty = FunctionType::get(ctx.int8PtrTy, {ctx.int8PtrTy, ctx.int8PtrTy, ctx.int8PtrTy}, false);
            auto udf = ctx.getBuilder().CreateBitCast(fc_ptr, fc_ty->getPointerTo());

            auto mat_node = ctx.extern_func("mat_node");
            auto mat_rship = ctx.extern_func("mat_rship");
            auto mat_reg = ctx.extern_func("mat_reg_value");
            auto r_to_q = ctx.extern_func("reg_to_qres");
            auto n_to_desc = ctx.extern_func("node_to_description");

            // convert register value into query_result
            auto tp = ctx.getBuilder().CreateCall(r_to_q, {reg_query_results.back().reg_val});
            
            // convert to node description
            auto tp2 = ctx.getBuilder().CreateCall(n_to_desc, {gdb});
            
            // call udf
            auto ret = ctx.getBuilder().CreateCall(fc_ty, udf, {tp2, tp2, tp2});

            // add to new result vector
            nqrv.push_back({ret, 10});
            continue;
        } else if(pe.prt == projection_expr::PROJECTION_TYPE::CONDITIONAL_VAL) {
            std::vector<Value*> prop_exists;
            std::vector<Value*> then_else;

            auto r  = reg_query_results[pe.id];

            for(auto & prop_str : pe.has_properties) {
                auto str_ptr = ctx.getBuilder().CreateGlobalStringPtr(prop_str);

                if(r.type == 0) {
                    prop_exists.emplace_back(
                        ctx.getBuilder().CreateCall(node_has_property, {gdb, r.reg_val, str_ptr}));
                } else {
                    prop_exists.emplace_back(
                        ctx.getBuilder().CreateCall(rship_has_property, {gdb, r.reg_val, str_ptr}));
                }
            }

            std::vector<Value*> add_sum;
            add_sum.push_back(ctx.LLVM_ZERO);
            
            for(auto p : prop_exists) {
                auto lhs = add_sum.back();
                add_sum.push_back(ctx.getBuilder().CreateAdd(lhs,p));
            }
            
            auto str_ptr_st = ctx.getBuilder().CreateGlobalStringPtr(pe.then_else.first);
            auto str_ptr_nd = ctx.getBuilder().CreateGlobalStringPtr(pe.then_else.second);
            
            ctx.getBuilder().CreateCall(apply_has_property, {add_sum.back(), str_ptr_st, str_ptr_nd, pv[i]});
            nqrv.push_back({pv[i], 4});
        }
        i++;
    }

    if(profiling) {
        t_end = ctx.getBuilder().CreateCall(fadd_now, {});
        ctx.getBuilder().CreateCall(fadd_time_diff, {query_context, ConstantInt::get(ctx.int64Ty, op->operator_id_), t_start, t_end});
    }

    // switch old register list with the new one
    reg_query_results = nqrv;

    prev_bb = entry;
}