#include "filter_expression.hpp"

std::map<int, Value*> expr_register;

fep_visitor_inline::fep_visitor_inline(PContext *ctx, Function *parent, std::vector<codegen_inline_visitor::QR_VALUE> &qr, BasicBlock *next, BasicBlock *end, Value *item_arr, Value *item, Value *pset,
                                       Value *unknown_id, Value *plist_id, Value *loop_cnt, Value *max_cnt, Value *pitem,
        Value *arg)
        : qr_regs_(qr),
          ctx_(ctx),
          fct_(parent),
          next_(next),
          end_(end) {
    cur_item = item;
    cur_item_arr = item_arr;
    cur_pset = pset;
    arg_ = arg;
    unknown_id_ = unknown_id;
    plist_id_ = plist_id;
    loop_cnt_ = loop_cnt;
    max_cnt_ = max_cnt;
    pitem_ = pitem;

    //roi_ = qr_node;
}

/**
 * Generates code to evaluate a number. The number value is stored in the evaluation stack
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<number_token> num) {
    num->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), num);

    //ConstantInt::get(ctx_->int64Ty, num->value_)
    //gen_vals_[opd_cnt] = alloc("int_" + std::to_string(num->value_), ctx_->int64Ty,
    //                           arg_);

    expr_register[opd_cnt] = arg_;

    opd_cnt++;
}

/**
 * Generates code to obtain a property value for evaluation
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<key_token> key)  {
    key->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), key);
    auto spitem = add_bb("search_pitem_" + key->key_);
    ctx_->getBuilder().CreateBr(spitem);
    ctx_->getBuilder().SetInsertPoint(spitem);

    // get the tuple from the register
    auto roi = qr_regs_.at(key->qr_id_).reg_val;

    // obtain the dict code
    auto dc = ctx_->get_dcode(key->key_);
    auto pcode = ConstantInt::get(ctx_->int32Ty, APInt(32, dc));

    // add the relevant basic blocks
    auto is_node = add_bb("is_node_" + std::to_string(opd_cnt));
    auto is_rship = add_bb("is_rship_" + std::to_string(opd_cnt));
    auto for_each_pitem = add_bb("for_each_pitem_" + std::to_string(opd_cnt));
    auto for_each_increment = add_bb("for_each_increment" + std::to_string(opd_cnt));
    auto pitem_found = add_bb("pitem_found" + std::to_string(opd_cnt));
    auto next_prop_it = add_bb("next_prop_it" + std::to_string(opd_cnt));
    //auto cmp_type = ctx_->getBuilder().CreateICmpEQ(type_, ctx_->LLVM_ZERO, "cmp_type");
    ctx_->getBuilder().CreateBr(is_node);
    //ctx_->getBuilder().CreateCondBr(cmp_type, is_node, is_rship);


    ctx_->getBuilder().SetInsertPoint(is_rship);
    ctx_->getBuilder().CreateRet(nullptr);

    // process node tuple
    ctx_->getBuilder().SetInsertPoint(is_node);
    auto res_node = ctx_->getBuilder().CreateBitCast(roi, ctx_->nodePtrTy);
    auto plist_id = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateStructGEP(res_node, 4));
    //auto plist_id_alloc = alloc("roi_plist_alloc", ctx_->int64Ty, plist_id);
    ctx_->getBuilder().CreateStore(plist_id, plist_id_);

    Value *item_arr;
    Value *cur_item_reg;

    auto gdb = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateStructGEP(fct_->args().begin(), 0));

    // iterate through property list of node
    auto loop_body = ctx_->while_loop_condition(fct_, plist_id_, unknown_id_, ctx_->WHILE_COND::LT, end_,
                                                [&](BasicBlock *body, BasicBlock *epilog) {

                                                    // load property list id
                                                    auto pid = ctx_->getBuilder().CreateLoad(plist_id_);

                                                    // load property list
                                                    auto pset = ctx_->getBuilder().CreateCall(
                                                            ctx_->extern_func("pset_get_item_at"),
                                                            {gdb, pid});
                                                    ctx_->getBuilder().CreateStore(pset, cur_pset);

                                                    // get pitem set
                                                    auto pitems = ctx_->getBuilder().CreateStructGEP(pset, 2);

                                                    // get item array
                                                    item_arr = /*ctx_->getBuilder().CreateLoad(*/
                                                            ctx_->getBuilder().CreateStructGEP(pitems, 0);
                                                    //ctx_->getBuilder().CreateStore(item_arr, cur_item_arr);
                                                    ctx_->getBuilder().CreateStore(ctx_->LLVM_ZERO, loop_cnt_);
                                                    ctx_->getBuilder().CreateBr(for_each_pitem);
                                                });

    // iterate through item array
    ctx_->getBuilder().SetInsertPoint(for_each_pitem);
    auto foreach_pitem_body = ctx_->while_loop_condition(fct_, loop_cnt_, max_cnt_, ctx_->WHILE_COND::LT,
                                                         next_prop_it, [&](BasicBlock *body, BasicBlock *epilog) {
                auto idx = ctx_->getBuilder().CreateLoad(loop_cnt_);

                // load item from array at idx into register
                cur_item_reg = ctx_->getBuilder().CreateInBoundsGEP(item_arr, {ctx_->LLVM_ZERO, idx});
                //ctx_->getBuilder().CreateStore(ctx_->getBuilder().CreateLoad(item), cur_item);

                // extract key 
                auto item_key = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateStructGEP(cur_item_reg, 1));

                // compare key with given key
                auto key_cmp = ctx_->getBuilder().CreateICmp(CmpInst::ICMP_EQ, item_key, pcode, "item.key_eq_pkey");
                ctx_->getBuilder().CreateCondBr(key_cmp, pitem_found, for_each_increment);
            });

    // increment idx for array iteration
    ctx_->getBuilder().SetInsertPoint(for_each_increment);
    {
        auto idx = ctx_->getBuilder().CreateLoad(loop_cnt_);
        auto idxpp = ctx_->getBuilder().CreateAdd(idx, ctx_->LLVM_ONE);
        ctx_->getBuilder().CreateStore(idxpp, loop_cnt_);
        ctx_->getBuilder().CreateBr(foreach_pitem_body);
    }

    // get next property iteration
    ctx_->getBuilder().SetInsertPoint(next_prop_it);
    {
    // get next prop -> GEP
        auto p_set = ctx_->getBuilder().CreateLoad(cur_pset);
        auto next_pset_id = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateStructGEP(p_set, 0));
        ctx_->getBuilder().CreateStore(next_pset_id, plist_id_);
        ctx_->getBuilder().CreateBr(loop_body);
    }

    //when item found -> store into the evaluation stack
    ctx_->getBuilder().SetInsertPoint(pitem_found);
    expr_register[opd_cnt] = cur_item_reg;

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));
    ctx_->getBuilder().CreateBr(epilog);
    ctx_->getBuilder().SetInsertPoint(epilog);
    opd_cnt++;
}

/**
 * Generates evaluation code for a string
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<str_token> str) {
    str->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), str);

    // get dictionary code of the given string
    auto dcode = ctx_->get_dcode(str->str_);

    expr_register[opd_cnt] = ConstantInt::get(ctx_->int64Ty, dcode);

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));
    ctx_->getBuilder().CreateBr(epilog);
    ctx_->getBuilder().SetInsertPoint(epilog);
    opd_cnt++;
}

/**
 * Generates evaluation code for a time value
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<time_token> time) {
    time->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), time);

    auto time_raw = ConstantInt::get(ctx_->int64Ty, (int64_t )&time->time_);
    auto time_ptr = ctx_->getBuilder().CreateIntToPtr(time_raw, ctx_->int64PtrTy);

    expr_register[opd_cnt] = time_ptr;

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));
    ctx_->getBuilder().CreateBr(epilog);
    ctx_->getBuilder().SetInsertPoint(epilog);
    opd_cnt++;
}

void fep_visitor_inline::visit(int rank, std::shared_ptr<func_call> fct) {
    
}

void fep_visitor_inline::visit(int rank, std::shared_ptr<fct_call> fct) {
    fct->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), fct);

    Value* fct_raw = nullptr;

    FunctionType* fct_callee_type = nullptr;

    switch(fct->fct_type_) {
        case FOP_TYPE::INT:
            fct_callee_type = FunctionType::get(ctx_->boolTy, {ctx_->int64PtrTy}, false);
            fct_raw = ConstantInt::get(ctx_->int64Ty, (int64_t )fct->fct_int_);
            break;
        case FOP_TYPE::STRING:
            fct_callee_type = FunctionType::get(ctx_->boolTy, {ctx_->int8PtrTy}, false);
            fct_raw = ConstantInt::get(ctx_->int64Ty, (int64_t )fct->fct_str_);
            break;
        case FOP_TYPE::UINT64:
            fct_callee_type = FunctionType::get(ctx_->boolTy, {ctx_->int64PtrTy}, false);
            fct_raw = ConstantInt::get(ctx_->int64Ty, (int64_t )fct->fct_uint_);
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::DATE:
        case FOP_TYPE::DOUBLE:
        case FOP_TYPE::KEY:
        case FOP_TYPE::OP:
        case FOP_TYPE::TIME:
            break;
    } 

    auto fct_ptr = ctx_->getBuilder().CreateIntToPtr(fct_raw, ctx_->int64PtrTy);

    auto fct_callee = ctx_->getBuilder().CreateBitCast(fct_ptr, fct_callee_type->getPointerTo());


    fct_callees_[opd_cnt] = {fct_callee, fct_callee_type};

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));
    ctx_->getBuilder().CreateBr(epilog);
    ctx_->getBuilder().SetInsertPoint(epilog);
    opd_cnt++;
}

/**
 * Generates code for the evaluation of an equality expression
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<eq_predicate> eq)  {
    eq->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), eq);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    
    auto cmp_alloc = add_bb("cmp_eq_"+ std::to_string(eq->opd_num));
    ctx_->getBuilder().CreateBr(cmp_alloc);
    ctx_->getBuilder().SetInsertPoint(cmp_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto rhs_alloc = expr_register[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    // convert it to the appropriate type and compare the two values
    switch ((*rhs_it)->ftype_) {
        case FOP_TYPE::INT: {
            auto val_rhs = rhs_alloc;
            auto int_value = ctx_->getBuilder().CreateLoad(
                    ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy));
            auto cmp_pitem = ctx_->getBuilder().CreateICmpEQ(int_value, val_rhs);

            expr_register[opd_cnt] = cmp_pitem;
            break;
        }
        case FOP_TYPE::DOUBLE: {
            auto val_rhs = ctx_->getBuilder().CreateLoad(rhs_alloc);
            auto int_value = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateBitCast(value_arr, ctx_->doubleTy));
            auto cmp_pitem = ctx_->getBuilder().CreateFCmpOEQ(int_value, val_rhs);
            gen_vals_[opd_cnt] = alloc("cmp_eq_res_" + std::to_string(eq->opd_num), ctx_->boolTy, cmp_pitem);
            break;
        }
        case FOP_TYPE::DATE:
        case FOP_TYPE::STRING:
        case FOP_TYPE::TIME:
        case FOP_TYPE::UINT64:
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::OP:
        case FOP_TYPE::KEY:
            break;
    }

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

/**
 * Generates code for the evaluation of an less equal expression
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<le_predicate> le)  {
    le->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), le);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    
    auto cmp_alloc = add_bb("cmp_le_"+ std::to_string(le->opd_num));
    ctx_->getBuilder().CreateBr(cmp_alloc);
    ctx_->getBuilder().SetInsertPoint(cmp_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto rhs_alloc = expr_register[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    // convert it to the appropriate type and compare the two values
    switch ((*rhs_it)->ftype_) {
        case FOP_TYPE::INT: {
            auto val_rhs = rhs_alloc;
            auto int_value = ctx_->getBuilder().CreateLoad(
                    ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy));
            auto cmp_pitem = ctx_->getBuilder().CreateICmpSLE(int_value, val_rhs);

            expr_register[opd_cnt] = cmp_pitem;
            break;
        }
        case FOP_TYPE::DOUBLE: {
            auto val_rhs = ctx_->getBuilder().CreateLoad(rhs_alloc);
            auto int_value = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateBitCast(value_arr, ctx_->doubleTy));
            auto cmp_pitem = ctx_->getBuilder().CreateFCmpOLE(int_value, val_rhs);
            gen_vals_[opd_cnt] = alloc("cmp_le_res_" + std::to_string(le->opd_num), ctx_->boolTy, cmp_pitem);
            break;
        }
        case FOP_TYPE::DATE:
        case FOP_TYPE::STRING:
        case FOP_TYPE::TIME:
        case FOP_TYPE::UINT64:
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::OP:
        case FOP_TYPE::KEY:
            break;
    }

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

/**
 * Generates code for the evaluation of an less equal expression
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<lt_predicate> op)  {
    op->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), op);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    
    auto cmp_alloc = add_bb("cmp_lt_"+ std::to_string(op->opd_num));
    ctx_->getBuilder().CreateBr(cmp_alloc);
    ctx_->getBuilder().SetInsertPoint(cmp_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto rhs_alloc = expr_register[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    // convert it to the appropriate type and compare the two values
    switch ((*rhs_it)->ftype_) {
        case FOP_TYPE::INT: {
            auto val_rhs = rhs_alloc;
            auto int_value = ctx_->getBuilder().CreateLoad(
                    ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy));
            auto cmp_pitem = ctx_->getBuilder().CreateICmpSLT(int_value, val_rhs);

            expr_register[opd_cnt] = cmp_pitem;
            break;
        }
        case FOP_TYPE::DOUBLE: {
            auto val_rhs = ctx_->getBuilder().CreateLoad(rhs_alloc);
            auto int_value = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateBitCast(value_arr, ctx_->doubleTy));
            auto cmp_pitem = ctx_->getBuilder().CreateFCmpOLT(int_value, val_rhs);
            gen_vals_[opd_cnt] = alloc("cmp_lt_res_" + std::to_string(op->opd_num), ctx_->boolTy, cmp_pitem);
            break;
        }
        case FOP_TYPE::DATE:
        case FOP_TYPE::STRING:
        case FOP_TYPE::TIME:
        case FOP_TYPE::UINT64:
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::OP:
        case FOP_TYPE::KEY:
            break;
    }

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

/**
 * Generates code for the evaluation of an less equal expression
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<ge_predicate> op)  {
    op->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), op);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    
    auto cmp_alloc = add_bb("cmp_ge_"+ std::to_string(op->opd_num));
    ctx_->getBuilder().CreateBr(cmp_alloc);
    ctx_->getBuilder().SetInsertPoint(cmp_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto rhs_alloc = expr_register[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    // convert it to the appropriate type and compare the two values
    switch ((*rhs_it)->ftype_) {
        case FOP_TYPE::INT: {
            auto val_rhs = rhs_alloc;
            auto int_value = ctx_->getBuilder().CreateLoad(
                    ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy));
            auto cmp_pitem = ctx_->getBuilder().CreateICmpSGE(int_value, val_rhs);

            expr_register[opd_cnt] = cmp_pitem;
            break;
        }
        case FOP_TYPE::DOUBLE: {
            auto val_rhs = ctx_->getBuilder().CreateLoad(rhs_alloc);
            auto int_value = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateBitCast(value_arr, ctx_->doubleTy));
            auto cmp_pitem = ctx_->getBuilder().CreateFCmpOGE(int_value, val_rhs);
            gen_vals_[opd_cnt] = alloc("cmp_ge_res_" + std::to_string(op->opd_num), ctx_->boolTy, cmp_pitem);
            break;
        }
        case FOP_TYPE::DATE:
        case FOP_TYPE::STRING:
        case FOP_TYPE::TIME:
        case FOP_TYPE::UINT64:
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::OP:
        case FOP_TYPE::KEY:
            break;
    }

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

/**
 * Generates code for the evaluation of an less equal expression
 */
void fep_visitor_inline::visit(int rank, std::shared_ptr<gt_predicate> op)  {
    op->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), op);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    
    auto cmp_alloc = add_bb("cmp_gt_"+ std::to_string(op->opd_num));
    ctx_->getBuilder().CreateBr(cmp_alloc);
    ctx_->getBuilder().SetInsertPoint(cmp_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto rhs_alloc = expr_register[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    // convert it to the appropriate type and compare the two values
    switch ((*rhs_it)->ftype_) {
        case FOP_TYPE::INT: {
            auto val_rhs = rhs_alloc;
            auto int_value = ctx_->getBuilder().CreateLoad(
                    ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy));
            auto cmp_pitem = ctx_->getBuilder().CreateICmpSGT(int_value, val_rhs);

            expr_register[opd_cnt] = cmp_pitem;
            break;
        }
        case FOP_TYPE::DOUBLE: {
            auto val_rhs = ctx_->getBuilder().CreateLoad(rhs_alloc);
            auto int_value = ctx_->getBuilder().CreateLoad(ctx_->getBuilder().CreateBitCast(value_arr, ctx_->doubleTy));
            auto cmp_pitem = ctx_->getBuilder().CreateFCmpOGT(int_value, val_rhs);
            gen_vals_[opd_cnt] = alloc("cmp_gt_res_" + std::to_string(op->opd_num), ctx_->boolTy, cmp_pitem);
            break;
        }
        case FOP_TYPE::DATE:
        case FOP_TYPE::STRING:
        case FOP_TYPE::TIME:
        case FOP_TYPE::UINT64:
        case FOP_TYPE::BOOL_OP:
        case FOP_TYPE::OP:
        case FOP_TYPE::KEY:
            break;
    }

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

void fep_visitor_inline::visit(int rank, std::shared_ptr<and_predicate> andpr) {
    andpr->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), andpr);

    //auto bopd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto cit = expr_stack.begin() + 1;
    auto offset = 0;

    while ((*cit)->ftype_ == FOP_TYPE::BOOL_OP) {
        offset++;
        cit--;
    }

    auto rhs_pos = std::pow(2, 2 + offset);
    auto lhs_it = expr_stack.begin() + rhs_pos;

    auto cmp_or_bb = add_bb("cmp_and_" + std::to_string(andpr->opd_num));
    ctx_->getBuilder().CreateBr(cmp_or_bb);
    ctx_->getBuilder().SetInsertPoint(cmp_or_bb);
    auto lhs = ctx_->getBuilder().CreateLoad(gen_vals_[(*lhs_it)->opd_num]);
    auto rval = gen_vals_[(*rhs_it)->opd_num];
    auto rhs = ctx_->getBuilder().CreateLoad(rval);
    auto cmp_eq = ctx_->getBuilder().CreateAnd(lhs, rhs);
    gen_vals_[opd_cnt] = alloc("cmp_and_res_" + std::to_string(andpr->opd_num), ctx_->boolTy, cmp_eq);

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    if (rank == 0) {
        auto ptr = gen_vals_[opd_cnt];
        auto cond = ctx_->getBuilder().CreateLoad(ptr);
        ctx_->getBuilder().CreateCondBr(cond, next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

void fep_visitor_inline::visit(int rank, std::shared_ptr<or_predicate> orpr) {
    orpr->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), orpr);

    //auto bopd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto cit = expr_stack.begin() + 1;
    auto offset = 0;

    while ((*cit)->ftype_ == FOP_TYPE::BOOL_OP) {
        offset++;
        cit--;
    }

    auto rhs_pos = std::pow(2, 2 + offset);
    auto lhs_it = expr_stack.begin() + rhs_pos;

    auto cmp_and_bb = add_bb("cmp_or_" + std::to_string(orpr->opd_num));
    ctx_->getBuilder().CreateBr(cmp_and_bb);
    ctx_->getBuilder().SetInsertPoint(cmp_and_bb);
    auto lhs = ctx_->getBuilder().CreateLoad(gen_vals_[(*lhs_it)->opd_num]);
    auto rhs = ctx_->getBuilder().CreateLoad(gen_vals_[(*rhs_it)->opd_num]);
    auto cmp_eq = ctx_->getBuilder().CreateOr(lhs, rhs);
    gen_vals_[opd_cnt] = alloc("cmp_or_res_" + std::to_string(orpr->opd_num), ctx_->boolTy, cmp_eq);

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));
    if (rank == 0) {
        auto ptr = gen_vals_[opd_cnt];
        auto cond = ctx_->getBuilder().CreateLoad(ptr);
        ctx_->getBuilder().CreateCondBr(cond, next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

void fep_visitor_inline::visit(int rank, std::shared_ptr<call_predicate> call) {
    call->opd_num = opd_cnt;
    expr_stack.insert(expr_stack.begin(), call);

    // obtain the evaluation stack values
    //auto opd_it = expr_stack.begin();
    auto rhs_it = expr_stack.begin() + 1;
    auto lhs_it = expr_stack.begin() + 2;

    auto call_alloc = add_bb("cmp_eq_"+ std::to_string(call->opd_num));
    ctx_->getBuilder().CreateBr(call_alloc);
    ctx_->getBuilder().SetInsertPoint(call_alloc);

    // extract the values from the stack
    auto pitem = expr_register[(*lhs_it)->opd_num];
    auto callee = fct_callees_[(*rhs_it)->opd_num];

    // get the value arr from the property item
    auto value_arr = ctx_->getBuilder().CreateStructGEP(pitem, 0);

    auto int_value = ctx_->getBuilder().CreateBitCast(value_arr, ctx_->int64PtrTy);
        
    expr_register[opd_cnt] = ctx_->getBuilder().CreateCall(callee.type_, callee.callee_, {int_value});

    auto epilog = add_bb("epilog_" + std::to_string(opd_cnt));

    // if no other expression is given, jump to the next block, otherwise link to the next expression
    if (rank == 0) {
        ctx_->getBuilder().CreateCondBr(expr_register[opd_cnt], next_, end_);
    } else {
        ctx_->getBuilder().CreateBr(epilog);
        ctx_->getBuilder().SetInsertPoint(epilog);
    }
    opd_cnt++;
}

Value * fep_visitor_inline::alloc(std::string name, Type *type, Value *val) {
    if (alloc_stack.find(name) == alloc_stack.end()) {
        alloc_stack[name] = ctx_->getBuilder().CreateAlloca(type);
        if (val) {
            ctx_->getBuilder().CreateStore(val, alloc_stack[name]);
        }
    }
    return alloc_stack[name];
}

BasicBlock * fep_visitor_inline::add_bb(std::string name) {
    bbs_[name] = BasicBlock::Create(ctx_->getModule().getContext(), name, fct_);
    return bbs_[name];
}