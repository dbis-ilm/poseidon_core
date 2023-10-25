#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include "codegen.hpp"
#include "filter_expression.hpp"

// process recursively the type vector of the rhs of a join
void get_rhs_type(qop_ptr  &qop, std::vector<int> &typv) {
    auto op = qop;

    while(op->has_subscriber()) {
        if(op->type_ == qop_type::scan) {
            typv.push_back(0);
        }
        else if(op->type_ == qop_type::foreach_rship) {
            typv.push_back(1);
        }
        else if(op->type_ == qop_type::expand)
            typv.push_back(0);
        else if(op->type_ == qop_type::project) {
            auto prj = std::dynamic_pointer_cast<projection>(op);
            std::vector<int> new_types;
            for(auto & pe : prj->prexpr_) {
                new_types.push_back((int)pe.type + 2);
            }
            typv = new_types;
        } else if(op->type_ == qop_type::end) {
            break;
        }
        op = op->subscriber();
    }
}

/*
* Function initialisation at first access path
*/
void codegen_inline_visitor::init_function(BasicBlock *bb) {

    // create function entry block
    ctx.getBuilder().SetInsertPoint(bb);

    // allocate result counter and initialize with 0
    res_counter = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.LLVM_ZERO, res_counter);
    resAlloc = ctx.getBuilder().CreateAlloca(ctx.res_arr_type);

    // forward parameters
    qr_size = ConstantInt::get(ctx.int64Ty, 1);
    sizes.push_back(qr_size);

    // rhs loop counter, used in while_loop
    // intialize with UNKNOWN ID

    rhs_alloca = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.UNKNOWN_ID, rhs_alloca);
    strAlloc = ctx.getBuilder().CreateAlloca(ctx.int8PtrTy);

    // allocate space for lhs, used in while_loop
    lhs_alloca = ctx.getBuilder().CreateAlloca(ctx.int64Ty);

    // allocate space for pitem iteration
    cur_item_arr = ctx.getBuilder().CreateAlloca(ctx.pSetRawArrTy);
    cur_item = ctx.getBuilder().CreateAlloca(ctx.pitemTy);
    cur_pset = ctx.getBuilder().CreateAlloca(ctx.propertySetPtrTy);
    pitem = ctx.getBuilder().CreateAlloca(ctx.pitemTy);
    plist_id = ctx.getBuilder().CreateAlloca(ctx.int64Ty);

    new_res_arr_type = ArrayType::get(ctx.int64PtrTy, new_type_vec.size() + 1);
    newResAlloc = ctx.getBuilder().CreateAlloca(new_res_arr_type);

    // allocate space for each projection variable
    for(auto & i : pv)
        i = ctx.getBuilder().CreateAlloca(ctx.int64Ty);

    // create vector of projection keys
    project_keys.resize(project_string.size());
    int i = 0;
    for(auto & s : project_string) {
        auto prj_alloc = ctx.getBuilder().CreateAlloca(ctx.int8PtrTy);
        GlobalVariable *label = ctx.getBuilder().CreateGlobalString(StringRef(s), "pkey_"+s);
        auto str_ptr = ctx.getBuilder().CreateBitCast(label, ctx.int8PtrTy);
        ctx.getBuilder().CreateStore(str_ptr, prj_alloc);
        project_keys[i] = ctx.getBuilder().CreateLoad(prj_alloc);
        i++;
    }

    // allocate space for iteration variables
    max_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    max_cnt = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    ctx.getBuilder().CreateStore(ctx.MAX_PITEM_CNT, max_cnt);
    //cur_idx = ctx.getBuilder().CreateAlloca(ctx.int64Ty);
    loop_cnt = ctx.getBuilder().CreateAlloca(ctx.int64Ty);

    cross_join_idx.push_back(ctx.getBuilder().CreateAlloca(ctx.int64Ty));
    //cross_join_rship = ctx.getBuilder().CreateAlloca(ctx.rshipPtrTy);   
}

void codegen_inline_visitor::init_finish(BasicBlock *bb) {
    // create function entry block
    ctx.getBuilder().SetInsertPoint(bb);
}


AllocaInst *codegen_inline_visitor::insert_alloca(Type *ty) {
    //get and save current basic block
    auto cur_bb = ctx.getBuilder().GetInsertBlock();

    //get current IR function
    auto cur_fct = cur_bb->getParent();

    //get entry block of the current IR function
    auto init_bb = &cur_fct->getEntryBlock();

    // move to init basic block
    ctx.getBuilder().SetInsertPoint(init_bb);

    // check if block has terminator
    auto term = init_bb->getTerminator();
    if(term) {
        // unlink temrinator from basic block
        term->removeFromParent();
    } 

    // insert new alloca
    auto alloc = ctx.getBuilder().CreateAlloca(ty);

    // insert old terminator again
    if(term) {
        ctx.getBuilder().Insert(term);
    }

    // set insert point to saved basic block
    ctx.getBuilder().SetInsertPoint(cur_bb);

    return alloc;
}

/*
void codegen_inline_visitor::visit(std::shared_ptr<store_op> op) {
    auto obtain_mat_tuple = ctx.extern_func("obtain_mat_tuple");
    auto mat_node = ctx.extern_func("mat_node");
    auto mat_rship = ctx.extern_func("mat_rship");
    auto persist = ctx.extern_func("persist_tuple");
    auto mat_reg = ctx.extern_func("mat_reg_value");

    auto store_entry = BasicBlock::Create(ctx.getContext(), "store_entry", main_function);
    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(store_entry);
    ctx.getBuilder().SetInsertPoint(store_entry);

    auto tp = ctx.getBuilder().CreateCall(obtain_mat_tuple, {});
    for(auto & res : reg_query_results) {
        //auto type = ConstantInt::get(ctx.int64Ty, res.type);
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

    ctx.getBuilder().CreateCall(persist, {gdb, tp});
    prev_bb = store_entry;
}*/