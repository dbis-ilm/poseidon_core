#include "codegen.hpp"

void codegen_inline_visitor::visit(std::shared_ptr<node_has_label> op) { 
    BasicBlock *has_label_entry = BasicBlock::Create(ctx.getContext(), "node_has_label_entry", cur_pipeline);
    BasicBlock *consume = BasicBlock::Create(ctx.getContext(), "node_has_label_consume", cur_pipeline);

    ctx.getBuilder().SetInsertPoint(prev_bb);
    ctx.getBuilder().CreateBr(has_label_entry);
    ctx.getBuilder().SetInsertPoint(has_label_entry);

    auto node = reg_query_results.back().reg_val;
    auto label_code = ctx.extract_arg_label(op->operator_id_, gdb, queryArgs);
    auto cond = ctx.node_cmp_label(node, label_code);

    ctx.getBuilder().CreateCondBr(cond, consume, main_return);

    ctx.getBuilder().SetInsertPoint(consume);
    
    prev_bb = consume;
} 