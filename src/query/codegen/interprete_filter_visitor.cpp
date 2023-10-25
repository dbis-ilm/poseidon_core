#include "interprete_filter_visitor.h"

interprete_filter_visitor::interprete_filter_visitor(graph_db_ptr gdb)  : gdb_(gdb){
}


interprete_filter_visitor::interprete_filter_visitor(graph_db_ptr gdb, arg_builder & args)  : gdb_(gdb), args_(args) {
}


void interprete_filter_visitor::visit(int rank, std::shared_ptr<number_token> num) {
    val_type_ = value_type::intv;
    //int_value_ = num->value_;
    int_value_ = args_.int_args[2];
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<key_token> key) {
    key_ = key->key_;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<str_token> str) {
    dcode_t dc = gdb_->get_code(str->str_);
    val_type_ = value_type::dcodev;
    dict_value_ = dc;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<time_token> time) {

}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<fct_call> fct) {
    //TODO: implement
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<func_call> fct) {
    //TODO: implement
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<eq_predicate> eq) {
    pred_ = predicate::eq;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<le_predicate> eq) {
    pred_ = predicate::le;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<lt_predicate> eq) {
    pred_ = predicate::lt;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<ge_predicate> eq) {
    pred_ = predicate::ge;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<gt_predicate> eq) {
    pred_ = predicate::gt;
}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<and_predicate> andpr) {

}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<or_predicate> orpr) {

}

void interprete_filter_visitor::visit(int rank, std::shared_ptr<call_predicate> orpr) {
    
}

std::function<bool(const p_item &)> interprete_filter_visitor::get_pred() {
    switch(val_type_) {
        case value_type::dcodev:
            return [&](const p_item &p) { return p.equal(dict_value_); };
        case value_type::intv:
            return [&](const p_item &p) { return p.equal(int_value_); };
        case value_type::fpv:
        case value_type::uiv:
        default:
            assert(false && "not implemented yet");
            return [&](const p_item &p) { return p.equal(int_value_); };
    }
}
