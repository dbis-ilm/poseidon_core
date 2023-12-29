/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#include "fmt/format.h"

#include "query_proc.hpp"
#include "query_planner.hpp"

#include "antlr4-runtime.h"
#include "antlr4_generated/poseidonLexer.h"
#include "antlr4_generated/poseidonParser.h"
#include "antlr4_generated/poseidonBaseVisitor.h"

#include "qop.hpp"
#include "qop_joins.hpp"
#include "func_call_expr.hpp"

class LexerErrorListener : public antlr4::BaseErrorListener {
public:
  LexerErrorListener() = default;

  virtual void syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *offendingSymbol, size_t line, size_t charPositionInLine,
                           const std::string &msg, std::exception_ptr e) override;
};

void LexerErrorListener::syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *offendingSymbol, size_t line, size_t charPositionInLine,
                           const std::string &msg, std::exception_ptr e) {
    spdlog::info("LexerErrorListener::syntaxError");
}

class ParserErrorListener : public antlr4::BaseErrorListener {
public:
  ParserErrorListener() = default;

  virtual void syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *offendingSymbol, size_t line, size_t charPositionInLine,
                           const std::string &msg, std::exception_ptr e) override;
};

void ParserErrorListener::syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *offendingSymbol, size_t line, size_t charPositionInLine,
                           const std::string &msg, std::exception_ptr e) {
    std::string mstr = fmt::format("syntax error at line {}:{}: {}", line, charPositionInLine, msg);
    throw query_processing_error(mstr);
}

query_proc::query_proc(query_ctx &ctx) : qctx_(ctx)
#ifdef USE_LLVM2
, compiler_(ctx) 
#endif
{}

bool query_proc::parse_(const std::string &query) {
    antlr4::ANTLRInputStream input(query);
    poseidonLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    poseidonParser parser(&tokens);  
    parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());

    try {
        poseidonParser::QueryContext* tree = parser.query();
        return tree != nullptr;
    } catch (std::exception& exc) {
        return false;
    }
}

qresult_iterator query_proc::execute_query(query_proc::mode m, const std::string& qstr, bool print_plan) {
    auto qplan = prepare_query(qstr);
    result_set result;
    qplan.append_collect(result);
    prepare_plan(qplan);

    if (m == Interpret) {
        //if (print_plan)
        //    qplan.print_plan();
        interp_query(qplan);
        if (print_plan)
            qplan.print_plan();
    }
    else {
        // TODO: compile & execute query
        // qplan.print_plan();
        compile_query(qplan);
        if (print_plan)
            qplan.print_plan();
    }
    return qresult_iterator(std::move(result));
}

std::size_t query_proc::execute_and_output_query(mode m, const std::string& qstr, bool print_plan) {
    auto qplan = prepare_query(qstr);
    qplan.append_printer(); 
    prepare_plan(qplan);

    if (m == Interpret) {
        // qplan.print_plan();
        interp_query(qplan);
        if (print_plan)
            qplan.print_plan();
    }
    else {
        // TODO: compile & execute query
        compile_query(qplan);
        if (print_plan)
            qplan.print_plan();
    }
    return 0; // qplan.result_size();    
}

query_batch query_proc::prepare_query(const std::string &query) {
    antlr4::ANTLRInputStream input(query);
    poseidonLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    poseidonParser parser(&tokens);   
    LexerErrorListener lexerErrorListener;
    ParserErrorListener parserErrorListener;
    // parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
    lexer.removeErrorListeners();
    lexer.addErrorListener(&lexerErrorListener);

    parser.removeParseListeners();
    parser.removeErrorListeners();
    parser.addErrorListener(&parserErrorListener);

    poseidonParser::QueryContext* tree = parser.query();
    
    query_planner visitor(qctx_);
    if (udf_lib_ && udf_lib_->is_loaded())
        visitor.add_udf_library(udf_lib_);

    visitor.visitQuery(tree);
    return visitor.get_query_plan();  
}

void query_proc::interp_query(query_batch& plan) {
    interp_.execute(qctx_, plan);
}

void query_proc::compile_query(query_batch& plan) {
#ifdef USE_LLVM2
    compiler_.execute(plan);    
#else
    abort();
#endif
}

void query_proc::abort_transaction() {
    qctx_.abort_transaction();
}

void query_proc::abort_query() {
    spdlog::info("abort running query...");
    // TODO
}

bool query_proc::load_library(const std::string& lib_path) {
    udf_lib_ = std::make_shared<boost::dll::shared_library>(lib_path);
    if (udf_lib_->is_loaded()) {
        return true;
    }
    return false;
}

class prepare_expr_visitor : public expression_visitor {
public:
    prepare_expr_visitor(query_ctx& ctx, std::shared_ptr<boost::dll::shared_library> udf_lib) : 
        udf_lib_(udf_lib) {}
    ~prepare_expr_visitor() = default;

    void visit(int rank, std::shared_ptr<func_call> op) override {
        // std::cout << "prepare func_call: " << op->func_name_ << " : " << op->param_list_.size() << std::endl;     
        if (op->param_list_.size() == 1) {          
            op->func1_ptr_ = udf_lib_->get<query_result(query_ctx&, query_result&)>(op->func_name_);
        } 
        else if (op->param_list_.size() == 2) {
            op->func2_ptr_ = udf_lib_->get<query_result(query_ctx&, query_result&, query_result&)>(op->func_name_);
        }
    }
private:
    std::shared_ptr<boost::dll::shared_library> udf_lib_;
};

class prepare_plan_visitor : public qop_visitor {
public:
    prepare_plan_visitor(query_ctx& ctx, std::shared_ptr<boost::dll::shared_library> udf_lib) : 
        expr_visitor_(ctx, udf_lib) {}

    ~prepare_plan_visitor() = default;

    void visit(std::shared_ptr<filter_tuple> op) override {
        auto ex = op->get_expression();
        if (ex) {
            ex->accept(0, expr_visitor_);
        }
    }

    void visit(std::shared_ptr<left_outer_join_op> op) override { 
        auto ex = op->get_expression();
        if (ex) {
            ex->accept(0, expr_visitor_);
        }
    }

   private:
        prepare_expr_visitor expr_visitor_;
};

 
void query_proc::prepare_plan(query_batch& qplan) {
    prepare_plan_visitor visitor(qctx_, udf_lib_);
    qplan.accept(visitor);
}
