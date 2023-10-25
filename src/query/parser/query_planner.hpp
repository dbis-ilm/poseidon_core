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
#ifndef query_planner_hpp_
#define query_planner_hpp_

#include "query_batch.hpp"
#include "query_ctx.hpp"

#include <fmt/format.h>

#include <boost/dll/import.hpp> 

#include "antlr4-runtime.h"
#include "antlr4_generated/poseidonLexer.h"
#include "antlr4_generated/poseidonParser.h"
#include "antlr4_generated/poseidonBaseVisitor.h"

class query_planner : public poseidonBaseVisitor {
public:
    query_planner(query_ctx& ctx) : qctx_(ctx) {}

    query_batch get_query_plan();

   void add_udf_library(std::shared_ptr<boost::dll::shared_library> udf_lib) { udf_lib_ = udf_lib; }

    std::any visitQuery(poseidonParser::QueryContext *ctx) override;
    std::any visitNode_scan_op(poseidonParser::Node_scan_opContext *ctx) override;
    std::any visitIndex_scan_op(poseidonParser::Index_scan_opContext *ctx) override;
    std::any visitProject_op(poseidonParser::Project_opContext *ctx) override;
    std::any visitFilter_op(poseidonParser::Filter_opContext *ctx) override;
    std::any visitLimit_op(poseidonParser::Limit_opContext *ctx) override;
    std::any visitExpand_op(poseidonParser::Expand_opContext *ctx) override;
    std::any visitSort_op(poseidonParser::Sort_opContext *ctx) override;
    std::any visitForeach_relationship_op(poseidonParser::Foreach_relationship_opContext *ctx) override;
    std::any visitCrossjoin_op(poseidonParser::Crossjoin_opContext *ctx) override;
    std::any visitLeftouterjoin_op(poseidonParser::Leftouterjoin_opContext *ctx) override;
     // std::any visitHashjoin_op(poseidonParser::Hashjoin_opContext *ctx) override;
    std::any visitAggregate_op(poseidonParser::Aggregate_opContext *ctx) override;
    std::any visitUnion_op(poseidonParser::Union_opContext *ctx) override;
    std::any visitGroup_by_op(poseidonParser::Group_by_opContext *ctx) override;
    
    std::any visitMatch_op(poseidonParser::Match_opContext *ctx) override;
    std::any visitPath_pattern(poseidonParser::Path_patternContext *ctx) override;
    std::any visitNode_pattern(poseidonParser::Node_patternContext *ctx) override;
 
    std::any visitCreate_op(poseidonParser::Create_opContext *ctx) override;
    std::any visitCreate_node(poseidonParser::Create_nodeContext *ctx) override;
    std::any visitCreate_rship(poseidonParser::Create_rshipContext *ctx) override;
    std::any visitProperty_list(poseidonParser::Property_listContext *ctx) override;
  
    std::any visitLogical_expr(poseidonParser::Logical_exprContext *ctx) override;
    std::any visitBoolean_expr(poseidonParser::Boolean_exprContext *ctx) override;
    std::any visitEquality_expr(poseidonParser::Equality_exprContext *ctx) override;
    std::any visitRelational_expr(poseidonParser::Relational_exprContext *ctx) override;
    std::any visitAdditive_expr(poseidonParser::Additive_exprContext *ctx) override;
    std::any visitMultiplicative_expr(poseidonParser::Multiplicative_exprContext *ctx) override;
    std::any visitUnary_expr(poseidonParser::Unary_exprContext *ctx) override;
    std::any visitPrimary_expr(poseidonParser::Primary_exprContext *ctx) override;

private:
  query_ctx qctx_;
  std::vector<qop_ptr> sources_;
  std::shared_ptr<boost::dll::shared_library> udf_lib_;

  /**
   * Returns the variable number from the variable name, 
   * e.g. $0.Id -> 0.
   */
  static uint32_t extract_tuple_id(const std::string& var_name);

  static std::string trim_string(const std::string& s);

  static expr property_list_to_expr(properties_t& plist);

  template <typename T>
  qop_ptr qop_append(qop_ptr parent, std::shared_ptr<T> qop) { 
    if (parent != nullptr)
      parent->connect(qop, std::bind(&T::process, dynamic_cast<T *>(qop.get()), ph::_1, ph::_2));
    return qop;
  }

    template <typename T>
  qop_ptr qop_append2(qop_ptr parent, std::shared_ptr<T> qop) { 
    if (parent != nullptr)
      parent->connect(qop, 
        std::bind(&T::process, dynamic_cast<T *>(qop.get()), ph::_1, ph::_2), 
        std::bind(&T::finish, dynamic_cast<T *>(qop.get()), ph::_1));
    return qop;
  }
  
    query_batch qplan_;
};

#endif