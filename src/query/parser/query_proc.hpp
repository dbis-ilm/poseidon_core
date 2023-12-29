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
#ifndef query_proc_hpp_
#define query_proc_hpp_

#include <any>
#include <string>

#include <boost/dll/import.hpp>

#include "query_ctx.hpp"
#include "query_batch.hpp"

#ifdef USE_LLVM2
#include "qcompiler.hpp"
#endif
#include "qinterp.hpp"
#include "qresult_iterator.hpp"

/**
 * query_proc is the main entry class for the Poseidon query processor. It
 * encapsulates the parser, the query planner as well as the query compiler and
 * interpreter.
 */
class query_proc {
public:
  enum mode { Interpret, Compile, Adaptive };

  query_proc(query_ctx &ctx);
  ~query_proc() = default;

  bool parse_(const std::string &query);

  query_batch prepare_query(const std::string &query);

  qresult_iterator execute_query(mode m, const std::string &qstr,
                                 bool print_plan = false);

  std::size_t execute_and_output_query(mode m, const std::string &qstr,
                                       bool print_plan = false);

  void interp_query(query_batch &plan);
  void compile_query(query_batch &plan);
  
  void abort_query();

  void abort_transaction();
  
  bool load_library(const std::string &lib_path);

private:
  void prepare_plan(query_batch &qplan);

  query_ctx& qctx_;
  qinterp interp_;
#ifdef USE_LLVM2
  qcompiler compiler_;
#endif
  std::shared_ptr<boost::dll::shared_library> udf_lib_;
};

#endif