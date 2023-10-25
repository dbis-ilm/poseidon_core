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

#ifndef query_pipeline_hpp_
#define query_pipeline_hpp_

#include "qop.hpp"

/**
 * A query_pipeline is a sequence of query operators, typically starting 
 * with a scan and ending with a pipeline breaker or the end of the query.
 * A set of query_pipeline objects form a query_batch which represents
 * a complete query in Poseidon.
 * Instances of query_pipeline are constructed either from the parser
 * or the query_builder.
 */

class query_pipeline {
    friend class query_builder;
    friend class query_batch;
    friend struct query_ctx;

public:
    query_pipeline() = default;

    query_pipeline(qop_ptr qop);

    ~query_pipeline() = default;

  query_pipeline& operator=(const query_pipeline &);

  /**
   * Start the execution of the query pipeline.
   */
  void start(query_ctx& ctx);

  /**
   * Print the query plan.
   */
  void print_plan(std::ostream& os = std::cout);

  void extract_args();

  qop_ptr &plan_head() { return plan_head_; }

private:
  query_pipeline &append_op(qop_ptr op, qop::consume_func cf, qop::finish_func ff);
  query_pipeline &append_op(qop_ptr op, qop::consume_func cf);

  qop_ptr plan_head_, plan_tail_;
};

#endif