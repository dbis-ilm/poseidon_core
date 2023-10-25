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
#ifndef query_ctx_hpp_
#define query_ctx_hpp_

#include "defs.hpp"
#include "graph_db.hpp"

class query_pipeline;

/**
 * query_ctx represents the query context which consists of a pointer to the underlying graph
 * database (graph_db_ptr) and implementations of the basic query execution functions on the
 * graph database.
 */
struct query_ctx {
  graph_db_ptr gdb_; /// the pointer to the graph database (storage engine)

  /**
   * Constructors.
   */
  query_ctx() = default;
  query_ctx(query_ctx& ctx) : gdb_(ctx.gdb_) {  }
  query_ctx(graph_db_ptr& gdb) : gdb_(gdb) {  }

  /**
   * Destructor.
  */
  ~query_ctx();

  using node_consumer_func = std::function<void(node &)>;
  using rship_consumer_func = std::function<void(relationship &)>;

  /* -------------- transaction management -------------- */

  /**
   * Starts a new transaction. This transaction is associated with the 
   * current thread and stored there as thread_local property.
   */
  void begin_transaction() { gdb_->begin_transaction(); }

  /**
   * Commits the currently active transaction associated with this thread.
   */
  bool commit_transaction() { return gdb_->commit_transaction(); }

  /**
   * Aborts the currently active transaction associated with this thread.
   */
  bool abort_transaction() { return gdb_->abort_transaction(); }

  /**
   * Encapsulated code for execution a transaction. If body returns true then
   * the transaction is committed, otherwise the transaction is aborted.
   */
  bool run_transaction(std::function<bool()> body) { return gdb_->run_transaction(body); }

  /* ---------------- query support ---------------- */

  /**
   * Scans all nodes of the graph with the given label and invokes for each of
   * these nodes the consumer function.
   */
  void nodes_by_label(const std::string &label, node_consumer_func consumer);

  static void _nodes_by_label(graph_db *gdb, const std::string &label, node_consumer_func consumer);

  /**
   * Scans all nodes of the graph with any of the given labels and invokes for each of
   * these nodes the consumer function. This is for entity objects belonging to the same
   * abstract entity (e.g. Post and Comment are sub-classes of Message)
   */
  void nodes_by_label(const std::vector<std::string> &labels, node_consumer_func consumer);

  /**
   * Scans all nodes of the graph and invokes for each nodes the given consumer
   * function.
   */
  void nodes(node_consumer_func consumer);

  /**
   * Scans all nodes of the graph and invokes for each nodes the given consumer
   * function. The scan is performed in parallel by multiple threads via a
   * thread pool.
   */
  void parallel_nodes(node_consumer_func consumer);

  void parallel_nodes(const std::string &label, node_consumer_func consumer);

  /**
   * Scans all nodes which satisfy the given predicate on the property with
   * label pkey and invokes for each of these nodes the consumer function.
   */
  void nodes_where(const std::string &pkey, p_item::predicate_func pred,
                   node_consumer_func consumer);

  /**
   * Scans all relationships of the graph with the given label and invokes for
   * each of these relationship the consumer function.
   */
  void relationships_by_label(const std::string &label,
                              rship_consumer_func consumer);

  /**
   * Scans all FROM relationships recursivley starting from the the given node
   * and invokes for each of these relationships the consumer function. The
   * parameters min and max determine the minimum and maximum number of hops.
   */
  void foreach_variable_from_relationship_of_node(const node &n,
                                                  std::size_t min,
                                                  std::size_t max,
                                                  rship_consumer_func consumer);

  /**
   * Scans all FROM relationships with the given label code recursivley starting
   * from the the given node and invokes for each of these relationships the
   * consumer function. The parameters min and max determine the minimum and
   * maximum number of hops.
   */
  void foreach_variable_from_relationship_of_node(const node &n, dcode_t lcode,
                                                  std::size_t min,
                                                  std::size_t max,
                                                  rship_consumer_func consumer);

  /**
   * Scans all FROM relationships of the the given node and invokes for each of
   * these relationships the consumer function.
   */
  void foreach_from_relationship_of_node(const node &n,
                                         rship_consumer_func consumer);

  /**
   * Scans all TO relationships of the the given node and invokes for each of
   * these relationships the consumer function.
   */
  void foreach_to_relationship_of_node(const node &n,
                                       rship_consumer_func consumer);

  /**
   * Iterates over all FROM relationships of node n with the given label
   * and invokes for each of these relationships the consumer function.
   */
  void foreach_from_relationship_of_node(const node &n,
                                         const std::string &label,
                                         rship_consumer_func consumer);

  /**
   * Iterates over all FROM relationships of node n with the given label code
   * and invokes for each of these relationships the consumer function.
   */
  void foreach_from_relationship_of_node(const node &n, dcode_t lcode,
                                         rship_consumer_func consumer);

  /**
   * Iterates over all TO relationships of node n with the given label
   * and invokes for each of these relationships the consumer function.
   */
  void foreach_to_relationship_of_node(const node &n, const std::string &label,
                                       rship_consumer_func consumer);

  /**
   * Scans all TO relationships recursivley ending at the the given node
   * and invokes for each of these relationships the consumer function. The
   * parameters min and max determine the minimum and maximum number of hops.
   */
  void foreach_variable_to_relationship_of_node(const node &n, std::size_t min,
                                                std::size_t max,
                                                rship_consumer_func consumer);

  /**
   * Scans all TO relationships with the given label code recursivley ending at
   * the the given node and invokes for each of these relationships the consumer
   * function. The parameters min and max determine the minimum and maximum
   * number of hops.
   */
  void foreach_variable_to_relationship_of_node(const node &n, dcode_t lcode,
                                                std::size_t min,
                                                std::size_t max,
                                                rship_consumer_func consumer);

  /**
   * Iterates over all TO relationships of node n with the given label code
   * and invokes for each of these relationships the consumer function.
   */
  void foreach_to_relationship_of_node(const node &n, dcode_t lcode,
                                       rship_consumer_func consumer);

  /**
   * Checks whether the property with name pkey of the given node satisfies the
   * predicate.
   */
  bool is_node_property(const node &n, const std::string &pkey,
                        p_item::predicate_func pred);

  /**
   * Checks whether the property with encoded name pcode of the given node
   * satisfies the predicate.
   */
  bool is_node_property(const node &n, dcode_t pcode,
                        p_item::predicate_func pred);

  /**
   * Checks whether the property with name pkey of the given relationship
   * satisfies the predicate.
   */
  bool is_relationship_property(const relationship &r, const std::string &pkey,
                                p_item::predicate_func pred);

  /**
   * Checks whether the property with the encoded name of the given relationship
   * satisfies the predicate.
   */
  bool is_relationship_property(const relationship &r, dcode_t pcode,
                                p_item::predicate_func pred);


  static void start(query_ctx& qctx, std::initializer_list<query_pipeline *> queries);
  static void print_plans(std::initializer_list<query_pipeline *> queries, std::ostream& os = std::cout);

};


struct scan_task {
  using range = std::pair<std::size_t, std::size_t>;
  scan_task(graph_db_ptr gdb, std::size_t first, std::size_t last,
	    query_ctx::node_consumer_func c, transaction_ptr tp = nullptr, std::size_t start_pos = 0);

  void operator()();

  static void scan(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, query_ctx::node_consumer_func consumer);

  static std::function<void(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, 
    query_ctx::node_consumer_func consumer)> callee_;

  graph_db_ptr graph_db_;
  range range_;
  query_ctx::node_consumer_func consumer_;
  transaction_ptr tx_;
  std::size_t start_pos_;
};

struct scan_task_with_label {
  using range = std::pair<std::size_t, std::size_t>;
  scan_task_with_label(graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
	    query_ctx::node_consumer_func c, transaction_ptr tp = nullptr, std::size_t start_pos = 0);

  void operator()();

  static void scan(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
    query_ctx::node_consumer_func consumer);

  static std::function<void(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
    query_ctx::node_consumer_func consumer)> callee_;

  graph_db_ptr graph_db_;
  range range_;
  dcode_t label_;
  query_ctx::node_consumer_func consumer_;
  transaction_ptr tx_;
  std::size_t start_pos_;
};

#endif