/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef query_hpp_
#define query_hpp_

#include <initializer_list>

#include "graph_db.hpp"
#include "qop.hpp"

/**
 * The query class allows to construct a plan for executing a graph query in dot
 * notation. The individual methods represent plan operators which can be
 * combined into a query. Example: auto q = query(graph) .all_nodes()
 *            .from_relationships()
 *            .to_node()
 *            .project({ PExpr_(0, builtin::int_property(res, "id")),
 *                       PExpr_(2, builtin::int_property(res, "id"))})
 *            .print();
 *   q.start();
 */
class query {
public:
  /**
   * Constructor for a query on the given graph database.
   */
  query(graph_db_ptr gdb) : graph_db_(gdb) {}

  /**
   * Default destructor.
   */
  ~query() = default;

  query &operator=(const query &) = default;

  /**
   * Add a scan over all nodes (optionally with the given label = type).
   */
  query &all_nodes(const std::string &label = "");

  /**
   * Add a scan over all nodes with the label which satisfy the given predicate
   * on the property with the given key.
   */
  query &nodes_where(const std::string &label, const std::string &key,
                     std::function<bool(const p_item &)> pred);
  /**
   * Add an operator that scans all incoming relationships of the last node in
   * the query result. Optionally, the given label of the relationship is
   * checked, too.
   */
  query &to_relationships(const std::string &label = "");
  query &to_relationships(std::pair<int, int> range,
                          const std::string &label = "");

  /**
   * Add an operator that scans all outgoing relationships of the last node in
   * the query result. Optionally, the given label of the relationship is
   * checked, too.
   */
  query &from_relationships(const std::string &label = "");

  query &from_relationships(std::pair<int, int> range,
                            const std::string &label = "");

  /**
   * Add a filter operator for checking that the property with the given key
   * satisfies the predicate. This predicate is applied to the last
   * node/relationship that was processed, i.e. in a pattern like
   * (n1:Node)-[r:Relationship]->(n2:Node) it refers to the node n2.
   */
  query &property(const std::string &key,
                  std::function<bool(const p_item &)> pred);

  /**
   * Add an operator the retrieves the node at the destination side of the
   * currently processed relationship with an optional filter for label.
   */
  query &to_node(const std::string &label = "");

  /**
   * Add an operator the retrieves the node at the source side of the
   * currently processed relationship with an optional filter for label.
   */
  query &from_node(const std::string &label = "");

  /**
   * Add a filter operator that checks whether the last node/relationship
   * in the result has the given label.
   */
  query &has_label(const std::string &label);

  /**
   * Add a limit operator that produces only the first n result elements.
   */
  query &limit(std::size_t n);

  /**
   * Add a projection operator that applies the given list of projection
   * functions to the query result.
   */
  query &project(const projection::expr_list &exprs);

  /**
   * Add an operator for sorting the results.
   */
  query &orderby(std::function<bool(const qr_tuple &, const qr_tuple &)> cmp);

  /**
   * Add a print operator for outputting the query results to cout.
   */
  query &print();

  /**
   * Add an operator for collecting the query results in the given result_set
   * object.
   */
  query &collect(result_set &rs);

  /**
   * TODO
   */
  query &crossjoin(query &other);

  /**
   * Add a left outerjoin operator for merging tuples of two 
   * queries if there exists a relationship defined by an object
   * (at a given position) in the left tuple as the source node 
   * and an object (at a given position) in the right tuple as 
   * the destination node 
   */
  query &outerjoin(std::pair<int, int> src_des, query &other);

  /**
   * Add an operator for invoking a LUA function as part of the query.
   */
  /* query &call_lua(const std::string &proc_name,
                  const std::vector<std::size_t> &params);
   */

  /*-------------------------------------------------------------------*/

  /**
   * Add an operator for creating a node.
   */
  query &create(const std::string &label, const properties_t &props);

  /**
   * Add an operator for creating a relationship that connects two nodes 
   * at any given positions in the result.
   */
  query &create_rship(std::pair<int, int> src_des, const std::string &label, const properties_t &props);

  /**
   *  Add an operator for updating a node.
   */
  query &update(std::size_t var, properties_t &props);

  /*-------------------------------------------------------------------*/

  /**
   * Start the execution of the query.
   */
  void start();

  /**
   * Print the query plan.
   */
  void dump(std::ostream &os = std::cout);

  static void start(std::initializer_list<query *> queries);

  /**
   * Return the pointer to the graph database.
   */
  graph_db_ptr &get_graph_db() { return graph_db_; }

private:
  query &append_op(qop_ptr op, qop::consume_func cf, qop::finish_func ff);
  query &append_op(qop_ptr op, qop::consume_func cf);

  qop_ptr plan_head_, plan_tail_;
  graph_db_ptr graph_db_;
};

/**
 * A query set combines multiple queries producing a joint result. This is
 * necessary because we use a push-based approach where each scan is represented
 * by a separate query object.
 */
class query_set {
public:
  query_set() = default;

  void add(query &q) { queries_.push_back(q); }
  std::size_t size() const { return queries_.size(); }
  query& front() { return queries_.front(); }
  query &at(std::size_t i) { return queries_[i];  }
  bool empty() const { return queries_.empty(); }
  /**
   * Start the execution of the query.
   */
  void start();

  /**
   * Print the query plan.
   */
  void dump(std::ostream &os = std::cout);

private:
  std::vector<query> queries_;
};

#endif
