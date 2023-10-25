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

#ifndef query_builder_hpp_
#define query_builder_hpp_

#include <initializer_list>

#include "graph_db.hpp"
#include "query_ctx.hpp"
#include "qop.hpp"
#include "query_pipeline.hpp"
#include "qop_aggregates.hpp"
#include "qop_analytics.hpp"

/**
 * The query_builder class allows to construct a plan for executing a graph query in dot
 * notation. The individual methods represent plan operators which can be
 * combined into a query. Example: auto q = query(graph) .all_nodes()
 *            .from_relationships()
 *            .to_node()
 *            .project({ PExpr_(0, builtin::int_property(res, "id")),
 *                       PExpr_(2, builtin::int_property(res, "id"))})
 *            .print();
 *   q.get_pipeline().start();
 * 
 * query_builder can also construct a query (batch) from multiple query_pipelines.
 */
class query_builder {
  friend class query_batch;
public:
  /**
   * Constructor for a query on the given graph database.
   */
  query_builder(query_ctx& ctx) : ctx_(ctx) {}

  // query_builder(query_ctx& ctx, qop_ptr qop);

  /**
   * Default destructor.
   */
  ~query_builder() = default;

  query_builder &operator=(const query_builder &);

  /**
   * Add a scan over all nodes (optionally with the given label = type).
   */
  query_builder &all_nodes(const std::string &label = "");
  query_builder &all_nodes(std::map<std::size_t, std::vector<std::size_t>> &range_map, const std::string &label = "");

  /**
   * Add a scan over all nodes with the label which satisfy the given predicate
   * on the property with the given key.
   */
  query_builder &nodes_where(const std::string &label, const std::string &key,
                     std::function<bool(const p_item &)> pred);
  
  /**
   * Add a scan over all nodes with any of the given labels which satisfy the given predicate
   * on the property with the given key. This is for entity objects belonging to the same
   * abstract entity (e.g. Post and Comment are sub-classes of Message)
   */
  query_builder &nodes_where(const std::vector<std::string> &labels, const std::string &key,
                     std::function<bool(const p_item &)> pred);

  /**
   * Add an index scan over nodes where the key is equal to the given value. 
   */
  query_builder &nodes_where_indexed(const std::string &label, const std::string &prop, uint64_t val);

  query_builder &nodes_where_indexed(const std::vector<std::string> &labels,
                              const std::string &prop, uint64_t val);

  /**
   * Add an operator that scans all incoming relationships of the last node in
   * the query result. Optionally, 1) the given label of the relationship is
   * checked, too. 2) Nodes that were already explored, i.e. other than the frontier,
   * can also be re-explored, given their position.
   */
  query_builder &to_relationships(const std::string &label = "",
                          int pos = std::numeric_limits<int>::max());
  query_builder &to_relationships(std::pair<int, int> range,
                          const std::string &label = "",
                          int pos = std::numeric_limits<int>::max());

  /**
   * Add an operator that scans all outgoing relationships of the last node in
   * the query result. Optionally, 1) the given label of the relationship is
   * checked, too. 2) Nodes that were already explored, i.e. other than the frontier,
   * can also be re-explored, given their position.
   */
  query_builder &from_relationships(const std::string &label = "",
                            int pos = std::numeric_limits<int>::max());

  query_builder &from_relationships(std::pair<int, int> range,
                            const std::string &label = "",
                            int pos = std::numeric_limits<int>::max());

  /**
   * Add an operator that scans all outgoing and incoming relationships of the last node in
   * the query result. Optionally, 1) the given label of the relationship is
   * checked, too. 2) Nodes that were already explored, i.e. other than the frontier,
   * can also be re-explored, given their position.
   */
  query_builder &all_relationships(const std::string &label = "",
                            int pos = std::numeric_limits<int>::max());

  query_builder &all_relationships(std::pair<int, int> range,
                            const std::string &label = "",
                            int pos = std::numeric_limits<int>::max());

  /**
   * Add a filter operator for checking that the property with the given key
   * satisfies the predicate. This predicate is applied to the last
   * node/relationship that was processed, i.e. in a pattern like
   * (n1:Node)-[r:Relationship]->(n2:Node) it refers to the node n2.
   */
  query_builder &property(const std::string &key,
                  std::function<bool(const p_item &)> pred);

  query_builder &filter(const expr &ex);

  /**
   * Add an operator the retrieves the node at the destination side of the
   * currently processed relationship with an optional filter for label(s).
   */
  query_builder &to_node(const std::string &label = "");

  query_builder &to_node(const std::vector<std::string> &labels);

  /**
   * Add an operator the retrieves the node at the source side of the
   * currently processed relationship with an optional filter for label(s).
   */
  query_builder &from_node(const std::string &label = "");

  query_builder &from_node(const std::vector<std::string> &labels);

  /**
   * Add a filter operator that checks whether the last node/relationship
   * in the result has the given label.
   */
  query_builder &has_label(const std::string &label);

  /**
   * Add a filter operator that checks whether the last node/relationship
   * in the result has one of the given labels.
   */
  query_builder &has_label(const std::vector<std::string> &labels);

  /**
   * Add a limit operator that produces only the first n result elements.
   */
  query_builder &limit(std::size_t n);


  /**
   * Add a projection operator that applies the given list of projection
   * functions to the query result.
   */
  query_builder &project(const projection::expr_list &exprs);

  query_builder &project(std::vector<projection_expr> prexpr);

  /**
   * Add an operator for sorting the results.
   */
  query_builder &orderby(std::function<bool(const qr_tuple &, const qr_tuple &)> cmp);

  /**
   * Add an operator for grouping and optional aggregation. The positions of the 
   * grouping keys in the query result tuple are specified by the positions in 
   * the vector pos. The aggregation function name(s) and the position(s) of the 
   * attribute(s) (in the tuple) to be aggregated are given as the vector of 
   * string-int pairs aggrs.
   */
  query_builder &groupby(const std::vector<group_by::group>& grps, const std::vector<group_by::expr>& exprs);

  query_builder &aggr(const std::vector<aggregate::expr>& exprs);

  /**
   * Add an operator for eliminating duplicates in result tuples.
   * Resulting tuples are distinct tuples.
   */
  query_builder &distinct();

  /**
   * Add an operator to filter projected result tuples based on the pred function.
   */
  query_builder &where_qr_tuple(std::function<bool(const qr_tuple &)> pred);

  /**
   * Add an operator to unions all the query tuples of the left query 
   * pipeline and the right query pipeline(s).
   */
  query_builder &union_all(query_pipeline &other);

  query_builder &union_all(std::initializer_list<query_pipeline *> queries);

  /**
   * Add an operator to count the number of tuples in the pipeline.
   */
  query_builder &count();

  /**
   * Add a print operator for outputting the query results to cout.
   */
  query_builder &print();

  /**
   * Add an operator for collecting the query results in the given result_set
   * object.
   */
  query_builder &collect(result_set &rs);

  /**
   * Ends a query pipeline.
   */
  query_builder &finish();

  /**
   * Add an operator for constructing the cartesian product of the query tuples 
   * of the left and right query pipelines.
   */
  query_builder &cross_join(query_pipeline &other);

  /**
   * Add a nested loop join operator for merging tuples of two
   * query pipelines if the node at a given position in the left tuple
   * is the same as the node at another given position in the right tuple.
   * The node positions are specified by the pos pair. 
   */
  query_builder &nested_loop_join(std::pair<int, int> left_right, query_pipeline &other);

  /**
   * Add a hash join operator for merging tuples of two
   * query pipelines if the node at a given position in the left tuple
   * is the same as the node at another given position in the right tuple.
   * The node positions are specified by the pos pair. 
   */
  query_builder &hash_join(std::pair<int, int> left_right, query_pipeline &other);

  /**
   * Add a left outer join operator for merging tuples of two queries based 
   * on the given join condition. Dangling tuples are padded with "null_val" 
   */
  query_builder &left_outer_join(query_pipeline &other, std::function<bool(const qr_tuple &, const qr_tuple &)> pred);

  /**
   * Add an operator to find the unweighted shortest path between the pair 
   * of nodes given their positions in the query tuple. Bidirectional 
   * search (i.e. via outgoing and incoming relationships) is optionally 
   * set using the flag, bidirectional.
   * rpred is a predicate for checking if a relationship is traversed.
   * The operator appends an array of IDs of the nodes along the shortest
   * path.
   * all_spaths specfies if all shortest path of equal distance are searched.
  */
  query_builder &algo_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
        rship_predicate rpred, bool bidirectional = false, bool all_spaths = false);

  /**
   * Add an operator to find the weighted shortest path between the pair 
   * of nodes given their positions in the query tuple. Bidirectional 
   * search (i.e. via outgoing and incoming relationships) is optionally 
   * set using the flag, bidirectional.
   * rpred is a predicate for checking if a relationship is traversed.
   * weight is a function that computes the weight of a relationship.
   * The operator appends the total weight of the shortest path to the
   * query tuple.
   * all_spaths specfies if all shortest path of equal weight are searched.
  */
  query_builder &algo_weighted_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
        rship_predicate rpred, rship_weight weight, bool bidirectional = false,
        bool all_spaths = false);

  /**
   * Add an operator to find the top k weighted shortest path between the pair 
   * of nodes given their positions in the query tuple. Bidirectional 
   * search (i.e. via outgoing and incoming relationships) is optionally 
   * set using the flag, bidirectional.
   * rpred is a predicate for checking if a relationship is traversed.
   * weight is a function that computes the weight of a relationship.
   * The operator appends the total weight of the shortest path to the
   * query tuple.
  */
  query_builder &algo_k_weighted_shortest_path(std::pair<std::size_t, std::size_t> start_stop,
      std::size_t k, rship_predicate rpred, rship_weight weight, bool bidirectional = false);

  /**
   * Add an operator for invoking a LUA function as part of the query.
   */
  /* query_builder &call_lua(const std::string &proc_name,
                  const std::vector<std::size_t> &params);
   */

  /*-------------------------------------------------------------------*/

  /**
   * Add an operator for creating a node.
   */
  query_builder &create(const std::string &label, const properties_t &props);

  /**
   * Add an operator for creating a relationship that connects two nodes 
   * at any given positions in the result.
   */
  query_builder &create_rship(std::pair<int, int> src_des, const std::string &label,
                        const properties_t &props);

  /**
   *  Add an operator for updating a node.
   */
  query_builder &update(std::size_t var, properties_t &props);

  /**
   * Add an operator for deleting the last node in a query tuple.
   * All relationship objects connected to the node are also deleted.
   * The optional pos specifies a node to be deleted at other
   * positions in the tuple.
   */
  query_builder &delete_detach(const std::size_t pos = std::numeric_limits<std::size_t>::max());

  /**
   * Add an operator for deleting the last node in a query tuple.
   * The optional pos specifies a node to be deleted at other
   * positions in the tuple.
   */
  query_builder &delete_node(const std::size_t pos = std::numeric_limits<std::size_t>::max());

  /**
   * Add an operator for deleting the last relationship in a query tuple.
   * The optional pos specifies a relationship to be deleted at other
   * positions in the tuple.
   */
  query_builder &delete_rship(const std::size_t pos = std::numeric_limits<std::size_t>::max());

  /*-------------------------------------------------------------------*/

  query_pipeline& get_pipeline() { return qpipeline_; }

private:
  query_pipeline qpipeline_;
  query_ctx& ctx_;
};


#endif