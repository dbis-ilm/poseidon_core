/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef graph_db_hpp_
#define graph_db_hpp_

#include <boost/any.hpp>
#include <map>
#include <mutex>
#include <string>

#include "dict.hpp"
#include "exceptions.hpp"
#include "nodes.hpp"
#include "properties.hpp"
#include "relationships.hpp"
#include "transaction.hpp"

/**
 * graph_db represents a graph consisting of nodes and relationships with
 * properties stored in a database. The class provides methods for constructing
 * and querying the graph.
 */
class graph_db {
public:
  /**
   * mapping_t is used during importing data from CSV files to map node names to
   * internal ids which are required for creating relationships.
   */
  using mapping_t = std::unordered_map<std::string, node::id_t>;

  using node_consumer_func = std::function<void(node &)>;
  using rship_consumer_func = std::function<void(relationship &)>;

  /**
   * Constructor for a new empty graph database.
   */
  graph_db(const std::string &db_name = "");

  /**
   * Destructor.
   */
  ~graph_db();

  /* -------------- transaction management -------------- */

  /**
   * Starts a new transaction and returns a pointer to this transaction.
   * This transaction is associated with the current thread and stored there
   * as thread_local property.
   */
  transaction_ptr begin_transaction();

  /**
   * Commits the currently active transaction associated with this thread.
   */
  bool commit_transaction();

  /**
   * Aborts the currently active transaction associated with this thread.
   */
  bool abort_transaction();

  /**
   * Performs initialization steps after starting the database.
   */
  void runtime_initialize();

  /* ---------------- graph construction ---------------- */

  /**
   * Add a new node to the graph with the given label (type) and the
   * set of properties (key-value pairs). The method returns the node identifier
   * which can be used to create a relationship from or to this node.
   */
  node::id_t add_node(const std::string &label, const properties_t &props,
                      bool append_only = false);

  /**
   * Add a new node to the graph with the given label (type) and the
   * set of properties (key-value pairs) without transactional support.
   * The method returns the node identifier which can be used to create a
   * relationship from or to this node.
   */
  node::id_t import_node(const std::string &label, const properties_t &props);

  /**
   * Add a new relationship to the graph that connects from_node and to_node.
   * This relationship has initialized with the given label and properties.
   */
  relationship::id_t add_relationship(node::id_t from_node, node::id_t to_node,
                                      const std::string &label,
                                      const properties_t &props,
                                      bool append_only = false);

  /**
   * Add a new relationship to the graph that connects from_node and to_node
   * without transactional support. This relationship has initialized with the
   * given label and properties.
   */
  relationship::id_t import_relationship(node::id_t from_node,
                                         node::id_t to_node,
                                         const std::string &label,
                                         const properties_t &props);

  /**
   * Returns a description of the node, i.e. with all decoded labels and
   * properties.
   */
  node_description get_node_description(const node &n);

  /**
   * Returns a description of the relationship, i.e. with all decoded labels and
   * properties.
   */
  rship_description get_rship_description(const relationship &r);

  /**
   * Returns the decoded label of the relationship.
   */
  const char *get_relationship_label(const relationship &r);

  /**
   * Returns the node identified by the given id.
   */
  node &node_by_id(node::id_t id);

  /**
   * Returns the relationship identified by the given id.
   */
  relationship &rship_by_id(relationship::id_t id);

  /* --------------- graph updates --------------- */
  /**
   * Updates the given node by changing the given
   * properties and replacing the label with the given one.
   */
  void update_node(node &n, const properties_t &props,
                   const std::string &label = "");

  /**
   * Updates the given relationship by changing the given
   * properties and replacing the label with the given one.
   */
  void update_relationship(relationship &r, const properties_t &props,
                           const std::string &label = "");

  /* ---------------- data import ---------------- */

  /**
   * Read the list of nodes from the given CSV file. The file is in neo4j
   * format with the given delimiter.
   */
  std::size_t import_nodes_from_csv(const std::string &label,
                                    const std::string &filename, char delim,
                                    mapping_t &m);

  /**
   * Read the list of relationships from the given CSV file. The file is in
   * neo4j format with the given delimiter.
   */
  std::size_t import_relationships_from_csv(const std::string &filename,
                                            char delim, const mapping_t &m);

  /* ---------------- helper ---------------- */

  /**
   * Returns a reference to the dictionary of string codes.
   */
  p_ptr<dict> &get_dictionary() { return dict_; }

  /**
   * Returns the string value encoded with the given dictionary code.
   */
  const char *get_string(dcode_t c);

  /**
   * Returns the dictionary code for the given string.
   */
  dcode_t get_code(const std::string &s);

  /**
   * Prints the graph (nodes, relationships) to standard output.
   */
  void dump();

  /**
   * Print the amount of allocated memory for debugging purpose.
   */
  void print_mem_usage();

  /* ---------------- query support ---------------- */

  /**
   * Scans all nodes of the graph with the given label and invokes for each of
   * these nodes the consumer function.
   */
  void nodes_by_label(const std::string &label, node_consumer_func consumer);

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

private:
  friend struct scan_task;

  /**
   * Return the node version from the dirty list that is valid for the
   * transaction identified by xid.
   */
  node &get_valid_node_version(node &n, xid_t xid);

  /**
   * Return the relationship version from the dirty list that is valid for the
   * transaction identified by xid.
   */
  relationship &get_valid_rship_version(relationship &r, xid_t xid);

  /**
   * Copy the properties from the dirty node to the nodes_ and properties_
   * tables.
   */
  void copy_properties(node &n, dirty_node_ptr dn);

  /**
   * Copy the properties from the dirty relationship to the rships_ and
   * properties_ tables.
   */
  void copy_properties(relationship &r, dirty_rship_ptr dr);

  p_ptr<node_list> nodes_; // the list of all nodes of the graph
  p_ptr<relationship_list>
      rships_; // the list of all relationships of the graph
  p_ptr<property_list>
      properties_;   // the list of all properties of nodes and relationships
  p_ptr<dict> dict_; // the dictionary used for string compression

  /**
   * These member variables are volatile and have to be reinitialized
   * during startup.
   */
  std::map<xid_t, transaction_ptr>
      *active_tx_;   // the list of all active transactions
  std::mutex *m_;    // mutex for accessing active_tx_
  xid_t oldest_xid_; // timestamp of the oldest transaction
};

using graph_db_ptr = p_ptr<graph_db>;

#endif
