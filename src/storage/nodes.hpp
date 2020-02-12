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

#ifndef nodes_hpp_
#define nodes_hpp_

#include <atomic>
#include <map>
#include <vector>

#include <boost/any.hpp>

#include "spdlog/spdlog.h"

#include "chunked_vec.hpp"
#include "defs.hpp"
#include "exceptions.hpp"
#include "properties.hpp"
#include "transaction.hpp"

struct node;
using dirty_node = dirty_object<node>;
using dirty_node_ptr = std::unique_ptr<dirty_node>;

/**
 * node represents a vertex in a graph with a non-unique label and a set of
 * properties, which can be part of a relationship. Both, properties and
 * relationships are stored in separate lists. The actual label is stored in a
 * dictionary, only the code value is stored as part of the node.
 */
struct node : public txn<dirty_node_ptr> {
  friend class node_list;
  using id_t =
      offset_t; // typedef for node identifier (used as offset in node list)

  dcode_t node_label;       // dictionary code for node label
  offset_t from_rship_list; // index in relationship list of first relationship
                            // where this node acts as from node
  offset_t to_rship_list;   // index of relationship list of first relationship
                            // where this node acts as to node
  offset_t property_list;   // index in property list

  /**
   * Default constructor.
   */
  node() = default;

  /**
   * Copy constructor.
   */
  node(const node &) = delete;

  node(node &&n)
      : txn(n), node_label(n.node_label), from_rship_list(n.from_rship_list),
        to_rship_list(n.to_rship_list), property_list(n.property_list),
        id_(n.id_) {}

  /**
   * Constructor for creating a node with the given label code.
   */
  node(dcode_t label)
      : node_label(label), from_rship_list(UNKNOWN), to_rship_list(UNKNOWN),
        property_list(UNKNOWN), id_(UNKNOWN) {}

  /**
   * Copy assignment operator. This implementation is needed because of atomic
   * xid_t.
   */
  node &operator=(const node &n) {
    txn::operator=(n);
    node_label = n.node_label;
    from_rship_list = n.from_rship_list;
    to_rship_list = n.to_rship_list;
    property_list = n.property_list;
    id_ = n.id_;

    return *this;
  }
	
  /**
   * Move assignment operator. This implementation is needed because of atomic  xid_t.
   * Move resources from source node.
   */
  node &operator=( node &&n) {
    txn::operator=(std::move(n));
    node_label = n.node_label;
    from_rship_list = n.from_rship_list;
    to_rship_list = n.to_rship_list;
    property_list = n.property_list;
    id_ = n.id_;

    return *this;
  }
	

  /**
   * Returns the node identifier.
   */
  id_t id() const { return id_; }

private:
  id_t id_;
};

/**
 * A class providing complete information for a node, i.e. string values for
 * labels and properties are available instead of only the dictionary codes.
 */
struct node_description {
  node::id_t id;           // the node identifier
  std::string label;       // the label (type)
  properties_t properties; // the list of properties

  /**
   * Return a string representation of the node_description object.
   */
  std::string to_string() const;
};

/**
 * Print a node description.
 */
std::ostream &operator<<(std::ostream &os, const node_description &ndescr);

/**
 * Helper function to print an any value.
 */
std::ostream &operator<<(std::ostream &os, const boost::any &any_value);

/**
 * A class for storing all nodes of a graph. It supports adding and removing
 * nodes as well as getting a node via its node_id.
 */
class node_list {
public:
  using range_iterator = chunked_vec<node>::range_iter;

  /**
   * Constructor
   */
  node_list() = default;
  node_list(const node_list &) = delete;

  /**
   * Destructor
   */
  ~node_list();

  /**
   * Performs initialization steps after starting the database, i.e. setting the
   * dirty_list to nullptr.
   */
  void runtime_initialize();

  /**
   * Add a new node to the list and return its identifier. The node is inserted
   * into the first available slot, i.e. to reuse space of deleted records.
   * If owner != 0 then the newly created node is locked by this owner
   * transaction.
   */
  node::id_t add(node &&n, xid_t owner = 0);

  /**
   * Append a new node to the list and return its identifier. In contrast to add
   * the node is appended at the end of the list without checking for available
   * slots. If owner == 0 then the newly created node is locked by this owner
   * transaction.
   */
  node::id_t append(node &&n, xid_t owner = 0);

  /**
   * Get a node via its identifier.
   */
  node &get(node::id_t id);

  /**
   * Remove a certain node specified by its identifier.
   */
  void remove(node::id_t id);

  /**
   * Returns the underlying vector of the node list.
   */
  chunked_vec<node> &as_vec() { return nodes_; }

  /**
   * Return a range iterator to traverse the node_list from first_chunk to
   * last_chunk.
   */
  range_iterator range(std::size_t first_chunk, std::size_t last_chunk) {
    return nodes_.range(first_chunk, last_chunk);
  }

  /**
   * Output the content of the node vector.
   */
  void dump();

  /**
   * Returns the number of occupied chunks of the underlying chunked_vec.
   */
  std::size_t num_chunks() const { return nodes_.num_chunks(); }

private:
  chunked_vec<node> nodes_; // the actual list of nodes
};

#endif
