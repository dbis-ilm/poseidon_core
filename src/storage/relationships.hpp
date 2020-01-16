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

#ifndef relationships_hpp_
#define relationships_hpp_

#include <vector>

#include "chunked_vec.hpp"
#include "defs.hpp"
#include "exceptions.hpp"
#include "nodes.hpp"
#include "transaction.hpp"

struct relationship;
using dirty_rship = dirty_object<relationship>;
using dirty_rship_ptr = std::shared_ptr<dirty_rship>;

/**
 * relationship represents a link in a graph with a non-unique label and a set
 * of properties, which connects to vertices. Both, properties and nodes are
 * stored in separate lists. The actual label is stored in a dictionary, only
 * the code value is stored as part of the node.
 */
struct relationship : public txn<dirty_rship_ptr> {
  friend class relationship_list;

  using id_t = offset_t;

  dcode_t rship_label;     // dictionary code for relationship type
  offset_t src_node;       // from-node of the relationship (index in node list)
  offset_t dest_node;      // to-node of the relationship (index in node list)
  offset_t next_src_rship; // next relationship of the from-node (index in
                           // relationship list)
  offset_t next_dest_rship; // next relationship of the to-node (index in
                            // relationship list)
  offset_t property_list;   // index in property list

  /**
   * Default constructor.
   */
  relationship() = default;

  /**
   * Constructor for a new relationship with the given label code which connects
   * the given two nodes.
   */
  relationship(dcode_t rlabel, offset_t src, offset_t dest)
      : rship_label(rlabel), src_node(src), dest_node(dest),
        next_src_rship(UNKNOWN), next_dest_rship(UNKNOWN),
        property_list(UNKNOWN) {}

  relationship::id_t id() const { return id_; }

  /**
   * Returns the node identifier of the to-node.
   */
  node::id_t to_node_id() const { return dest_node; }

  /**
   * Returns the node identifier of the from-node.
   */
  node::id_t from_node_id() const { return src_node; }

private:
  id_t id_;
};

/**
 * A class providing complete information for a relationship, i.e. string values
 * for labels and properties are available instead of only the dictionary codes.
 */
struct rship_description {
  relationship::id_t id;     // the relationship identifier
  node::id_t from_id, to_id; // identifiers of the source and destination node
  std::string label;         // the label (type)
  properties_t properties;   // the list of properties

  /**
   * Return a string representation of the node_description object.
   */
  std::string to_string() const;
};

/**
 * Print a relationship description.
 */
std::ostream &operator<<(std::ostream &os, const rship_description &rdescr);

/**
 * A class for storing all relationships of a graph. It supports adding and
 * removing relationships as well as getting a relationship via its
 * relationship_id.
 */
class relationship_list {
public:
  /**
   * Constructors.
   */
  relationship_list() = default;
  relationship_list(const relationship_list &) = delete;

  /**
   * Destructor.
   */
  ~relationship_list() = default;

  /**
   * Performs initialization steps after starting the database, i.e. setting the
   * dirty_list to nullptr.
   */
  void runtime_initialize();

  /**
   * Add a new relationship to the list and return its identifier. The
   * relationship is inserted into the first available slot, i.e. to reuse space
   * of deleted records.
   * If owner != 0 then the newly created relationship is locked by this owner
   * transaction.
   */
  relationship::id_t add(relationship &&r, xid_t owner = 0);

  /**
   * Append a new relationship to the list and return its identifier. In
   * contrast to add the relationship is appended at the end of the list without
   * checking for available slots.
   * If owner != 0 then the newly created relationship is locked by this owner
   * transaction.
   */
  relationship::id_t append(relationship &&r, xid_t owner = 0);

  /**
   * Get a relationship via its identifier.
   */
  relationship &get(relationship::id_t id);

  /**
   * Remove a certain relationship specified by its identifier.
   */
  void remove(relationship::id_t id);

  /**
   * Return the last entry (i.e. relationship) of the list of relationships
   * of a node which starts at the relationship identifier id.
   */
  relationship &last_in_from_list(relationship::id_t id);

  /**
   * Return the last entry (i.e. relationship) of the list of relationships
   * of a node which end at the relationship identifier id.
   */
  relationship &last_in_to_list(relationship::id_t id);

  /**
   * Returns the underlying vector of the relationship list.
   */
  chunked_vec<relationship> &as_vec() { return rships_; }

  /**
   * Output the content of the relationship vector.
   */
  void dump();

  /**
   * Returns the number of occupied chunks of the underlying chunked_vec.
   */
  std::size_t num_chunks() const { return rships_.num_chunks(); }

private:
  chunked_vec<relationship> rships_; // the actual list of relationships
};
#endif