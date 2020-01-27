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

#ifndef properties_hpp_
#define properties_hpp_

#include <array>
#include <map>
#include <vector>

#include <boost/any.hpp>
#include <boost/variant.hpp>

#include "defs.hpp"

#include "chunked_vec.hpp"
#include "dict.hpp"
#include "exceptions.hpp"

#include "spdlog/spdlog.h"

/* ------------------------------------------------------------------------ */

/**
 * Checks the typecode of property flags for the given type.
 */
#define P_UNUSED(f) ((f & 0xe0) == p_item::p_unused)
#define P_INT_VAL(f) ((f & 0xe0) == p_item::p_int)
#define P_DOUBLE_VAL(f) ((f & 0xe0) == p_item::p_double)
#define P_DICT_VAL(f) ((f & 0xe0) == p_item::p_dcode)

/**
 * Sets the typecode bits in property flags.
 */
#define P_SET_VAL(f, tc) (f = tc)

/**
 * p_item represents a property as a key-value pair describing an attribute of a
 * node or relationship.
 */
struct p_item {
  using predicate_func = std::function<bool(const p_item &)>;
  using int_t = int;
  using double_t = double;
  using string_t = std::string;

  /**
   * Typecodes for property values represented by p_item types.
   */
  enum p_typecode {
    p_unused = 0b11100000, // unused
    p_int = 0b00100000,    // integer
    p_double = 0b01000000, // double
    p_dcode = 0b01100000   // dictionary codes stored as integer values
  };

  p_item() : flags_(0), key_(0) { P_SET_VAL(flags_, p_unused); }

  p_item(const p_item &) = default;

  p_item(dcode_t k, double v);
  p_item(dcode_t k, int v);
  p_item(dcode_t k, dcode_t v);

  p_item(const boost::any &v, dict_ptr &dct);
  p_item(const std::string &k, const boost::any &v, dict_ptr &dct);
  p_item(dcode_t k, const boost::any &v, dict_ptr &dct);

  p_item &operator=(const p_item &p);

  p_typecode typecode() const { return static_cast<p_typecode>(flags_ & 0xe0); }
  dcode_t key() const { return key_; }

  const char *key(dict_ptr &dct) { return dct->lookup_code(key_); }

  template <typename T> T get() const;
  template <typename T> void set(T v);

  bool equal(int i) const;

  bool equal(double d) const;

  bool equal(dcode_t c) const;

  bool empty() const { return P_UNUSED(flags_); }

  uint8_t flags_; // Bit 0-2 used for representing the typecode (see p_typecode)
  dcode_t key_;   // dictionary code for property name
  uint8_t value_[8]; // placeholder for storing int, double or dcode_t values
};

/* ------------------------------------------------------------------------ */

/**
 * Returns true if s is a quoted string ("" or '').
 */
bool is_quoted_string(const std::string &s);

/**
 * Returns true if s is a floating point number containing a decimal point.
 */
bool is_float(const std::string &s);

/**
 * Returns true if s is an integer number.
 */
bool is_int(const std::string &s);

/**
 * A typedef for a list of properties (key-value pairs) where values are
 * either numeric or full strings.
 */
using properties_t = std::map<std::string, boost::any>;

template <typename T>
T get_property(const properties_t &p, const std::string &key) {
  auto it = p.find(key);
  if (it == p.end()) {
    spdlog::info("unknown property: {}", key);
    throw unknown_property();
  }
  return boost::any_cast<T>(it->second);
}

#define SET_NODE_TYPE(f) (f &= 0x7f)
#define SET_RELATIONSHIP_TYPE(f) (f |= 0x80)
#define IS_NODE_TYPE(f) !(f & 0x80)

/**
 * property_set represents a set of key-value pairs describing  attributes of a
 * node or relationship. A property_set is stored as a single record in the
 * property_list table.
 *
 */
struct property_set {
  using id_t = offset_t; // typedef for property identifier (used as offset in
                         // property list)
  using p_item_list = std::array<p_item, 5>;

  uint8_t flags;     // Bit 0 = owner: 0 for node, 1 for relationship
  p_item_list items; // we are storing 5 property items per set
  offset_t next;     // index of next property item
  offset_t owner;    // node id or relationship id owning the property

  /**
   * Default constructor.
   */
  property_set() = default;

  property_set(offset_t o, const p_item_list &pil, offset_t n,
               bool is_node = true)
      : flags(0), items(pil), next(n), owner(o) {
    if (!is_node)
      SET_RELATIONSHIP_TYPE(flags);
  }

  bool is_node_owner() const { return IS_NODE_TYPE(flags); }
};

/**
 * A class for storing all properties associated with nodes and relationships of
 * a graph. It supports adding and removing properties as well as looking up
 * nodes via their property values.
 */
class property_list {
public:
  /**
   * Constructor
   */
  property_list() = default;
  property_list(const property_list &) = delete;

  /**
   * Destructor
   */
  ~property_list() = default;

  /**
   * Inserts the properties from the given list and assign them to the node with
   * the given id. The keys (names) of these properties are encoded via the
   * dictionary. This method assumes that no properties are associated yet with
   * this node.
   */
  property_set::id_t
  add_node_properties(offset_t nid, const properties_t &props, dict_ptr &dct);

  /**
   * Inserts the p_items from the given list and assign them to the
   * node/relationship with the given id. This method assumes that no properties
   * are associated yet with this node/relationship.
   */
  property_set::id_t add_pitems(offset_t nid, const std::list<p_item> &props,
                                dict_ptr &dct, bool is_node = true);

  /**
   * Appends the properties from the given list and assign them to the node with
   * the given id. The keys (names) of these properties are encoded via the
   * dictionary. This method assumes that no properties are associated yet with
   * this node. Note, that this is an append-only method - it doesn't reuse
   * available slots from previously deleted properties.
   */
  property_set::id_t append_node_properties(offset_t nid,
                                            const properties_t &props,
                                            dict_ptr &dct);

  /**
   * Inserts the properties from the given list and assign them to the
   * relationship with the given id. The keys (names) of these properties
   * are encoded via the dictionary. This method assumes that no properties
   * are associated yet with this node.
   */
  property_set::id_t add_relationship_properties(offset_t rid,
                                                 const properties_t &props,
                                                 dict_ptr &dct);

  /**
   * Appends the properties from the given list and assign them to the
   * relationship with the given id. The keys (names) of these properties are
   * encoded via the dictionary. This method assumes that no properties are
   * associated yet with this node. Note, that this is an append-only method -
   * it doesn't reuse available slots from previously deleted properties.
   */
  property_set::id_t append_relationship_properties(offset_t rid,
                                                    const properties_t &props,
                                                    dict_ptr &dct);

  /**
   * Returns the value of the property of a node/relationship in the
   * corresponding property list at offset id. If the property doesn't exist
   * then an execption is raised.
   */
  p_item property_value(offset_t id, dcode_t pkey);

  /**
   * Scans all node properties with the name represented by the encoded key and
   * for all properties satisfying the predicate, the function f is invoked.
   */
  void foreach_node(dcode_t pkey, p_item::predicate_func pred,
                    std::function<void(offset_t)> f);

  /**
   * Returns a list of all properties of a node/relationship where the list
   * starts at the given id. The keys of the properties are decoded via the
   * given dictionary.
   */
  properties_t all_properties(offset_t id, dict_ptr &dct);

  /**
   * Updates the values of the given properties of a node where the list
   * starts at the given id. If the node/relationship or the
   * property doesn't exist an execption is raised.
   */
  property_set::id_t update_properties(offset_t nid, offset_t id,
                                       const properties_t &props,
                                       dict_ptr &dct);

  /**
   * Updates the values of the given properties of a node/relationship where the
   * list starts at the given id. If the node/relationship or the property
   * doesn't exist an execption is raised.
   */
  property_set::id_t update_pitems(offset_t nid, offset_t id,
                                   const std::list<p_item> &props,
                                   dict_ptr &dct);

  /**
   * Build a list of p_items from the list of properties represented by props.
   * This method is used to handle transactional inserts.
   */
  std::list<p_item> build_dirty_property_list(const properties_t &props,
                                              dict_ptr &dct);

  /**
   * Build a list of p_items from the currently stored properties of the
   * node/relationship identified by nid where the list starts at offset id +
   * the list of updated/added properties represented by props. This method is
   * used to handle transactional updates.
   */
  std::list<p_item> build_dirty_property_list(offset_t nid, offset_t id/*,
                                              const properties_t &props,
                                              dict_ptr &dct*/);

  std::list<p_item> &apply_updates(std::list<p_item> &pitems,
                                   const properties_t &props, dict_ptr &dct);

  /**
   * Returns a list of the properties of a node/relationship from the list of
   * p_items.
   */
  properties_t build_properties_from_pitems(const std::list<p_item> &pitems,
                                            dict_ptr &dct);

  /**
   * Returns the number of occupied chunks of the underlying chunked_vec.
   */
  std::size_t num_chunks() const { return properties_.num_chunks(); }

private:
  chunked_vec<property_set> properties_; // the actual list of properties
};

#endif