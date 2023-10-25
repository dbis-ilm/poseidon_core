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
#include <any>

#include <boost/variant.hpp>

#include "defs.hpp"

#include "vec.hpp"
#include "dict.hpp"
#include "exceptions.hpp"

#include "spdlog/spdlog.h"

bool is_quoted_string(const std::string &s);
bool is_float(const std::string &s);
bool is_int(const std::string &s);
bool is_date(const std::string &s);
bool is_dtime(const std::string &s);

/* ------------------------------------------------------------------------ */

/**
 * Checks the typecode of property flags for the given type.
 */
#define P_UNUSED(f) ((f & 0xe0) == p_item::p_unused)
#define P_INT_VAL(f) ((f & 0xe0) == p_item::p_int)
#define P_DOUBLE_VAL(f) ((f & 0xe0) == p_item::p_double)
#define P_DICT_VAL(f) ((f & 0xe0) == p_item::p_dcode)
#define P_UINT64_VAL(f) ((f & 0xe0) == p_item::p_uint64)
#define P_PTIME_VAL(f) ((f & 0xe0) == p_item::p_ptime)

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
    p_int =    0b00100000, // integer
    p_double = 0b01000000, // double
    p_dcode =  0b01100000, // dictionary codes stored as integer values
    p_uint64 = 0b10000000, // unsigned 64-bit integer
    p_ptime =  0b10100000, // datetime
    p_date =   0b11100000, // date - used only during import not for
                           // storing the value (use ptime instead) 
  };

  p_item() = default; // : key_(0), flags_(0) { P_SET_VAL(flags_, p_unused); }
  p_item(int) : key_(0), flags_(0) { P_SET_VAL(flags_, p_unused); }
  p_item(const p_item &) = default;

  p_item(dcode_t k, p_typecode tc, const std::any &v);
  p_item(dcode_t k, double v);
  p_item(dcode_t k, int v);
  p_item(dcode_t k, dcode_t v);
  p_item(dcode_t k, uint64_t v);
  p_item(dcode_t k, boost::posix_time::ptime v);

  p_item(const std::any &v, dict_ptr &dct);
  p_item(const std::string &k, const std::any &v, dict_ptr &dct);
  p_item(dcode_t k, p_item::p_typecode tc, const std::string& v, dict_ptr &dict);

  p_item(dcode_t k, const std::any &v, dict_ptr &dct);

  p_item &operator=(const p_item &p);

  p_typecode typecode() const { return static_cast<p_typecode>(flags_ & 0xe0); }
  dcode_t key() const { return key_; }

  const char *key(dict_ptr &dct) { return dct->lookup_code(key_); }

  uint64_t get_raw() const;
  
  template <typename T> T get() const;
  template <typename T> void set(T v);

  bool equal(int i) const;

  bool equal(double d) const;

  bool equal(dcode_t c) const;

  bool equal(uint64_t ll) const;

  bool equal(boost::posix_time::ptime dt) const;

  bool empty() const { return P_UNUSED(flags_); }

  uint8_t value_[8]; // placeholder for storing int, double or dcode_t values
  dcode_t key_;   // dictionary code for property name
  uint8_t flags_; // Bit 0-2 used for representing the typecode (see p_typecode)
};

std::ostream& operator<< (std::ostream& os, const p_item& pi);

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
 * Returns true if s is a date with format YYYY-MM-DD
 */
bool is_date(const std::string &s);

/**
 * Returns true if s is a datetime with format YYYY-MM-DDTHH:MM:SS+ffff.
 */
bool is_dtime(const std::string &s);

/**
 * A typedef for a list of properties (key-value pairs) where values are
 * either numeric or full strings.
 */
using properties_t = std::map<std::string, std::any>;

template <typename T>
std::optional<T> get_property(const properties_t &p, const std::string &key) {
  auto it = p.find(key);
  if (it == p.end()) {
    // spdlog::warn("unknown property: {}", key);
    return std::nullopt;
  }
  return std::optional<T> { std::any_cast<T>(it->second) };
}

/**
 * property_set represents a set of key-value pairs describing  attributes of a
 * node or relationship. A property_set is stored as a single record in the
 * property_list table.
 *
 */
struct property_set {
  using id_t = offset_t; // typedef for property identifier (used as offset in
                         // property list)
  using p_item_list = std::array<p_item, 3>;

  offset_t next;     // index of next property item
  offset_t owner;    // node id or relationship id owning the property
  p_item_list items; // we are storing 3 property items per set

  /**
   * Default constructor.
   */
  property_set() = default;

  property_set(offset_t o, const p_item_list &pil, offset_t n)
      : next(n), owner(o), items(pil) {
  }

  inline void runtime_initialize() { /* nothing */ }
};

/**
 * A class for storing all properties associated with nodes and relationships of
 * a graph. It supports adding and removing properties as well as looking up
 * nodes via their property values.
 */
template <template <typename I> typename T>
class property_list {
public:
  using foreach_cb_func = std::function<void(offset_t oid, property_set::p_item_list& items, offset_t next)>; 
  /**
   * Constructor
   */
  template <typename ... Args>
  property_list(Args&& ... args) : properties_(std::forward<Args>(args)...) {} 

  property_list(const property_list &) = delete;

  /**
   * Destructor
   */
  ~property_list() = default;

  /**
   * Inserts the properties from the given list and assign them to the node/relationship 
   * with the given id. The keys (names) of these properties are encoded via the
   * dictionary. This method assumes that no properties are associated yet with
   * this node/relationship.
   */
  property_set::id_t
  add_properties(offset_t nid, const properties_t &props, dict_ptr &dct) {
 property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      if (properties_.is_full())
        properties_.resize(1);
      auto id = properties_.first_available();
      assert(id != UNKNOWN);

      properties_.store_at(id,
                           property_set(nid, std::move(pil), next_id));

      next_id = id;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;    
  }

  /**
   * Inserts the p_items from the given list and assign them to the
   * node/relationship with the given id. This method assumes that no properties
   * are associated yet with this node/relationship. If a callback is given this 
   * function is called before the slot is reserved (used for undo logging).
   */
  property_set::id_t add_pitems(offset_t nid, const std::list<p_item> &props,
                                dict_ptr &dct, 
                                std::function<void(offset_t)> callback = nullptr) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      std::unique_lock<std::mutex> ulock(m); 
      auto pr = properties_.store(property_set(nid, std::move(pil), next_id), callback);
      ulock.unlock();  
      next_id = pr.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
  }

  /**
   * Appends the properties from the given list and assign them to the 
   * node/relationship with the given id. The keys (names) of these properties 
   * are encoded via the dictionary. This method assumes that no properties are 
   * associated yet with this node/rship. Note, that this is an append-only method 
   * - it doesn't reuse available slots from previously deleted properties.
   */
  property_set::id_t append_properties(offset_t nid,
                                            const properties_t &props,
                                            dict_ptr &dct) {
 property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
  }

  property_set::id_t append_typed_properties(offset_t nid, 
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist, 
                              const std::vector<std::any>& values) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0;
  // spdlog::info("append_typed_properties: {}, {}", values.size(), keys.size());

  pil.fill(p_item(0));
  for (auto i = 0u; i < keys.size(); i++) {
    if (!values[i].has_value()) // we don't add empty properties
      continue;
    // spdlog::info("property @{} <- {}, type={}", pidx, i, typelist[i]);
    pil[pidx++] = p_item(keys[i], typelist[i], values[i]);
    if (i == keys.size() - 1 || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
  }

  property_set::id_t append_typed_properties(offset_t nid,
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist,
							                const std::vector<std::string>& values,
                              dict_ptr &dict) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0;
  // spdlog::info("append_typed_properties: {}, {}", values.size(), keys.size());

  pil.fill(p_item(0));
  for (auto i = 0u; i < keys.size(); i++) {
    if (values[i].empty()) // we don't add empty properties
      continue;
    // spdlog::info("property @{} <- {}, type={}", pidx, i, typelist[i]);
    pil[pidx++] = p_item(keys[i], typelist[i], values[i], dict);
    if (i == keys.size() - 1 || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
  }

 
  /**
   * Returns the value of the property of a node/relationship in the
   * corresponding property list at offset id. If the property doesn't exist
   * then an execption is raised.
   */
  p_item property_value(offset_t id, dcode_t pkey) {
 offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
      if (item.key() == pkey)
        return item;
    }
    pset_id = p.next;
  }
  return p_item();    
  }

  /**
   * Scans all properties with the name represented by the encoded key and
   * for all properties satisfying the predicate, the function f is invoked.
   */
  void foreach(dcode_t pkey, p_item::predicate_func pred,
                    std::function<void(offset_t)> f) {
  for (auto &p : properties_) {
      for (auto &item : p.items) {
        if (item.key() == pkey && pred(item))
          f(p.owner);
      }
  }
  }

  void foreach_property(std::function<void(const p_item& pi)> f) {
  for (const auto& ps : properties_) {
    for (const auto& p : ps.items) {
      if (!P_UNUSED(p.flags_))
        f(p);
    }
  }    
  }

  /**
   * Returns a list of all properties of a node/relationship where the list
   * starts at the given id. The keys of the properties are decoded via the
   * given dictionary.
   */
  properties_t all_properties(offset_t id, dict_ptr &dct) {
  properties_t pmap;
  if (properties_.capacity() <= id)
    return pmap;

  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
      auto s = dct->lookup_code(item.key());
      // spdlog::info("property: {} - {}", s, item.typecode());
      switch (item.typecode()) {
      case p_item::p_int:
        pmap.insert({s, item.template get<int>()});
        break;
      case p_item::p_double:
        pmap.insert({s, item.template get<double>()});
        break;
      case p_item::p_uint64:
        pmap.insert({s, item.template get<uint64_t>()});
        break;
      case p_item::p_dcode: {
        auto s2 = dct->lookup_code(item.template get<dcode_t>());
        pmap.insert({s, std::string(s2)});
        break;
      }
      case p_item::p_ptime:
        pmap.insert({s, item.template get<boost::posix_time::ptime>()});
        break;
      case p_item::p_unused:
        // spdlog::info("{} -> p_unused", s);
        break;
      }
    }
    pset_id = p.next;
  }
  return pmap;    
  }

  /**
   * Traverses the properties of a node/relationship where the list
   * starts at the given id and calls the callback function for each entry.
   * This method is used e.g. for undo logging.
   */
  void foreach_property_set(offset_t id, foreach_cb_func cb) {
  // std::cout << "foreach_property_set..." << std::endl;
  offset_t pset_id = id;
  try {
    while (pset_id != UNKNOWN) {
      auto &p = properties_.at(pset_id);
      if (cb != nullptr)
        cb(pset_id, p.items, p.next);
      pset_id = p.next;
    }
  } catch (unknown_id& exc) {
    throw unknown_property();
  }    
  }

  /**
   * Updates the values of the given properties of a node where the list
   * starts at the given id. If the node/relationship or the
   * property doesn't exist an execption is raised.
   */
  property_set::id_t update_properties(offset_t nid, offset_t id,
                                       const properties_t &props,
                                       dict_ptr &dct) {
 // first, we build the list of properties to be updated
  using todo_list_t = std::list<std::pair<dcode_t, std::any>>;
  todo_list_t todo_list;
  property_set::id_t next_id = id;

  for (auto &pv : props) {
    auto dc = dct->insert(pv.first);
    todo_list.push_back(std::make_pair(dc, pv.second));
  }

  offset_t item = id;
  // next, we process property_set-wise
  while (!todo_list.empty()) {
    assert(item != UNKNOWN);
    auto &pset = properties_.at(item);
    for (auto i = 0u; i < pset.items.size(); i++) {
      todo_list_t::iterator prop =
          std::find_if(todo_list.begin(), todo_list.end(),
                       [&](auto p) { return p.first == pset.items[i].key(); });
      if (prop != todo_list.end()) {
        pset.items[i] = p_item(prop->first, prop->second, dct);
        todo_list.erase(prop);
      }
    }
    if (!todo_list.empty()) {
      // fetch the next property_set record
      item = pset.next;
      // if this was the last one, let's stop here
      if (item == UNKNOWN)
        break;
    }
  }

  // finally, we have to add the remaining properties
  if (!todo_list.empty()) {
    property_set::p_item_list pil;
    std::size_t pidx = 0, n = 0;
    for (auto &kv : todo_list) {
      pil[pidx++] = p_item(kv.first, kv.second, dct);
      if (++n == todo_list.size() || pidx == pil.max_size()) {
        auto p = properties_.append(
            property_set(nid, std::move(pil), next_id));
        next_id = p.first;
        pidx = 0;
        pil.fill(p_item(0));
      }
    }
  }

  return next_id;
  }

  /**
   * Updates the values of the given properties of a node/relationship where the
   * list starts at the given id. If the node/relationship or the property
   * doesn't exist an execption is raised.
   */
  property_set::id_t update_pitems(offset_t nid, offset_t id,
                                   const std::list<p_item> &props,
                                   dict_ptr &dct) {
  // we know that props contains all previous properties
  // thus, we can overwrite all existing property_sets starting with id
  property_set::id_t head_id = id;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      if (id != UNKNOWN) {
        auto &old = properties_.at(id);
        old.items = pil;
        id = old.next;
        /// spdlog::info("next id: {}", id);
        pidx = 0;
        pil.fill(p_item(0));
      } else {
        // we have some additional properties which do not fit on the
        // current properties_ entries -> let's create a new one
        if (properties_.is_full())
          properties_.resize(1);
        auto new_id = properties_.first_available();
        assert(new_id != UNKNOWN);

        properties_.store_at(new_id,
                             property_set(nid, std::move(pil), head_id));

        head_id = new_id;
        pidx = 0;
        pil.fill(p_item(0));
      }
    }
  }
  return head_id;                                    
  }

  /**
   * Get a property set via its identifier.
   */
  property_set &get(property_set::id_t id) {
  if (properties_.capacity() <= id)
    throw unknown_id();
  auto &pset = properties_.at(id);
  return pset;    
  }

  /**
   * Removes a certain property set specified by its identifier.
   */
  void remove(property_set::id_t id) {
  // std::cout << "property_list::remove: " << id << std::endl;
  if (properties_.capacity() <= id)
    throw unknown_id();
  properties_.erase(id);    
  }

  /**
   * Removes all properties belonging to a node/relationship starting at the given property id.
   */
  void remove_properties(property_set::id_t id) {
 // std::cout << "property_list::remove_properties: " << id << std::endl;
  if (id == UNKNOWN)
    return;
    
  if (properties_.capacity() <= id)
    throw unknown_property();

  auto pset_id = id;
  while (pset_id != UNKNOWN) {
    auto pid = pset_id;
    auto &p = properties_.at(pset_id);
    pset_id = p.next;
    properties_.erase(pid);
  }    
  }

  /**
   * Returns the underlying vector of the property set list.
   */
  auto &as_vec() { return properties_; }

  /**
   * Build a list of p_items from the list of properties represented by props.
   * This method is used to handle transactional inserts.
   */
  std::list<p_item> build_dirty_property_list(const properties_t &props,
                                              dict_ptr &dct) {
  std::list<p_item> p_item_list;
  for (auto &kv : props) {
    p_item_list.push_back(p_item(kv.first, kv.second, dct));
  }
  return p_item_list;
  }

  /**
   * Build a list of p_items from the currently stored properties of the
   * node/relationship identified by the id (n.property_list) where the list starts at offset id +
   * the list of updated/added properties represented by props. This method is
   * used to handle transactional updates.
   */
  std::list<p_item> build_dirty_property_list(offset_t id) {
  std::list<p_item> p_item_list;
  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
        // Optimization: Insert only valid keys to avoid holes in Property list and avoid resource leak.
        if(item.key_ != 0)  
          p_item_list.push_back(item);
    }
    pset_id = p.next;
  }
  return p_item_list;    
  }

  std::list<p_item> &apply_updates(std::list<p_item> &pitems,
                                   const properties_t &props, dict_ptr &dct) {
  // update p_item_list with properties
  std::list<p_item> todo_list;
  for (auto &kv : props) {
    p_item pnew(kv.first, kv.second, dct);
    bool updated = false;
    for (auto &pi : pitems) {
      if (pi.key() == pnew.key()) {
        pi = pnew;
        updated = true;
        break;
      }
    }
    if (!updated)
      todo_list.push_back(pnew);
  }
  // append the elements from todo_list
  pitems.splice(pitems.end(), todo_list);
  return pitems;
  }

  /**
   * Returns a list of the properties of a node/relationship from the list of
   * p_items.
   */
  properties_t build_properties_from_pitems(const std::list<p_item> &pitems,
                                            dict_ptr &dct) {
  properties_t pmap;

  for (auto &item : pitems) {
    auto s = dct->lookup_code(item.key());
    switch (item.typecode()) {
    case p_item::p_int:
      pmap.insert({s, item.get<int>()});
      break;
    case p_item::p_double:
      pmap.insert({s, item.get<double>()});
      break;
    case p_item::p_uint64:
      pmap.insert({s, item.get<uint64_t>()});
      break;
    case p_item::p_dcode: {
      auto s2 = dct->lookup_code(item.get<dcode_t>());
      pmap.insert({s, std::string(s2)});
      break;
    }
    case p_item::p_ptime:
      pmap.insert({s, item.get<boost::posix_time::ptime>()});
      break;
    case p_item::p_unused:
      break;
    }
  }
  return pmap;
  }

  /**
   * Returns the number of occupied chunks of the underlying chunked_vec.
   */
  std::size_t num_chunks() const { return properties_.num_chunks(); }

  /**
   * Output the content of the property vector.
   */
  void dump() {
  std::cout << "------- PROPERTY SETS -------" << std::endl;
  for (const auto& ps : properties_) {
    std::cout << "owner=" 
      << ps.owner 
      << ", next=" << uint64_to_string(ps.next) 
      << ", items=[";
    for (auto i = 0u; i < 3; i++) {
      std::cout << std::hex << ps.items[i].key_ << ",";
    }
    std::cout << "]" << std::endl;
  }    
  }

private:
  T<property_set> properties_; // the actual list of properties
  std::mutex m;
};

#endif
