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
#include <utility>
#include <any>

#include "vec.hpp"
#include "defs.hpp"
#include "exceptions.hpp"
#include "properties.hpp"
#include "transaction.hpp"
#include "txn_data.hpp"

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
  friend class graph_db;
  
  template <template <typename I> typename V> friend class node_list;

  using id_t =
      offset_t; // typedef for node identifier (used as offset in node list)
private:
  id_t id_;
public:
  offset_t from_rship_list; // index in relationship list of first relationship
                            // where this node acts as from node
  offset_t to_rship_list;   // index of relationship list of first relationship
                            // where this node acts as to node
  offset_t property_list;   // index in property list
  dcode_t node_label;       // dictionary code for node label

  /**
   * Default constructor.
   */
  node() = default;

  /**
   * Copy constructor.
   */
  node(const node &) = delete;

  node(node &&n)
      : txn(n), id_(n.id_), from_rship_list(n.from_rship_list),
        to_rship_list(n.to_rship_list), property_list(n.property_list),
        node_label(n.node_label) {}

  /**
   * Constructor for creating a node with the given label code.
   */
  node(dcode_t label)
      : id_(UNKNOWN), from_rship_list(UNKNOWN), to_rship_list(UNKNOWN),
        property_list(UNKNOWN), node_label(label) {}

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

  std::size_t _offset() const {
    return (uint64_t)((uint8_t *)&id_) - (uint64_t)((uint8_t *)this);
  }
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

  /**
   * Return true if a property with the given name exists.
   */
  bool has_property(const std::string& pname) const;

  bool operator==(const node_description& other) const;
};

/**
 * Print a node description.
 */
std::ostream &operator<<(std::ostream &os, const node_description &ndescr);

/**
 * Helper function to print an any value.
 */
std::ostream &operator<<(std::ostream &os, const std::any &any_value);

/**
 * A class for storing all nodes of a graph. It supports adding and removing
 * nodes as well as getting a node via its node_id.
 */
template <template <typename I> typename T>
class node_list {

  /**
   * A helper class for parallel inititialization of all nodes.
   */
  struct init_node_task {
    using range = std::pair<std::size_t, std::size_t>;

    init_node_task(T<node> &n, std::size_t first, std::size_t last) : nodes_(n), range_(first, last) {}

    void operator()() {
      auto iter = nodes_.range(range_.first, range_.second);
      while (iter) {
        auto &n = *iter;
        n.runtime_initialize();
        ++iter;
      }
    }

    T<node> &nodes_;
    range range_;
  };

public:
  using vec = T<node>;
  using range_iterator = typename T<node>::range_iter;

  /**
   * Constructor
   */
  template <typename ... Args>
  node_list(Args&& ... args) : nodes_(std::forward<Args>(args)...) {} 

  node_list(const node_list &) = delete;

  /**
   * Destructor
   */
  ~node_list() = default;

  /**
   * Performs initialization steps after starting the database, i.e. setting the
   * dirty_list to nullptr.
   */
  void runtime_initialize() {
  // make sure that all locks are released and no dirty objects exist
#ifdef PARALLEL_INIT
  const int nchunks = 100;
  std::vector<std::future<void>> res;
  res.reserve(num_chunks() / nchunks + 1);
  // spdlog::info("starting {} init node tasks...", num_chunks() / nchunks + 1);
  thread_pool pool;
  std::size_t start = 0, end = nchunks - 1;
  while (start < num_chunks()) {
    res.push_back(pool.submit(
        init_node_task(*this, start, end)));
    // spdlog::info("starting: {}, {}", start, end);
    start = end + 1;
    end += nchunks;
  }
 // std::cout << "waiting ..." << std::endl;
  for (auto &f : res)
    f.get();
#else
  for (auto &n : nodes_) {
    n.runtime_initialize();
  }
#endif   
  }

  /**
   * Add a new node to the list and return its identifier. The node is inserted
   * into the first available slot, i.e. to reuse space of deleted records.
   * If owner != 0 then the newly created node is locked by this owner
   * transaction.
   */
  node::id_t add(node &&n, xid_t owner = 0) {
    if (nodes_.is_full())
    nodes_.resize(1);

  auto id = nodes_.first_available();
  assert(id != UNKNOWN);
  n.id_ = id;
  if (owner != 0) {
    /// spdlog::info("lock node #{} by {}", id, owner);
    n.lock(owner);
  }
  nodes_.store_at(id, std::move(n));
  return id;  
  }

  /**
   * Add a new node to the list and return its identifier. The node is inserted
   * into the first available slot, i.e. to reuse space of deleted records.
   * If owner != 0 then the newly created node is locked by this owner
   * transaction. If a callback is given this function is called before the slot 
   * is reserved (used for undo logging).
   */
  node::id_t insert(node &&n, xid_t owner = 0, std::function<void(offset_t)> callback = nullptr) {
    auto p = nodes_.store(std::move(n), callback);
  p.second->id_ = p.first;
  if (owner != 0) {
    // spdlog::info("lock node #{} by {}", p.first, owner);
    p.second->lock(owner);
  }

  return p.first;  
  }

  /**
   * Append a new node to the list and return its identifier. In contrast to add
   * the node is appended at the end of the list without checking for available
   * slots. If owner == 0 then the newly created node is locked by this owner
   * transaction. If a callback is given this function is called before the slot 
   * is reserved (used for undo logging).
   */
  node::id_t append(node &&n, xid_t owner = 0, std::function<void(offset_t)> callback = nullptr) {
    auto p = nodes_.append(std::move(n), callback);
  p.second->id_ = p.first;
  if (owner != 0) {
    /// spdlog::info("lock node #{} by {}", p.first, owner);
    p.second->lock(owner);
  }

  return p.first;  
  }

  /**
   * Get a node via its identifier.
   */
  node &get(node::id_t id) {
   if (nodes_.capacity() <= id) {
    spdlog::warn("unknown node_id {}", id);
    throw unknown_id();
   }
  auto &n = nodes_.at(id);
  return n;   
  }

  /**
   * Remove a certain node specified by its identifier.
   */
  void remove(node::id_t id) {
    if (nodes_.capacity() <= id)
    throw unknown_id();
  nodes_.erase(id);  
  }

  /**
   * Returns the underlying vector of the node list.
   */
  auto &as_vec() { return nodes_; }

  /**
   * Return a range iterator to traverse the node_list from first_chunk to
   * last_chunk.
   */
  range_iterator range(std::size_t first_chunk, std::size_t last_chunk, std::size_t start_pos = 0) {
    return nodes_.range(first_chunk, last_chunk, start_pos);
  }

  range_iterator* range_ptr(std::size_t first_chunk, std::size_t last_chunk, std::size_t start_pos = 0) {
    return nodes_.range_ptr(first_chunk, last_chunk, start_pos);
  }
  /**
   * Output the content of the node vector.
   */
  void dump() {
  std::cout << "----------- NODES -----------\n";
  for (auto& n : nodes_) {
    std::cout << std::dec << "#" << n.id() << ", @" << (unsigned long)&n
              << " [ txn-id=" << short_ts(n.txn_id()) << ", bts=" << short_ts(n.bts())
              << ", cts=" << short_ts(n.cts()) << ", dirty=" << (n.d_ != nullptr ? n.d_->is_dirty_ : false)
              << " ], label=" << n.node_label << ", from="
              << uint64_to_string(n.from_rship_list) << ", to=" << uint64_to_string(n.to_rship_list) << ", props="
              << uint64_to_string(n.property_list);
    if (n.has_dirty_versions()) {
      // print dirty list
      std::cout << " {\n";
      for (const auto& dn : *(n.dirty_list())) {
        std::cout << "\t( @" << (unsigned long)&(dn->elem_)
                  << ", txn-id=" << short_ts(dn->elem_.txn_id())
                  << ", bts=" << short_ts(dn->elem_.bts()) << ", cts=" << short_ts(dn->elem_.cts())
                  << ", label=" << dn->elem_.node_label
                  << ", dirty=" << dn->elem_.is_dirty()
                  << ", from=" << uint64_to_string(dn->elem_.from_rship_list)
                  << ", to=" << uint64_to_string(dn->elem_.to_rship_list)
                  << ", [";
        for (const auto& pi : dn->properties_) {
          std::cout << " " << pi;
        }
        std::cout << " ])\n";
      }
      std::cout << "}";
    }
    std::cout << "\n";
  }
  std::cout << "-----------------------------\n";    
  }

  /**
   * Returns the number of occupied chunks of the underlying chunked_vec.
   */
  std::size_t num_chunks() const { return nodes_.num_chunks(); }

private:
  T<node> nodes_; // the actual list of nodes
};

#endif
