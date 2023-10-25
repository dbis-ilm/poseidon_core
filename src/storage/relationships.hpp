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

#ifndef relationships_hpp_
#define relationships_hpp_

#include <vector>

#include "vec.hpp"
#include "defs.hpp"
#include "exceptions.hpp"
#include "nodes.hpp"
#include "transaction.hpp"
#include "txn_data.hpp"

struct relationship;
using dirty_rship = dirty_object<relationship>;
using dirty_rship_ptr = std::unique_ptr<dirty_rship>;

/**
 * relationship represents a link in a graph with a non-unique label and a set
 * of properties, which connects to vertices. Both, properties and nodes are
 * stored in separate lists. The actual label is stored in a dictionary, only
 * the code value is stored as part of the node.
 */
struct relationship : public txn<dirty_rship_ptr> {
  template <template <typename I> typename V> friend class relationship_list;

  using id_t = offset_t;

private:
  id_t id_;

public:
  offset_t src_node;       // from-node of the relationship (index in node list)
  offset_t dest_node;      // to-node of the relationship (index in node list)
  offset_t next_src_rship; // next relationship of the from-node (index in
                           // relationship list)
  offset_t next_dest_rship; // next relationship of the to-node (index in
                            // relationship list)
  offset_t property_list;   // index in property list
  dcode_t rship_label;     // dictionary code for relationship type

  /**
   * Default constructor.
   */
  relationship() = default;

  /**
   * Constructor for a new relationship with the given label code which connects
   * the given two nodes.
   */
  relationship(dcode_t rlabel, offset_t src, offset_t dest)
      : id_(UNKNOWN), src_node(src), dest_node(dest),
        next_src_rship(UNKNOWN), next_dest_rship(UNKNOWN),
        property_list(UNKNOWN), rship_label(rlabel) {}

  relationship::id_t id() const { return id_; }

  /**
   * Returns the node identifier of the to-node.
   */
  node::id_t to_node_id() const { return dest_node; }

  /**
   * Returns the node identifier of the from-node.
   */
  node::id_t from_node_id() const { return src_node; }
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

  /**
   * Return true if a property with the given name exists.
   */
  bool has_property(const std::string& pname) const;

  bool operator==(const rship_description& other) const;
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
template <template <typename I> typename T>
class relationship_list {

struct init_rship_task {
  using range = std::pair<std::size_t, std::size_t>;
  init_rship_task(T<relationship> &r, std::size_t first, std::size_t last)
      : rships_(r), range_(first, last) {}

  void operator()() {
    auto iter = rships_.range(range_.first, range_.second);
    while (iter) {
      auto &r = *iter;
      r.runtime_initialize();
     ++iter;
    }
  }

  T<relationship> &rships_;
  range range_;
};

public:
  using vec = T<relationship>;
  using range_iterator = typename T<relationship>::range_iter;
  using iterator = typename T<relationship>::iter;

  /**
   * Constructors.
   */
  template <typename ... Args>
  relationship_list(Args&& ... args) : rships_(std::forward<Args>(args)...) {} 

  relationship_list(const relationship_list &) = delete;

  /**
   * Destructor.
   */
  ~relationship_list() = default;

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
  // spdlog::info("starting {} init tasks for rships...", num_chunks() / nchunks + 1);
  thread_pool pool;
  std::size_t start = 0, end = nchunks - 1;
  while (start < num_chunks()) {
    res.push_back(pool.submit(
        init_rship_task(*this, start, end)));
    // spdlog::info("starting: {}, {}", start, end);
    start = end + 1;
    end += nchunks;
  }
 // std::cout << "waiting ..." << std::endl;
  for (auto &f : res)
    f.get();
#else
  for (auto &r : rships_) {
    r.runtime_initialize();
  }
#endif   
  }

  /**
   * Add a new relationship to the list and return its identifier. The
   * relationship is inserted into the first available slot, i.e. to reuse space
   * of deleted records.
   * If owner != 0 then the newly created relationship is locked by this owner
   * transaction. If a callback is given this function is called before the slot 
   * is reserved (used for undo logging).
   */
  relationship::id_t add(relationship &&r, xid_t owner = 0) {
  if (rships_.is_full())
    rships_.resize(1);

  auto id = rships_.first_available();
  assert(id != UNKNOWN);
  r.id_ = id;
  if (owner != 0) {
    /// spdlog::info("lock relationship #{} by {}", id, owner);
    r.lock(owner);
  }
  rships_.store_at(id, std::move(r));
  return id;    
  }

  relationship::id_t insert(relationship &&r, xid_t owner = 0, std::function<void(offset_t)> callback = nullptr) {
  auto p = rships_.store(std::move(r), callback);
  p.second->id_ = p.first;
  if (owner != 0) {
    /// spdlog::info("lock relationship #{} by {}", p.first, owner);
    p.second->lock(owner);
  }
  return p.first;    
  }

  /**
   * Append a new relationship to the list and return its identifier. In
   * contrast to add the relationship is appended at the end of the list without
   * checking for available slots.
   * If owner != 0 then the newly created relationship is locked by this owner
   * transaction. If a callback is given this function is called before the slot 
   * is reserved (used for undo logging).
   */
  relationship::id_t append(relationship &&r, xid_t owner = 0, std::function<void(offset_t)> callback = nullptr) {
  auto p = rships_.append(std::move(r), callback);
  p.second->id_ = p.first;
  if (owner != 0) {
    /// spdlog::info("lock relationship #{} by {}", p.first, owner);
    p.second->lock(owner);
  }
  return p.first;    
  }

  /**
   * Get a relationship via its identifier.
   */
  relationship &get(relationship::id_t id) {
  if (rships_.capacity() <= id) {
    spdlog::warn("unknown relationship_id {}", id);
    throw unknown_id();
  }
  auto &r = rships_.at(id);
  return r;    
  }

  /**
   * Remove a certain relationship specified by its identifier.
   */
  void remove(relationship::id_t id) {
  if (rships_.capacity() <= id)
    throw unknown_id();
  rships_.erase(id);    
  }

  /**
   * Return the last entry (i.e. relationship) of the list of relationships
   * of a node which starts at the relationship identifier id.
   */
  relationship &last_in_from_list(relationship::id_t id) {
  relationship *rship = &get(id);
  while (rship->next_src_rship != UNKNOWN) {
    rship = &get(rship->next_src_rship);
  }
  return *rship;    
  }

  /**
   * Return the last entry (i.e. relationship) of the list of relationships
   * of a node which end at the relationship identifier id.
   */
  relationship &last_in_to_list(relationship::id_t id) {
  relationship *rship = &get(id);
  while (rship->next_dest_rship != UNKNOWN) {
    rship = &get(rship->next_dest_rship);
  }
  return *rship;    
  }

  /**
   * Returns the underlying vector of the relationship list.
   */
  auto &as_vec() { return rships_; }

  /**
   * Return a range iterator to traverse the relationship_list from first_chunk to
   * last_chunk.
   */
  range_iterator range(std::size_t first_chunk, std::size_t last_chunk) {
    return rships_.range(first_chunk, last_chunk);
  }

  /**
   * Output the content of the relationship vector.
   */
  void dump() {
  std::cout << "------- RELATIONSHIPS -------\n";
  for (auto& r : rships_) {
    std::cout << "#" << r.id() 
      << " [ txn-id=" << short_ts(r.txn_id()) << ", bts=" << short_ts(r.bts())
              << ", cts=" << short_ts(r.cts()) << ", dirty=" << r.d_->is_dirty_ 
              << " ], label = " << r.rship_label << ", " << r.src_node
              << "->" << r.dest_node << ", next_src=" << uint64_to_string(r.next_src_rship) << ", next_dest="
              << uint64_to_string(r.next_dest_rship);
    if (r.has_dirty_versions()) {
      // print dirty list
      std::cout << " {\n";
       for (const auto& dr : *(r.dirty_list())) {
        std::cout << "\t( @" << (unsigned long)&(dr->elem_)
                  << ", txn-id=" << short_ts(dr->elem_.txn_id())
                  << ", bts=" << short_ts(dr->elem_.bts()) << ", cts=" << short_ts(dr->elem_.cts())
                  << ", label=" << dr->elem_.rship_label
                  << ", dirty=" << dr->elem_.is_dirty();
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
  std::size_t num_chunks() const { return rships_.num_chunks(); }

private:
  T<relationship> rships_; // the actual list of relationships
};

#endif
