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

#ifndef transaction_hpp_
#define transaction_hpp_

#include "defs.hpp"
#include "exceptions.hpp"
#include "properties.hpp"
#include <atomic>
#include <list>

/**
 * Typedef for transaction ids.
 */
using xid_t = unsigned long;

/**
 * Typedef for timestamps.
 */
using timestamp_t = unsigned long;

/**
 * Value for infinity timestamp.
 */
constexpr unsigned long INF = std::numeric_limits<unsigned long>::max();

struct node;
struct relationship;

/**
 * A transaction represents the atomic unit of execution graph operations.
 */
class transaction {
public:
  /**
   * Default constructor. Shouldn't be used directly, because transactions are
   * created only via the begin_transaction() method of the class graph_db.
   */
  transaction();

  /**
   * Default destructor.
   */
  ~transaction() = default;

  /**
   * Returns the transaction id (=timestamp).
   */
  xid_t xid() const { return xid_; }

  /**
   * Add the given node to the vector of dirty node objects.
   */
  void add_dirty_node(offset_t id);

  /**
   * Add the given relationship to the vector of dirty relationships objects.
   */
  void add_dirty_relation(offset_t id);

  /**
   * Return the list of dirty nodes modified in this transaction.
   */
   std::vector<offset_t>& dirty_nodes() { return dirty_nodes_; }

  /**
   * Return the list of dirty relationships modified in this transaction.
   */
   std::vector<offset_t>& dirty_relationships() { return dirty_rships_; }

private:
  xid_t xid_; // transaction identifier
  std::vector<offset_t>
      dirty_nodes_; // the vector  of node ids of nodes which were modified by this transaction
  std::vector<offset_t> dirty_rships_; // the vector of relationship ids or relationships which
                                       // were modified by this transaction
};

using transaction_ptr = std::shared_ptr<transaction>;

/**
 * The currently active transaction is stored as thread local attribute.
 */
extern thread_local transaction_ptr current_transaction_;

/**
 * Check whether we are inside an active transaction that is associated
 * with the current thread. If not then an exception is raised.
 */
void check_tx_context();

/**
 * Return a pointer to the active transaction.
 */
transaction_ptr current_transaction();

/**
 * This template encapsulates all member variables and methods
 * used for multiversion transaction processing;
 */
template <typename T> struct txn {
  timestamp_t bts, cts;      // begin timestamp, commit timestamp
  std::atomic<xid_t> txn_id; // transaction id if locked, 0 otherwise
  bool is_dirty_;            // true if the object represents a dirty object

  using dirty_list_ptr =
      std::list<T> *;        // typedef for the list of dirty objects from
                             // currently active transactions
  dirty_list_ptr dirty_list; // the list of dirty objects

  /**
   * Default constructor.
   */
  txn() : bts(0), cts(INF), txn_id(0), is_dirty_(false), dirty_list(nullptr) {}

  /**
   * Copy constructor.
   */
  txn(const txn &n)
      : bts(n.bts), cts(n.cts), txn_id(n.txn_id.load()), is_dirty_(n.is_dirty_),
        dirty_list(n.dirty_list) {}

  /**
   * Copy assignment operator.
   */
  txn &operator=(const txn &t) {
    bts = t.bts;
    cts = t.cts;
    txn_id = t.txn_id.load();
    is_dirty_ = t.is_dirty_;
    dirty_list = t.dirty_list;
    return *this;
  }

  /**
   * Move assignment operator to move resources.
   */
  txn &operator=( txn &&t) {
    bts = t.bts;
    cts = t.cts;
    txn_id = t.txn_id.load();
    is_dirty_ = t.is_dirty_;
    dirty_list = t.dirty_list;
    t.dirty_list = nullptr; //After moving resorce from source, reset its pointer.
    return *this;
  }

  /* ---------------- concurrency control ---------------- */
  /**
   * Set the begin and commit timestamps.
   */
  void set_timestamps(xid_t beg, xid_t end) {
    bts = beg;
    cts = end;
  }

  /**
   * Set the commit timestamps.
   */
  void set_cts(xid_t end) { cts = end; }

  /**
   * Return true if the node is locked by a transaction.
   */
  bool is_locked() const { return txn_id != 0; }

  /**
   * Checks if the node is already locked by a transaction with the given xid.
   */
  bool is_locked_by(xid_t xid) const { return txn_id == xid; }

  /**
   * Locks the object. xid is the id of the owner transaction.
   */
  void lock(xid_t xid) {
    xid_t expected = 0;
    while (!txn_id.compare_exchange_weak(expected, xid) && expected != xid)
      ;
  }

  /**
   * Release the lock.
   */
  void unlock() { txn_id = 0; }

  /**
   * Try to lock the object and return true if successful. Otherwise,
   * false is returned.
   */
  bool try_lock(xid_t xid) {
    xid_t expected = 0;
    return txn_id.compare_exchange_strong(expected, xid);
  }

  /* ---------------- dirty object handling ---------------- */

  /**
   * Set the dirty flag to true, i.e. indicating that the object
   * (node/relationship) is a dirty object where properties are kept separately
   * from the properties_ table.
   */
  void set_dirty() { is_dirty_ = true; }

  bool is_dirty() const { return is_dirty_; }

  /**
   * Check if the node is valid for the transaction with the give xid.
   */
  bool is_valid(xid_t xid) const { return bts <= xid && xid < cts; }

  /**
   * Return true if dirty copies of this node exist, i.e. if
   * other active transactions working on it.
   */
  bool has_dirty_versions() const { return dirty_list != nullptr && !dirty_list->empty(); }

  /**
   * Find a valid version from the list of objects (stored in the dirty list)
   * that is valid for the transaction with the given xid by checking bts and
   * cts timestamps.
   */
  const T& find_valid_version(xid_t xid) const {
    if (has_dirty_versions()) {
      for (const auto& dn : *dirty_list) {
        if (dn->elem_.is_valid(xid) &&
            (!dn->elem_.is_locked() || dn->elem_.is_locked_by(xid))) {
          return dn;
        }
      }
    }
    throw unknown_id();
  }

  /**
   * Retrieve the dirty object version belonging to the transaction with the
   * given xid.
   */
  const T& get_dirty_version(xid_t xid) {
    if (has_dirty_versions()) {
      for (const auto& dn : *dirty_list) {
        if (dn->elem_.txn_id == xid) // TODO: !!!!
          return dn;
      }
    }
    throw unknown_id();
  }

  /**
   * Remove the dirty object versions belonging to the transaction with the
   * given xid.
   */
  void remove_dirty_version(xid_t xid) {
    if (has_dirty_versions()) {
      auto iter =
          std::remove_if(dirty_list->begin(), dirty_list->end(), [&](const T& dn) {
            return dn->elem_.txn_id == xid && dn->elem_.cts == INF;
          });
      assert(iter != dirty_list->end());
      dirty_list->erase(iter, dirty_list->end());
    }
  }

  /**
   * Add a new dirty object to the list of dirty objects and return a reference
   * of the newly inserted object.
   */
  T& add_dirty_version(T&& tptr) {
    if (!dirty_list) //Cannot use  if(!has_dirty_versions()) as it will leak memory on heap
      dirty_list = new std::list<T>;
    tptr->elem_.dirty_list = dirty_list;
    dirty_list->push_front(std::move(tptr));

    return dirty_list->front();
  }

  /**
   * Perform garbage collection by deleting all dirty nodes which are
   * not used anymore.
   */
  void gc(xid_t oldest) {
    // we can safely delete all elements from the dirty list where cts <= txn
    // of the oldest active transaction
    if (has_dirty_versions()) {
      // spdlog::info("GC: remove everything smaller than {}: #{} elements",
      //             oldest, dirty_list->size());
      dirty_list->remove_if([oldest](const T& dn) { return dn->elem_.cts <= oldest; });
      // spdlog::info("GC done: #{} elements", dirty_list->size());
    }
    //Optional: After garbage collection, if there are no more versions, then we can delete the list.
    if (!has_dirty_versions()){
     delete  dirty_list;
     dirty_list = nullptr;
    }
  }
};

/**
 * dirty_object represents a version of a node or relationship kept in
 * volatile memory to implement MVTO. All these versions are managed in a
 * dirty object list associated with the persistent node.
 */
template <typename T> struct dirty_object {
  /**
   * Create a new dirty version from the object and the list of property
   * items.
   */
  dirty_object(const T &n, const std::list<p_item> &p, bool updated = true)
      : update_mode_(updated), properties_(p) {
    elem_ = n;
  }
  /**
   * Default destructor.
   */
  ~dirty_object() = default;

  /**
   * Returns true of the object was updated, false if the object was newly
   * inserted.
   */
  bool updated() const { return update_mode_; }

  bool update_mode_; // true if the object was updated, false if it was
                     // inserted
  T elem_;           // the actual object
  std::list<p_item> properties_; // the list of property items
};

#endif
