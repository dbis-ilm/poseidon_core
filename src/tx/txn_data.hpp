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

#ifndef txn_data_hpp_
#define txn_data_hpp_

#include "defs.hpp"
#include "exceptions.hpp"
#include "properties.hpp"
#include <atomic>
#include <list>
#include "ts_list.hpp"
#include "transaction.hpp"

/**
 * This template encapsulates all member variables and methods
 * used for multiversion transaction processing;
 */
template <typename T> struct txn_data {
  // using dirty_list_t = std::list<T>;
  using dirty_list_t = ts_list<T>;
  using dirty_list_ptr = dirty_list_t*; // typedef for the list of dirty objects from
                                        // currently active transactions

  timestamp_t bts_, cts_, rts_;         // begin timestamp, commit timestamp, read timestamp
  dirty_list_ptr dirty_list_;           // the list of dirty objects
  bool is_dirty_;                       // true if the object represents a dirty object

  /**
   * Constructor
   */
  txn_data() : bts_(0), cts_(INF), rts_(0), dirty_list_(nullptr), is_dirty_(false) {}

  /**
   * Copy Constructor
   */
  txn_data(const txn_data &n)
      : bts_(n.bts_), cts_(n.cts_), rts_(n.rts_),  
        dirty_list_(n.dirty_list_), is_dirty_(n.is_dirty_) {}

  /**
   * Destructor
   */
  ~txn_data() { /* don't delete dirty_list_ here - it will be deleted at other places! */ }

 /**
   * Copy assignment operator.
   */
  txn_data &operator=(const txn_data &t) {
    bts_ = t.bts_;
    cts_ = t.cts_;
    rts_ = t.rts_;
    is_dirty_ = t.is_dirty_;
    dirty_list_ = t.dirty_list_;
    return *this;
  }
    
  /**
   * Move assignment operator to move resources.
   */
  txn_data &operator=(txn_data &&t) {
    bts_ = t.bts_;
    cts_ = t.cts_;
    rts_ = t.rts_;
    is_dirty_ = t.is_dirty_;
    dirty_list_ = t.dirty_list_;
    t.dirty_list_ = nullptr; //After moving resource from source, reset its pointer.
    return *this;
  }

};

/**
 * Base structure for transactional objects. It stores the transaction_id as lock 
 * and the txn_data structure as a volatile pointer.
 */ 
template <typename T> struct txn {
  using txn_data_t = txn_data<T>;
  using txn_data_ptr = txn_data_t *;

  std::atomic<xid_t> txn_id_;       // transaction id if locked, 0 otherwise
  timestamp_t bts_, cts_, rts_;  // begin timestamp, commit timestamp, read timestamp
  txn_data_ptr d_;                  // pointer to volatile data!!

  /**
   * Default constructor.
   */
  txn() : txn_id_(0), bts_(0), cts_(INF), rts_(0), d_(nullptr) { /*d_ = new txn_data_t();*/ }

  /**
   * Copy constructor.
   */
  txn(const txn &t) : txn_id_(t.txn_id_.load()), bts_(0), cts_(INF), rts_(0) { d_ = new txn_data_t(*t.d_); }

  /**
   * Destructor
   */
  ~txn() { if (d_) delete d_; }

  void runtime_initialize() { 
    txn_id_ = 0;
    bts_ = rts_ = 0; cts_ = INF;
    d_ = nullptr;
  }

  /**
   * Prepare the volatile part of an object, i.e. create the volatile
   * txn_data structure with timestamps and dirty_list.
   */
  void prepare() {
    if (!d_) d_ = new txn_data_t(); 
  }

  /**
   * Copy assignment operator.
   */
  txn &operator=(const txn &t) {
    d_ = (t.d_ != nullptr) ? new txn_data_t(*t.d_) : nullptr;
    // d_ = new txn_data_t(*t.d_);
    txn_id_ = t.txn_id_.load();
    bts_ = t.bts_;
    cts_ = t.cts_;
    rts_ = t.rts_;
    return *this;
  }

  /**
   * Move assignment operator to move resources.
   */
  txn &operator=(txn &&t) {
    d_ = t.d_;
    t.d_ = nullptr;
    txn_id_ = t.txn_id_.load();
    bts_ = t.bts_;
    cts_ = t.cts_;
    rts_ = t.rts_;
    return *this;
  }

  /* ---------------- concurrency control ---------------- */

  inline xid_t txn_id() const { return txn_id_.load(); }
  /**
   * Return the value of the begin timestamp.
   */
  inline timestamp_t bts() const { return bts_; }

  /**
   * Return the value of the commit timestamp.
   */
  inline timestamp_t cts() const { return cts_; }

  /**
   * Return the value of the read timestamp.
   */
  inline timestamp_t rts() const { return rts_; }

  /**
   * Set the begin and commit timestamps.
   */
  void set_timestamps(xid_t beg, xid_t end) {
    bts_ = beg;
    cts_ = end;
  }

  /**
   * Set the commit timestamp.
   */
  void set_cts(xid_t end) { cts_ = end; }

  /**
   * Set the read timestamp.
   */
  void set_rts(xid_t end) { 
    // update only if rts < end
    if (rts_ < end) 
      rts_ = end; 
  }

  /**
   * Return true if the node is locked by a transaction.
   */
  bool is_locked() const { return txn_id_ != 0; }

  /**
   * Checks if the node is already locked by a transaction with the given xid.
   */
  bool is_locked_by(xid_t xid) const { return txn_id_ == xid; }

  /**
   * Locks the object. xid is the id of the owner transaction.
   */
  void lock(xid_t xid) {
    xid_t expected = 0;
    while (!txn_id_.compare_exchange_weak(expected, xid) && expected != xid)
      ;
  }

  /**
   * Release the lock.
   */
  void unlock() { txn_id_ = 0; }

  /**
   * Try to lock the object and return true if successful. Otherwise,
   * false is returned.
   */
  bool try_lock(xid_t xid) {
    xid_t expected = 0;
    return txn_id_.compare_exchange_strong(expected, xid);
  }

  /* ---------------- dirty object handling ---------------- */

  /**
   * Set the dirty flag to true, i.e. indicating that the object
   * (node/relationship) is a dirty object where properties are kept separately
   * from the properties_ table.
   */
  void set_dirty() { d_->is_dirty_ = true; }

  /**
   * Check of the dirty flag is set. In this case the object is stored in volatile memory.
   */
  bool is_dirty() const { return d_ == nullptr ? false : d_->is_dirty_; }

  /**
   * Check if the node is valid for the transaction with the give xid.
   */
  bool is_valid_for(xid_t xid) const { 
	  return d_ == nullptr || (bts_ <= xid && xid < cts_); 
  }

  /**
   * Check whether the object is valid, i.e. not modified by an active transaction.
   */
  bool is_valid() const { return /*d_ == nullptr ||*/ cts_ == INF; }

  /**
   * Return the dirty list.
   */
  decltype(auto) dirty_list() { return d_->dirty_list_; }

  std::mutex& dirty_list_mutex() { return d_->dirty_list_->mtx_; }

  /**
   * Delete the dirty list and sets it back to nullptr.
   */
  void delete_dirty_list() { 
    delete d_->dirty_list_; 
    d_->dirty_list_ = nullptr;
  }

  /**
   * Return true if dirty copies of this node exist, i.e. if
   * other active transactions working on it.
   */
  bool has_dirty_versions() const { return d_ != nullptr && d_->dirty_list_ != nullptr && !d_->dirty_list_->empty(); }

  /**
   * Find a valid version from the list of objects (stored in the dirty list)
   * that is valid for the transaction with the given xid by checking bts and
   * cts timestamps.
   */
  const T& find_valid_version(xid_t xid) {
    if (has_dirty_versions()) {
      bool abort = false;
      std::lock_guard<std::mutex> guard (dirty_list_mutex());
      for (const auto& dn : *(d_->dirty_list_)) {
       if (!dn->elem_.is_locked() || dn->elem_.is_locked_by(xid)) {
        if (dn->elem_.is_valid_for(xid))
          return dn;
        else {
          // if the object is locked by us but not valid, then we have it 
          // already deleted!
          if (dn->elem_.is_locked_by(xid) && (dn->elem_.bts() == dn->elem_.cts()))
            throw unknown_id();

          // if the object is not locked but we cannot find a valid version
          // then we probably should abort the transaction instead of
          // throw unknown_id: but let's first check all versions
          abort = true;
        }
       } 
      }
      if (abort)
        // no valid version found -> abort the transaction
        throw transaction_abort();
    }
    throw unknown_id();
  }

  /**
   * Check if a version exists in the list of objects (stored in the dirty list)
   * that is valid for the transaction with the given xid by checking bts and
   * cts timestamps.
   */
  bool has_valid_version(xid_t xid) {
    if (has_dirty_versions()) {
      std::lock_guard<std::mutex> guard(dirty_list_mutex());
      for (const auto& dn : *(d_->dirty_list_)) {
       if (!dn->elem_.is_locked() || dn->elem_.is_locked_by(xid)) {
        if (dn->elem_.is_valid_for(xid))
          return true;
        else {
          // if the object is locked by us but not valid, then we have it 
          // already deleted!
          if (dn->elem_.is_locked_by(xid) && (dn->elem_.bts() == dn->elem_.cts()))
            return false;
        }
       } 
      }
    }
    return false;
  }

  /**
   * Retrieve the dirty object version belonging to the transaction with the
   * given xid.
   */
  const T& get_dirty_version(xid_t xid) {
    if (has_dirty_versions()) {
      std::lock_guard<std::mutex> guard(dirty_list_mutex());
      for (const auto& dn : *(d_->dirty_list_)) {
        if (dn->elem_.txn_id() == xid) // TODO: !!!!
          return dn;
      }
    }
    throw unknown_id();
  }

  /**
   * Return a pointer to the dirty list if it exists and has dirty versions.
   * Currently used in Garbage collection Test case.
   */
  decltype(auto) get_dirty_objects() const {
    using dirty_list_ptr = const typename txn_data<T>::dirty_list_t*;
    return has_dirty_versions() 
      ? std::optional<dirty_list_ptr>(d_->dirty_list_) 
      : std::optional<dirty_list_ptr>{};
  }

  /**
   * Remove the dirty object versions belonging to the transaction with the
   * given xid.
   */
  void remove_dirty_version(xid_t xid) {
    if (has_dirty_versions()) {
      std::lock_guard<std::mutex> guard(dirty_list_mutex());
      auto iter =
          std::remove_if(dirty_list()->begin(), dirty_list()->end(), [&](const T& dn) {
            return dn->elem_.txn_id_ == xid && dn->elem_.cts() == INF;
          });
      // assert(iter != dirty_list->end());
      dirty_list()->erase(iter, dirty_list()->end());
    }
  }

  /**
   * Add a new dirty object to the list of dirty objects and return a reference
   * of the newly inserted object.
   */
  T& add_dirty_version(T&& tptr) {
    prepare();
    if (!dirty_list()) {
      d_->dirty_list_ = new typename txn_data<T>::dirty_list_t;
    }
 
    std::lock_guard<std::mutex> guard(dirty_list_mutex());
    tptr->elem_.prepare();
    tptr->elem_.d_->dirty_list_ = this->dirty_list();
    d_->dirty_list_->push_front(std::move(tptr));

    return dirty_list()->front();
  }

  /**
   * Check if the version belonging to transaction given by xid was updated. In this case
   * return true. Otherwise, the object was added and return false.
   */
  bool updated_in_version(xid_t xid) {
    if (!dirty_list()) return false;
    
    std::lock_guard<std::mutex> guard(dirty_list_mutex());
    for (const auto& dn : *(d_->dirty_list_)) {
        if (dn->elem_.txn_id_ == xid) 
          return dn->updated();
    }
    return false;
  }

  /**
   * Perform garbage collection by deleting all dirty nodes which are
   * not used anymore.
   */
  void gc(xid_t oldest) {
    // we can safely delete all elements from the dirty list where cts <= txn
    // of the oldest active transaction
    if (has_dirty_versions()) {
      std::lock_guard<std::mutex> guard(dirty_list_mutex());
      // spdlog::info("GC: remove everything smaller than {}: #{} elements",
      //             oldest, dirty_list->size());
      dirty_list()->remove_if([oldest](const T& dn) { return dn->elem_.cts() <= oldest; });
      // spdlog::info("GC done: #{} elements", dirty_list->size());
    }
    //Optional: After garbage collection, if there are no more versions, then we can delete the list.
    if (!has_dirty_versions()) {
     delete_dirty_list();
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