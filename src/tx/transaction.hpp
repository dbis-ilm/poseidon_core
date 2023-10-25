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
#include <set>
#include "ts_list.hpp"
#include "analytics.hpp"

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

/**
 * A helper function for debugging to return a shortened version of the timestamps.
 */
inline int short_ts(timestamp_t ts) {
  return (ts == INF) ? -1 : static_cast<int>(ts & 0xffff);
}

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
  void add_dirty_relationship(offset_t id);

  /**
   * Return the list of dirty nodes modified in this transaction.
   */
   std::vector<offset_t>& dirty_nodes() { return dirty_nodes_; }

  /**
   * Return the list of dirty relationships modified in this transaction.
   */
   std::vector<offset_t>& dirty_relationships() { return dirty_rships_; }

  /**
   * Store the id of the log associated with this transaction.
   * NOTE: Used only for PMDK.
   */
  void set_logid(std::size_t lid) { logid_ = lid; }

  /**
   * Return the id of the log associated with this transaction.
   */
  std::size_t logid() const { return logid_; }

private:
  xid_t xid_; // transaction identifier
  std::size_t logid_; // log identifier
  std::vector<offset_t>
      dirty_nodes_; // the vector of ids of nodes which were modified by this transaction
  std::vector<offset_t> dirty_rships_; // the vector of ids of relationships which
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

#endif
