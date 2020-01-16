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

#include <cassert>
#include <chrono>

#include "spdlog/spdlog.h"

#include "transaction.hpp"

thread_local transaction_ptr current_transaction_;

// TODO: replace by atomic counter
xid_t now2Xid() {
  using namespace std::chrono;
  // Get current time with native precision
  auto now = system_clock::now();
  // Convert time_point to signed integral type
  return now.time_since_epoch().count();
}

transaction::transaction() : xid_(now2Xid()) {}

void transaction::add_dirty_object(node *n) { dirty_nodes_.push_back(n); }

void transaction::add_dirty_object(relationship *r) {
  dirty_rships_.push_back(r);
}

void check_tx_context() {
#ifdef USE_TX
  if (!current_transaction_)
    throw out_of_transaction_scope();
#endif
}

transaction_ptr current_transaction() { return current_transaction_; }