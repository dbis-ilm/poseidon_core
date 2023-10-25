/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#include <boost/algorithm/string.hpp>

#include "graph_db.hpp"
#include "spdlog/spdlog.h"
#include "query_builder.hpp"
#include <iostream>


void graph_db::poseidon_to_csr(csr_arrays &csr, rship_weight weight_func, bool bidirectional) {
#if defined CSR_DELTA
  if (delta_store_->delta_mode_) {
    // we use weight_func and bidirectional as per the delta store
    #ifdef USE_GUNROCK
    device_csr_update_with_delta(std::make_shared<graph_db>(*this), csr);
    #else
    host_csr_update_with_delta(csr);
    #endif
  }
  else {
    #ifdef PARALLEL_CSR_BUILD
    parallel_host_csr_build(csr, weight_func, bidirectional);
    #elif defined USE_GUNROCK
    device_csr_build(std::make_shared<graph_db>(*this), csr, weight_func, bidirectional);
    #else
    host_csr_build(csr, weight_func, bidirectional);
    #endif
  }
#elif defined PARALLEL_CSR_BUILD
  parallel_host_csr_build(csr, weight_func, bidirectional);
#else
  host_csr_build(csr, weight_func, bidirectional);
#endif
}

#ifndef USE_GUNROCK

void graph_db::host_csr_build(csr_arrays &csr, rship_weight weight_func, bool bidirectional) {
  auto txid = current_transaction()->xid();

  offset_t edges = 0;
  offset_t max_num_edges = rships_->as_vec().last_used() + 1;
  auto &cv_nodes = nodes_->as_vec();
  offset_t max_num_nodes = cv_nodes.last_used() + 1;

  auto &row_offsets = csr.row_offsets;
  auto &col_indices = csr.col_indices;
  auto &edge_values = csr.edge_values;

  int multi = bidirectional ? 1 : 2;
  row_offsets.reserve(max_num_nodes + 1);
  col_indices.reserve(multi * max_num_edges);
  edge_values.reserve(multi * max_num_edges);

  row_offsets.push_back(0); // First value is always 0

  for (offset_t nid = 0; nid < max_num_nodes; nid++) {
    if (cv_nodes.is_used(nid)) {
      auto& n = node_by_id(nid);

      auto rid = n.from_rship_list;
      while (rid != UNKNOWN) {
        auto &r = rship_by_id(rid);
        if (r.is_valid_for(txid)) {
          col_indices.push_back(r.to_node_id());
          edge_values.push_back(weight_func(r));
          edges++;
        }
        rid = r.next_src_rship;
      }

      if (bidirectional) {
        rid = n.to_rship_list;
        while (rid != UNKNOWN) {
          auto &r = rship_by_id(rid);
          if (r.is_valid_for(txid)) {
            col_indices.push_back(r.from_node_id());
            edge_values.push_back(weight_func(r));
            edges++;
          }
          rid = r.next_dest_rship;
        }
      }
    }
    row_offsets.push_back(edges);
  }
#if defined CSR_DELTA
  delta_store_->last_txn_id_ = txid; // update id of the last transaction that made a CSR update
  delta_store_->last_node_id_ = row_offsets.size() - 2; // update last node id in the CSR

  bool clear = true;
  for (auto &rec : delta_store_->delta_recs_) {
    if (rec.txid_ < txid) {
      // the modifications of transaction with id "txid_" has been included in the CSR build
      // therefore, the delta record is not needed later for CSR update
      rec.merged_ = true;
    }
    else if (rec.txid_ > txid) {
      // txid_ started after txid but committed before it
      // updates by txid_ were not include in the CSR build, since they are not visible to txid

      // however, this delta record is needed later for CSR update
      // therefore, we do not clear the vector of delta records after the CSR build
      clear = false;
    }
  }
  if (clear) {
    // no delta record is needed later for updating CSR
    delta_store_->clear_deltas();
  }

  delta_store_->row_offsets_ = row_offsets;
  delta_store_->col_indices_ = col_indices;
  delta_store_->edge_values_ = edge_values;
#endif
}
