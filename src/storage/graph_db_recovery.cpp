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

#include <filesystem>
#include "graph_db.hpp"
#include "spdlog/spdlog.h"
#include "thread_pool.hpp"
#include "common_log.hpp"

using namespace boost::posix_time;

void graph_db::apply_redo(wa_log& log, wa_log::log_iter& li) {
  switch (li.log_type()) {
  case log_tx:
    // do nothing
    break;
  case log_insert:
  case log_update:
    if (li.obj_type() == log_node) {
      // insert or update node
      auto rec = li.get<wal::log_node_record>();
      spdlog::info("REDO: insert node #{}", rec->oid);
      node n(rec->after.label);
      n.id_ = rec->oid;
      n.from_rship_list = rec->after.from_rship_list;
      n.to_rship_list = rec->after.to_rship_list;
      n.property_list = rec->after.property_list;
      try {
        nodes_->as_vec().store_at(rec->oid, std::move(n));
      } catch (index_out_of_range& exc) {
        spdlog::info("WARNING: page for node @{} doesn't exist - last={}, is_used={}", rec->oid, nodes_->as_vec().last_used(), nodes_->as_vec().is_used(rec->oid));
        nodes_->as_vec().store_at(rec->oid, std::move(n));
      }

    }
    else if (li.obj_type() == log_rship) {
      // insert or update relationship
      auto rec = li.get<wal::log_rship_record>();
      spdlog::info("REDO: insert rship #{}", rec->oid);
      relationship r;
      r.rship_label = rec->after.label;
      r.src_node = rec->after.src_node;
      r.dest_node = rec->after.dest_node;
      r.next_src_rship = rec->after.next_src_rship;
      r.next_dest_rship = rec->after.next_dest_rship;
      rships_->as_vec().store_at(rec->oid, std::move(r));
    }
    else if (li.obj_type() == log_property) {
      // insert or update properties
    }
    else if (li.obj_type() == log_dict) {
      // insert string in dictionary
      auto rec = li.get<wal::log_dict_record>();
      auto new_code = dict_->insert(std::string(rec->str));
      spdlog::info("REDO: insert dictionary key {}({})->{}", rec->code, new_code, rec->str);
    }
    break;
  case log_delete:
    if (li.obj_type() == log_node) {
      // delete node
      auto rec = li.get<wal::log_node_record>();
      spdlog::info("REDO: delete node #{}", rec->oid);
      nodes_->remove(rec->oid);
    }
    else if (li.obj_type() == log_rship) {
      // delete relationship
      auto rec = li.get<wal::log_rship_record>();
      spdlog::info("REDO: delete rship #{}", rec->oid);
      rships_->remove(rec->oid);
    }
    else if (li.obj_type() == log_property) {
        // delete properties
    }
    break;
  default:
    // we ignore checkpoints 
    break;
  }
}

void graph_db::apply_undo(wa_log& log, xid_t txid, offset_t pos) {

}

void graph_db::apply_log() {
  // map of transaction id, offset of the last log entry for this transaction
  std::map<xid_t, offset_t> loser_tx;
  uint64_t max_lsn = 0;
  bool redo_performed = false;
  
  std::filesystem::path path_obj(pool_path_);
  path_obj /= database_name_;
  std::string prefix = path_obj.string() + "/";

  spdlog::info("processing log file...");
  wa_log log(prefix + "poseidon.wal");

  // 1. analyze log: find winners and losers
  for(auto li = log.log_begin(); li != log.log_end(); ++li) {
    if (li.log_type() == log_tx) {
      auto tx_log = li.get<wal::log_tx_record>();
      switch (tx_log->tx_cmd) {
        case log_bot:
          loser_tx.emplace(li.transaction_id(), li.log_position()); break;
        case log_commit:
          loser_tx.erase(li.transaction_id()); break;
        case log_abort:
          loser_tx[li.transaction_id()] = li.log_position(); break;
      }
    }
    else if (li.log_type() == log_chkpt) {
      // we found a checkpoint, i.e. all redo log entries 
      // before this LSN can be ignored
      max_lsn = li.lsn();
    }
    else {
      loser_tx[li.transaction_id()] = li.log_position();
    }
  }
  spdlog::info("recovery from log: {} losers, starting at LSN #{}", loser_tx.size(), max_lsn);

  // 2. apply redo
  for(auto li = log.log_begin(); li != log.log_end(); ++li) {
    if (li.lsn() > max_lsn && li.obj_type() != log_none) {
      redo_performed = true;
      apply_redo(log, li);
    }
  }
  if (redo_performed) {
    flush();
    log.checkpoint();
  }
  
  // 3. apply undo 
  for (auto it = loser_tx.begin(); it != loser_tx.end(); it++) {
    apply_undo(log, it->first, it->second);
  }
}
