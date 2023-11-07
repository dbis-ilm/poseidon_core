/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include <filesystem>

#include "graph_db.hpp"
#include "vec.hpp"
#include "spdlog/spdlog.h"
#include <iostream>
#include <stdio.h>
#include <set>
#include <variant>

#define UNDO_CB nullptr

void graph_db::destroy(graph_db_ptr gp) {
  std::filesystem::path path_obj(gp->database_name_);
  if (std::filesystem::exists(path_obj))
    std::filesystem::remove_all(path_obj);
}

void graph_db::prepare_files(const std::string &pool_path, const std::string &pfx) {
  spdlog::debug("graph_db: prepare files {} / {}", pool_path, pfx);
  std::filesystem::path path_obj(pool_path);
  path_obj /= pfx;
  // check if path exists and is of a regular file
  if (! std::filesystem::exists(path_obj))
    std::filesystem::create_directory(path_obj);

  std::string prefix = path_obj.string() + "/";

  spdlog::debug("graph_db: prepare files in '{}'", prefix);

  node_file_ = std::make_shared<paged_file>();
  node_file_->open(prefix + "nodes.db", NODE_FILE_ID);
  bpool_.register_file(NODE_FILE_ID, node_file_);

  rship_file_ = std::make_shared<paged_file>();
  rship_file_->open(prefix + "rships.db", RSHIP_FILE_ID);
  bpool_.register_file(RSHIP_FILE_ID, rship_file_);

  nprops_file_ = std::make_shared<paged_file>();
  nprops_file_->open(prefix + "nprops.db", NPROPS_FILE_ID);
  bpool_.register_file(NPROPS_FILE_ID, nprops_file_);

  rprops_file_ = std::make_shared<paged_file>();
  rprops_file_->open(prefix + "rprops.db", RPROPS_FILE_ID);
  bpool_.register_file(RPROPS_FILE_ID, rprops_file_);

  dict_ = p_make_ptr<dict>(bpool_, prefix);
}

graph_db::graph_db(const std::string &db_name, const std::string& pool_path) : database_name_(db_name) {
  pool_path_ = pool_path;
  prepare_files(pool_path, db_name);
  nodes_ = p_make_ptr<node_list<buffered_vec> >(bpool_, NODE_FILE_ID);
  rships_ = p_make_ptr<relationship_list<buffered_vec> >(bpool_, RSHIP_FILE_ID);
  node_properties_ = p_make_ptr<property_list<buffered_vec> >(bpool_, NPROPS_FILE_ID);
  rship_properties_ = p_make_ptr<property_list<buffered_vec> >(bpool_, RPROPS_FILE_ID);
  index_map_ = p_make_ptr<index_map>();
  restore_indexes(pool_path, db_name);
  active_tx_ = new std::map<xid_t, transaction_ptr>();
  /*
  m_ = new std::mutex();
  garbage_ = new gc_list();
  gcm_ = new std::mutex();
  */
 runtime_initialize();
}

graph_db::~graph_db() {
  // we don't delete persistent objects here!
  for (auto tp : *active_tx_) {
    // TODO: abort all active transactions
  }
  delete active_tx_;
  delete m_;
  delete garbage_;
  delete gcm_;
  current_transaction_.reset();
  close_files();
}

void graph_db::flush() {
  bpool_.flush_all();
}

void graph_db::flush(const std::set<offset_t>& dirty_chunks) {
  for (const auto& dc : dirty_chunks) {
    bpool_.flush_page(dc, false);
  }
}

void graph_db::close_files() {
  // spdlog::info("graph_db::close_files()");
  if (dict_) dict_->close_file();
  if (node_file_) node_file_->close();
  if (rship_file_) rship_file_->close();
  if (nprops_file_) nprops_file_->close();
  if (rprops_file_) rprops_file_->close();
  index_map_->clear();
  for (auto pf : index_files_) {
    pf->close();
  }
  if (walog_) {
    // if we close the log regularly, we can truncate it
    walog_->close(true);
  }
}


void graph_db::runtime_initialize() {
  spdlog::info("graph_db::runtime_initialize()");
  nodes_->runtime_initialize();
  rships_->runtime_initialize();
  // make sure the dictionary is initialized
  // dict_->initialize();

  // perform recovery using the log
  apply_log();

  std::string prefix = pool_path_ + "/" + database_name_;
  walog_ = p_make_ptr<wa_log>(prefix + "/poseidon.wal");

  // recreate volatile objects: active_tx_ table and mutex
  active_tx_ = new std::map<xid_t, transaction_ptr>();
  oldest_xid_ = 0;
  m_ = new std::mutex();
  gcm_ = new std::mutex();
  garbage_ = new gc_list();
}

bool graph_db::run_transaction(std::function<bool()> body) {
  begin_transaction();
  return body() ? commit_transaction() : abort_transaction();
}

void graph_db::begin_transaction() {
  if (current_transaction_)
    throw invalid_nested_transaction();
  auto tx = std::make_shared<transaction>();
  current_transaction_ = tx;
  walog_->transaction_begin(tx->xid());  
  std::lock_guard<std::mutex> guard(*m_);
  active_tx_->insert({tx->xid(), tx});
  // spdlog::info("begin transaction {}", tx->xid());
}

void graph_db::commit_dirty_node(transaction_ptr tx, node::id_t node_id) {
    // TODO: add WAL log entry for REDO
    auto xid = tx->xid();
 	  auto &n = nodes_->get(node_id);
    // If the node was already deleted we can skip all other entries...
    if (n.cts() != INF) {
      n.remove_dirty_version(0);
      return;
    }
	  /* A dirty object was just inserted, when add_node() or update_node() was executed.
	   * So there must be at least one dirty version.
	   */

	  if (!n.has_dirty_versions()) {
      // TODO: in case of inserts perform undo using the log
		  throw transaction_abort();
	  }

	  // get the version of dirty object.
	  // Note: Dirty version are always put in front of the list.
	  // If that order is changed, then same order must be used during access.
	  if (const auto& dn {n.dirty_list()->front()}; !dn->updated()) {
      /* ------------------------------------------
       *                  INSERT
       * -----------------------------------------*/
      // std::cout << "COMMIT INSERT" << std::endl;
		  // CASE #1 = INSERT: we have added a new node to node_list, but its properties
		  // are stored in a dirty_node object in this case, we simply copy the
		  // properties to property_list and release the lock
		  // spdlog::info("commit INSERT transaction {}: copy properties", xid);
      // TODO: handle properties
      auto log_rec = wal::create_insert_node_record(dn);
      walog_->append(xid, log_rec);
		  copy_properties(n, dn);
      n.node_label = dn->elem_.node_label;
      n.from_rship_list = dn->elem_.from_rship_list;
      n.to_rship_list = dn->elem_.to_rship_list;
		  // set bts/cts
		  n.set_timestamps(xid, INF);
	
      // TODO: handle the case of multiple indexes
      auto idx = get_index(dn->elem_.node_label, dn->properties_);
      if (idx.first.which() > 0) {
        spdlog::debug("NODE INDEX UPDATE: insert");
        index_insert(idx, dn->elem_.id(), dn->properties_);
      }
	    // we can already delete the object from the dirty version list
		  n.dirty_list()->pop_front();

	  } else {
		  // case #2: we have updated a node, thus copy both properties
		  // and node version to the main tables
		  // spdlog::info("commit UPDATE transaction {}: copy properties", xid);
		  // update node (label)

      if (dn->elem_.bts() == dn->elem_.cts()) {
        /* ------------------------------------------
         *                  DELETE
         * -----------------------------------------*/
        // std::cout << "COMMIT DELETE" << std::endl;
        // spdlog::info("COMMIT DELETE: [{},{}]", short_ts(dn->elem_.bts), short_ts(dn->elem_.cts));
        auto log_rec = wal::create_delete_node_record(n);
        walog_->append(xid, log_rec);
        // TODO: handle properties!!
        node_properties_->foreach_property_set(n.property_list, UNDO_CB);
        // Because there might be an active transaction which still needs the object
        // we cannot delete the node, yet. However, we set the bts and cts accordingly.
        {
          std::unique_lock<std::mutex> l(*gcm_);
          garbage_->push_back(gc_item { xid, node_id, gc_item::gc_node });
        }
		    n.set_cts(xid);
        // spdlog::info("===> COMMIT DELETE: #{}: [{},{}]", n.id(), short_ts(n.bts),short_ts(n.cts));

        // TODO: handle the case of multiple indexes
        auto props = node_properties_->build_dirty_property_list(n.property_list);
        auto idx = get_index(dn->elem_.node_label, props);
        if (idx.first.which() > 0) {
          spdlog::debug("NODE INDEX UPDATE: delete");
          index_delete(idx, n.id(), props);
        }

		    // we can already delete the object from the dirty version list
		    n.dirty_list()->pop_front();
		    // release the lock of the old version.
		    n.dirty_list()->front()->elem_.unlock();
        // we can delete properties because the old values are still in the dirty list
        node_properties_->remove_properties(n.property_list);
        n.property_list = UNKNOWN;

      }
      else {
        /* ------------------------------------------
         *                  UPDATE
         * -----------------------------------------*/
        // std::cout << "COMMIT UPDATE" << std::endl;
        // create and append a log_node_record BEFORE we copy the properties and override the label
        auto log_rec = wal::create_update_node_record(n, dn);
        walog_->append(xid, log_rec);
		    n.node_label = dn->elem_.node_label;
        n.from_rship_list = dn->elem_.from_rship_list;
        n.to_rship_list = dn->elem_.to_rship_list;
		    n.set_timestamps(xid, INF);
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
		    copy_properties(n, dn);
		    // spdlog::info("COMMIT UPDATE: set new={},{} @{}", xid, INF, n.id());
		    /// spdlog::info("COMMIT UPDATE: set old.cts={} @{}", xid,
		    ///             (unsigned long)&(dn->node_));
        // TODO: handle the case of multiple indexes
        assert(n.dirty_list()->size() > 1);
        auto it = n.dirty_list()->begin();
        it++;
        auto idx = get_index(dn->elem_.node_label, dn->properties_);
        if (idx.first.which() > 0) {
          spdlog::debug("NODE INDEX UPDATE: update: {}", n.dirty_list()->size());
          index_update(idx, dn->elem_.id(), (*it)->properties_, dn->properties_);
        }
		    // we can already delete the object from the dirty version list
		    n.dirty_list()->pop_front();
		    // release the lock of the old version.
		    n.dirty_list()->front()->elem_.unlock();
	    }
    }
	  // finally, release the lock of the persistent object and initiate the
	  // garbage collection
	  n.unlock();
	  n.gc(oldest_xid_);
  }

void graph_db::commit_dirty_relationship(transaction_ptr tx, relationship::id_t rel_id) {
    // TODO: add WAL log entry for REDO
  // std::cout << "commit_dirty_relationship" << std::endl;
  auto xid = tx->xid();
	auto& r = rships_->get(rel_id);
  // If the relationship was already deleted we can skip all other entries...
  if (r.cts() != INF) {
    r.remove_dirty_version(0);
    return;
  }

	  /* A dirty object was just inserted, when add_relation() or update_relation() was executed.
	   * So there must be at least one dirty version.
	   */
	  if (!r.has_dirty_versions()) {
      // TODO: in case of inserts perform undo using the log
		  throw transaction_abort();
	  }
	  // get the version of dirty object.
      // Note: Dirty versions are always put in front of the list.
      // If that order is changed, then same order must be used during access.
	  if(const auto& dr {r.dirty_list()->front()}; !dr->updated()) {
		  // CASE #1 = INSERT: we have added a new relationship to relationship_list, but
		  // its properties are stored in a dirty_rship object in this case, we
		  // simply copy the properties to property_list and release the lock
		  // set bts/cts
      // TODO: handle properties
      auto log_rec = wal::create_insert_rship_record(dr);
      walog_->append(xid, log_rec);
		  r.set_timestamps(xid, INF);
		  copy_properties(r, dr);

            // TODO: handle the case of multiple indexes
      auto idx = get_index(dr->elem_.rship_label, dr->properties_);
      if (idx.first.which() > 0) {
        spdlog::info("RSHIP INDEX UPDATE: insert");
        index_insert(idx, dr->elem_.id(), dr->properties_);
      }
		  // we can already delete the object from the dirty version list
		  r.dirty_list()->pop_front();
	  } else {
		  // case #2: we have updated a relationship, thus copy both properties
		  // and relationship version to the main tables
		  // update relationship (label)
      if (dr->elem_.bts() == dr->elem_.cts()) {
        // CASE #2 = DELETE
        // spdlog::info("COMMIT DELETE: {}, {}", dr->elem_.bts(), dr->elem_.cts());
        auto log_rec = wal::create_delete_rship_record(r);
        walog_->append(xid, log_rec);
        // TODO: handle properties!!
        rship_properties_->foreach_property_set(r.property_list, UNDO_CB);

        // Because there might be an active transaction which still needs the object
        // we cannot delete the relationship, yet. However, we set the cts accordingly.
        // TODO: make sure that r is eventually removed from the rships_ table!!
        {
          std::unique_lock<std::mutex> l(*gcm_);
          garbage_->push_back(gc_item { xid, rel_id, gc_item::gc_rship });
        }
		    r.set_cts(xid);
        
        // TODO: handle the case of multiple indexes
        auto props = rship_properties_->build_dirty_property_list(r.property_list);
        auto idx = get_index(dr->elem_.rship_label, props);
        if (idx.first.which() > 0) {
          spdlog::info("RSHIP INDEX UPDATE: insert");
          index_delete(idx, r.id(), props);
        }

		    // we can already delete the object from the dirty version list
		    r.dirty_list()->pop_front();
		    // release the lock of the old version.
		    r.dirty_list()->front()->elem_.unlock();
        rship_properties_->remove_properties(r.property_list);
      }
      else {
        // CASE #3 = UPDATE
      // create and append a log_rship_record BEFORE we copy the properties and override the label
      auto log_rec = wal::create_update_rship_record(r, dr);
      walog_->append(xid, log_rec);
		  r.set_timestamps(xid, INF);
		  r.rship_label = dr->elem_.rship_label;
		  copy_properties(r, dr);

      // TODO: handle the case of multiple indexes
      assert(r.dirty_list()->size() > 1);
      auto it = r.dirty_list()->begin();
      it++;
      auto idx = get_index(dr->elem_.rship_label, dr->properties_);
      if (idx.first.which() > 0) {
        spdlog::info("RSHIP INDEX UPDATE: update: {}", r.dirty_list()->size());
        index_update(idx, dr->elem_.id(), (*it)->properties_, dr->properties_);
      }
		  // we can already delete the dirty object from the dirty version list
		  r.dirty_list()->pop_front();
		  // release the lock of the older version.
		  r.dirty_list()->front()->elem_.unlock();
	  }
  }
  // finally, release the lock of the persistent object and initiate the
	// garbage collection
	r.unlock();
	r.gc(oldest_xid_);
}

bool graph_db::commit_transaction() {
  check_tx_context();
  auto tx = current_transaction();
  auto xid = tx->xid();

  {
    // remove transaction from the active transaction set
    std::lock_guard<std::mutex> guard(*m_);
    active_tx_->erase(xid);
    oldest_xid_ = !active_tx_->empty() ? active_tx_->begin()->first : xid;
  }

  // process dirty_rships list
  for (auto rel_id : tx->dirty_relationships())  {
    commit_dirty_relationship(tx, rel_id);
  }

  // process dirty_nodes list
  for (auto node_id : tx->dirty_nodes()) {
    commit_dirty_node(tx, node_id);
  }

  walog_->transaction_commit(xid);  

  current_transaction_.reset();
  vacuum(xid);

  return true;
}

bool graph_db::abort_transaction() {
  if (!current_transaction_)
    return false;

  check_tx_context();
  auto tx = current_transaction();
  auto xid = tx->xid();

  {
    // remove transaction from the active transaction set
    std::lock_guard<std::mutex> guard(*m_);
    active_tx_->erase(xid);
    oldest_xid_ = !active_tx_->empty() ? active_tx_->begin()->first : xid;
  }
  // if we have added a node or relationship then
  // we have to delete it again from the nodes_ or rships_ tables.
   for (auto node_id  : tx->dirty_nodes()) {
    auto &n = nodes_->get(node_id);
    bool was_updated = n.updated_in_version(xid);
    n.remove_dirty_version(xid);
    n.unlock();
    // remove the node only if it was added but not updated!
    if (!was_updated) {
      // spdlog::info("remove dirty node for tx {} and node #{}", xid, node_id);
      nodes_->remove(node_id);
    }
  }

  for (auto rel_id  : tx->dirty_relationships()) {
    /// spdlog::info("remove dirty relationship for tx {}", xid);
    auto &r = rships_->get(rel_id );
    bool was_updated = r.updated_in_version(xid);
    r.remove_dirty_version(xid);
    r.unlock();
    // remove the relationship only if it was added but not updated!
    if (!was_updated) {
      rships_->remove(rel_id);
    }
  }
  vacuum(xid);
 
  walog_->transaction_abort(xid);  

  current_transaction_.reset();

  return true;
}

void graph_db::print_stats() {
  std::cout << "nodes: " << nodes_->num_chunks() << " chunks, "
            << "chunk_size = " << nodes_->as_vec().real_chunk_size() << " Bytes, "
            << "node_size = " << sizeof(node) << " Bytes, "
            << "capacity = " << nodes_->as_vec().capacity() << std::endl;
  std::cout << "relationships: " << rships_->num_chunks() << " chunks, "
            << "chunk_size = " << rships_->as_vec().real_chunk_size() << " Bytes, "
            << "rship_size = " << sizeof(relationship) << " Bytes, "
            << "capacity = " << rships_->as_vec().capacity() << std::endl;
  std::cout << "node properties: " << node_properties_->num_chunks() << " chunks, "
            << "chunk_size = " << node_properties_->as_vec().real_chunk_size() << " Bytes, "
            << "property_set_size = " << sizeof(property_set) << " Bytes, "
            << "capacity = " << node_properties_->as_vec().capacity() << std::endl;
  std::cout << "rship properties: " << rship_properties_->num_chunks() << " chunks, "
            << "chunk_size = " << rship_properties_->as_vec().real_chunk_size() << " Bytes, "
            << "property_set_size = " << sizeof(property_set) << " Bytes, "
            << "capacity = " << rship_properties_->as_vec().capacity() << std::endl;

  uint64_t mem = 0;
  mem += nodes_->num_chunks() * nodes_->as_vec().real_chunk_size();
  mem += rships_->num_chunks() * rships_->as_vec().real_chunk_size();
  std::cout << mem / 1024;
  mem += node_properties_->num_chunks() * node_properties_->as_vec().real_chunk_size();
  mem += rship_properties_->num_chunks() * rship_properties_->as_vec().real_chunk_size();
  std::cout << " (" << mem / 1024 << ") KiB total" << std::endl;

  uint64_t nprops = 0;
  node_properties_->foreach_property([&nprops](auto pi) { nprops++; });
  std::cout << nprops << " node properties total." << std::endl;
  uint64_t rprops = 0;
  rship_properties_->foreach_property([&rprops](auto pi) { rprops++; });
  std::cout << rprops << " relationship properties total." << std::endl;

  std::cout << dict_->size() << " strings in dictionary." << std::endl;

  std::cout << "bufferpool hit ratio: " << bpool_.hit_ratio() << std::endl;
}

node::id_t graph_db::add_node(const std::string &label,
                              const properties_t &props, bool append_only) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  auto type_code = dict_->lookup_string(label);
  if (type_code == 0) {
    type_code = dict_->insert(label);
    // spdlog::info("add log_dict_record {}, {}", label, type_code);
    wal::log_dict_record log_rec(type_code, label);
    walog_->append(txid, log_rec);
  }

  auto node_id = append_only ? nodes_->append(node(type_code), txid, UNDO_CB)
                             : nodes_->insert(node(type_code), txid, UNDO_CB);
  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

  // handle properties
  const auto dirty_list = node_properties_->build_dirty_property_list(props, dict_);
  const auto &dv = n.add_dirty_version(
      std::make_unique<dirty_node>(n, dirty_list, false /* insert */));
  dv->elem_.set_dirty();

  current_transaction()->add_dirty_node(node_id);

  return node_id;
}

relationship::id_t graph_db::add_relationship(node::id_t from_id,
                                              node::id_t to_id,
                                              const std::string &label,
                                              const properties_t &props,
                                              bool append_only) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();
  auto &from_node = node_by_id(from_id);
  auto &to_node = node_by_id(to_id);
  auto type_code = dict_->insert(label);
    // spdlog::info("add log_dict_record {}, {}", label, type_code);
    wal::log_dict_record log_rec(type_code, label);
    walog_->append(txid, log_rec);
  auto rid =
      append_only
          ? rships_->append(relationship(type_code, from_id, to_id), txid, UNDO_CB)
          : rships_->insert(relationship(type_code, from_id, to_id), txid, UNDO_CB);
  auto &r = rships_->get(rid);

  const auto dirty_list = rship_properties_->build_dirty_property_list(props, dict_);
  const auto &rv = r.add_dirty_version(
      std::make_unique<dirty_rship>(r, dirty_list, false /* insert */));
  rv->elem_.set_dirty();

  current_transaction()->add_dirty_relationship(rid);

  update_from_node(current_transaction(), from_node, r);
  update_to_node(current_transaction(), to_node, r);

  return rid;
}

node &graph_db::get_valid_node_version(node &n, xid_t xid) {
  if (n.is_locked_by(xid)) {
    // spdlog::debug("[tx {}] node #{} is locked by {}", short_ts(xid), n.id(), short_ts(n.txn_id()));
    // because the node is locked we know that it was already updated by us
    // and we should look for the dirty object containing the new values
    assert(n.has_dirty_versions());
    return n.find_valid_version(xid)->elem_;
  }
  // or (2) is not locked and xid is in [bts,cts]
  if (!n.is_locked()) {
    //spdlog::debug("node_by_id: node #{} is unlocked: [{}, {}] <=> {}", n.id(),
    //             n.bts(), n.cts(), xid);
    return n.is_valid_for(xid) ? n : n.find_valid_version(xid)->elem_;
  }

  // or (3) node is locked by another transaction
  else {
    spdlog::debug("node #{} is locked by another tx in {}", n.id(), xid);
    // dump();
    // try to find a valid version which is not locked
    auto &nv = n.find_valid_version(xid)->elem_;
    if (!nv.is_locked() || nv.is_locked_by(xid))
      return nv;
    throw transaction_abort();
  }
  throw unknown_id();
}

relationship &graph_db::get_valid_rship_version(relationship &r, xid_t xid) {
  // we can read relationship r if
  // (1) we own the lock
  if (r.is_locked_by(xid)) {
    // because the relationship is locked we know that it was already updated
    // by us and we should look for the dirty object containing the new values
    assert(r.has_dirty_versions());
    return r.find_valid_version(xid)->elem_;
  }
  // or (2) is unlocked and xid is in [bts,cts]
  if (!r.is_locked()) {
    return r.is_valid_for(xid) ? r : r.find_valid_version(xid)->elem_;
  }

  // relationship is locked by another transaction -> abort!!
  if (r.is_locked())  {
    // try to find a valid version which is not locked
    auto &rv = r.find_valid_version(xid)->elem_;
    if (!rv.is_locked() || rv.is_locked_by(xid))
      return rv;
    // spdlog::info("relationship #{} is locked by another tx", n.id());
    throw transaction_abort();
  }
  throw unknown_id();
}

node &graph_db::node_by_id(node::id_t id) {
  check_tx_context();
  auto xid = current_transaction()->xid();
  /// spdlog::info("[{}] try to fetch node #{}", xid, id);
  auto &n = nodes_->get(id);
  n.prepare();
  n.set_rts(xid);
  return get_valid_node_version(n, xid);
}

relationship &graph_db::rship_by_id(relationship::id_t id) {
  check_tx_context();
  auto xid = current_transaction()->xid();
  auto &r = rships_->get(id);
  r.prepare();
  r.set_rts(xid);
  return get_valid_rship_version(r, xid);
}

node_description graph_db::get_node_description(node::id_t nid) {
  std::string label;
  properties_t props;
  auto& n = node_by_id(nid);
  auto xid = current_transaction()->xid();
  // spdlog::info("get_node_description @{}", (unsigned long)&n);
  if (!n.has_dirty_versions()) {
    // the simple case: no concurrent transactions are active and
    // we can get the properties from the properties_ table
     // spdlog::info("get_node_description - not dirty");
    props = node_properties_->all_properties(n.property_list, dict_);
    label = dict_->lookup_code(n.node_label);
  }
  else {
    //spdlog::info("tx #{}: get_node_description - is_dirty={} - is_valid={}",
    //  xid, n.is_dirty(), n.is_valid(xid));
    // dump();
    // otherwise there are two options:
    if (!n.is_dirty() && n.is_valid_for(xid)) {
      if (n.cts() == INF) {
        // (1) we still can get the data from the properties_ table
        props = node_properties_->all_properties(n.property_list, dict_);
        label = dict_->lookup_code(n.node_label);
      }
      else {
        // the node is deleted and its property values are no more in the properties_ table
        // (2) we get the property values directly from the valid version
        const auto& dn = n.find_valid_version(xid);
        props = node_properties_->build_properties_from_pitems(dn->properties_, dict_);
        label = dict_->lookup_code(dn->elem_.node_label);
      }
    }
    else {
      // (2) we get the property values directly from the p_item list
      const auto& dn = n.find_valid_version(xid);
      // spdlog::info("got dirty version: @{}", (unsigned long)&(dn->elem_));
      // dump();
      props = node_properties_->build_properties_from_pitems(dn->properties_, dict_);
      label = dict_->lookup_code(dn->elem_.node_label);
    }
  }
  return node_description{n.id(), label, props};
}

rship_description graph_db::get_rship_description(relationship::id_t rid) {
  std::string label;
  properties_t props;
  auto& r = rship_by_id(rid);
  auto xid = current_transaction()->xid();
  if (!r.has_dirty_versions()) {
   // the simple case: no concurrent transactions are active and
    // we can get the properties from the properties_ table
     // spdlog::info("get_node_description - not dirty");
    props = rship_properties_->all_properties(r.property_list, dict_);
    label = dict_->lookup_code(r.rship_label);
  }
  else {
    // otherwise there are two options:
    // (1) we still can get the data from the properties_ table
    if (!r.is_dirty() && r.is_valid_for(xid)) {
      props = rship_properties_->all_properties(r.property_list, dict_);
      label = dict_->lookup_code(r.rship_label);
    }
    else {
      // (2) we get the property values directly from the p_item list
      const auto& dr = r.find_valid_version(xid);
      // spdlog::info("got dirty version: @{}", (unsigned long)&(dn->elem_));
      // dump();
      props = rship_properties_->build_properties_from_pitems(dr->properties_, dict_);
      label = dict_->lookup_code(dr->elem_.rship_label);
    }
  }
  return rship_description{r.id(), r.from_node_id(), r.to_node_id(),
                           label, props};
}

const char *graph_db::get_relationship_label(const relationship &r) {
  return dict_->lookup_code(r.rship_label);
}

void graph_db::update_node(node &n, const properties_t &props,
                           const std::string &label) {
  auto lc = label.empty() ? 0 : dict_->insert(label);
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (n.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!n.is_locked_by(txid) && !n.try_lock(txid))
    throw transaction_abort();

  bool first_update = true;
  if (n.has_dirty_versions()) {
    // let's look for a version which we already have created in this transaction
    try {
      auto& dn = n.get_dirty_version(txid);
      // apply update to dn
      node_properties_->apply_updates(dn->properties_, props, dict_);
      if (lc > 0)
        dn->elem_.node_label = lc;
      first_update = false;
    } catch (unknown_id& exc) { /* do nothing */ }
  }

  if (first_update) {
    // first, we make a copy of the original node which is stored in
    // the dirty list
    std::list<p_item> pitems =
      node_properties_->build_dirty_property_list( /* n.id(),*/ n.property_list);
    // cts is set to txid
    const auto& oldv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    oldv->elem_.set_timestamps(n.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();

    // ... and create another copy as the new version
    pitems = node_properties_->apply_updates(pitems, props, dict_);
    const auto &newv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    newv->elem_.set_timestamps(txid, INF);
    newv->elem_.set_dirty();
    if (lc > 0)
      newv->elem_.node_label = lc;

    current_transaction()->add_dirty_node(n.id());
  }
}

void graph_db::update_from_node(transaction_ptr tx, node &n, relationship& r) {
  xid_t txid = tx->xid();

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (n.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!n.is_locked_by(txid) && !n.try_lock(txid))
    throw transaction_abort();

  bool first_update = true;
  if (n.has_dirty_versions()) {
    // let's look for a version which we already have created in this transaction
    try {
      auto& dn = n.get_dirty_version(txid)->elem_;
      // apply update to dn
      if (dn.from_rship_list == UNKNOWN)
        dn.from_rship_list = r.id();
      else {
        r.next_src_rship = dn.from_rship_list;
        dn.from_rship_list = r.id();
      }
      first_update = false;
    } catch (unknown_id& exc) { /* do nothing */ }
  }

  if (first_update) {
    // first, we make a copy of the original node which is stored in
    // the dirty list
    std::list<p_item> pitems =
      node_properties_->build_dirty_property_list( /* n.id(),*/ n.property_list);
    // cts is set to txid
    const auto& oldv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    oldv->elem_.set_timestamps(n.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();

    // ... and create another copy as the new version
    const auto &newv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    newv->elem_.set_timestamps(txid, INF);
    newv->elem_.set_dirty();

    if (newv->elem_.from_rship_list == UNKNOWN)
        newv->elem_.from_rship_list = r.id();
    else {
        r.next_src_rship = newv->elem_.from_rship_list;
        newv->elem_.from_rship_list = r.id();
    }
    current_transaction()->add_dirty_node(n.id());
  }
}

void graph_db::update_to_node(transaction_ptr tx, node &n, relationship& r) {
 xid_t txid = tx->xid();

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (n.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!n.is_locked_by(txid) && !n.try_lock(txid))
    throw transaction_abort();

  bool first_update = true;
  if (n.has_dirty_versions()) {
    // let's look for a version which we already have created in this transaction
    try {
      auto& dn = n.get_dirty_version(txid)->elem_;
      // apply update to dn
      if (dn.to_rship_list == UNKNOWN)
        dn.to_rship_list = r.id();
      else {
        r.next_dest_rship = dn.to_rship_list;
        dn.to_rship_list = r.id();
      }
      first_update = false;
    } catch (unknown_id& exc) { /* do nothing */ }
  }

  if (first_update) {
    // first, we make a copy of the original node which is stored in
    // the dirty list
    std::list<p_item> pitems =
      node_properties_->build_dirty_property_list( /* n.id(),*/ n.property_list);
    // cts is set to txid
    const auto& oldv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    oldv->elem_.set_timestamps(n.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();

    // ... and create another copy as the new version
    const auto &newv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    newv->elem_.set_timestamps(txid, INF);
    newv->elem_.set_dirty();

    if (newv->elem_.to_rship_list == UNKNOWN)
        newv->elem_.to_rship_list = r.id();
    else {
        r.next_dest_rship = newv->elem_.to_rship_list;
        newv->elem_.to_rship_list = r.id();
    }
    current_transaction()->add_dirty_node(n.id());
  }
}

void graph_db::update_relationship(relationship &r, const properties_t &props,
                                   const std::string &label) {
  auto lc = label.empty() ? 0 : dict_->insert(label);
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (r.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!r.is_locked_by(txid) && !r.try_lock(txid))
    throw transaction_abort();

  bool first_update = true;
  if (r.has_dirty_versions()) {
    // let's look for a version which we already have created in this transaction
    try {
      auto& dr = r.get_dirty_version(txid);
      // apply update to dn
      rship_properties_->apply_updates(dr->properties_, props, dict_);
      if (lc > 0)
        dr->elem_.rship_label = lc;
      first_update = false;
    } catch (unknown_id& exc) { /* do nothing */ }
  }

  if (first_update) {
  // first, we make a copy of the original node which is stored in
  // the dirty list
  std::list<p_item> pitems =
      rship_properties_->build_dirty_property_list(/*r.id(),*/ r.property_list);
  // cts is set to txid
  const auto& oldv = r.add_dirty_version(std::make_unique<dirty_rship>(r, pitems));
  oldv->elem_.set_timestamps(r.bts(), txid);
  oldv->elem_.set_dirty();
  // but unlock it for readers
  oldv->elem_.unlock();

  // ... and create another copy as the new version
  pitems = rship_properties_->apply_updates(pitems, props, dict_);
  const auto& newv = r.add_dirty_version(std::make_unique<dirty_rship>(r, pitems));
  newv->elem_.set_timestamps(txid, INF);
  newv->elem_.set_dirty();
  if (lc > 0)
    newv->elem_.rship_label = lc;

  current_transaction()->add_dirty_relationship(r.id());
  }
}

void graph_db::delete_node(node::id_t id) {
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  auto &n = this->node_by_id(id);

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (n.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!n.is_locked_by(txid) && !n.try_lock(txid))
    throw transaction_abort();

  // first, we check whether the node is still connected via relationships ..
  // TODO: the relationship could still be stored in the rships_ table but marked as deleted!!
  if (n.from_rship_list != UNKNOWN || n.to_rship_list != UNKNOWN) {
    if (has_valid_from_rships(n, txid) || has_valid_to_rships(n, txid)) {
      // in this case we have to abort
      // spdlog::info("abort delete of node #{}", n.id());
      throw orphaned_relationship();
    }
  }

  // then, we make a copy of the original node which is stored in
  // the dirty list
    std::list<p_item> pitems =
        node_properties_->build_dirty_property_list(n.property_list);
    // cts is set to txid
    const auto& oldv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    oldv->elem_.set_timestamps(n.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();

  // ... and create another copy as the new deleted version
  const auto &newv = n.add_dirty_version(std::make_unique<dirty_node>(n, std::list<p_item>(), true /* updated */));
  newv->elem_.set_timestamps(txid, txid);
  newv->elem_.set_dirty();

  current_transaction()->add_dirty_node(n.id());
  // spdlog::info("delete_node: {}", n.id());

}

void graph_db::detach_delete_node(node::id_t id) {
  spdlog::info("try to detach_delete_node: {}", id);
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  auto &n = this->node_by_id(id);

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (n.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!n.is_locked_by(txid) && !n.try_lock(txid))
    throw transaction_abort();

  // we collect the ids of all relationships in which n is involved
  std::list<relationship::id_t> rships;
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    auto& relship = rships_->get(relship_id);
    if (relship.is_locked_by(txid)) {
      // because the relationship is locked we know that it was already updated
      // by us and we should look for the dirty object containing the new values
      assert(relship.has_dirty_versions());
      if (relship.has_valid_version(txid))
        rships.push_back(relship_id);
    }
    else if (relship.is_valid_for(txid))
      rships.push_back(relship_id);

    relship_id = relship.next_src_rship;
  }
  relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    auto& relship = rships_->get(relship_id);
    if (relship.is_locked_by(txid)) {
      // because the relationship is locked we know that it was already updated
      // by us and we should look for the dirty object containing the new values
      assert(relship.has_dirty_versions());
      if (relship.has_valid_version(txid))
        rships.push_back(relship_id);
    }
    else if (relship.is_valid_for(txid))
      rships.push_back(relship_id);

    relship_id = relship.next_dest_rship;
  }

  for (auto& r : rships) {
    spdlog::info("detach_delete_node => delete rship: {}", r);
    delete_relationship(r);
  }

  // then, we make a copy of the original node which is stored in
  // the dirty list
  // do this only if the original node isn't already in the dirty list
  bool first_update = !n.has_dirty_versions();

  if (first_update) {
    std::list<p_item> pitems =
      node_properties_->build_dirty_property_list(n.property_list);
    // cts is set to txid
    const auto& oldv = n.add_dirty_version(std::make_unique<dirty_node>(n, pitems));
    oldv->elem_.set_timestamps(n.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();
  }
  // ... and create another copy as the new deleted version
  const auto &newv = n.add_dirty_version(std::make_unique<dirty_node>(n, std::list<p_item>(), true /* updated */));
  newv->elem_.set_timestamps(txid, txid);
  newv->elem_.set_dirty();

  current_transaction()->add_dirty_node(n.id());
}

void graph_db::delete_relationship(node::id_t src, node::id_t dest) {
    // acquire lock and create a dirty object
    check_tx_context();
    xid_t txid = current_transaction()->xid();

    auto &n = this->node_by_id(src);

    auto relship_id = n.from_rship_list;
    while (relship_id != UNKNOWN) {
      auto& relship = rships_->get(relship_id);
      if (relship.is_locked_by(txid)) {
        // because the relationship is locked we know that it was already updated
        // by us and we should look for the dirty object containing the new values
        assert(relship.has_dirty_versions());
        if (relship.has_valid_version(txid) && (relship.to_node_id() == dest))
          return delete_relationship(relship_id);
      }
      else if (relship.is_valid_for(txid) && (relship.to_node_id() == dest))
        return delete_relationship(relship_id);

      relship_id = relship.next_src_rship;
    }

}

void graph_db::delete_relationship(relationship::id_t id) {
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  auto &r = this->rship_by_id(id);

  // make sure we don't overwrite an object that was read by
  // a more recent transaction
  if (r.rts() > txid)
   throw transaction_abort();

  // if we don't own the lock and cannot acquire a lock, we have to abort
  if (!r.is_locked_by(txid) && !r.try_lock(txid))
    throw transaction_abort();

  // first, we make a copy of the original relationship which is stored in
  // the dirty list
   // do this only if the original node isn't already in the dirty list
  bool first_update = !r.has_dirty_versions();

  if (first_update) {
    std::list<p_item> pitems =
      rship_properties_->build_dirty_property_list(r.property_list);
    // cts is set to txid
    const auto& oldv = r.add_dirty_version(std::make_unique<dirty_rship>(r, pitems));
    oldv->elem_.set_timestamps(r.bts(), txid);
    oldv->elem_.set_dirty();
    // but unlock it for readers
    oldv->elem_.unlock();
  }
  // ... and create another copy as the new deleted version
  const auto &newv = r.add_dirty_version(std::make_unique<dirty_rship>(r, std::list<p_item>(), true /* updated */));
  newv->elem_.set_timestamps(txid, txid);
  newv->elem_.set_dirty();

  current_transaction()->add_dirty_relationship(r.id());
}

const char *graph_db::get_string(dcode_t c) { return dict_->lookup_code(c); }

dcode_t graph_db::get_code(const std::string &s) {
  return dict_->lookup_string(s);
}

void graph_db::dump() {
  nodes_->dump();
  rships_->dump();
  node_properties_->dump();
  rship_properties_->dump();
}

void graph_db::dump_dot(const std::string& fname) {
  std::ofstream out;
  out.open(fname, std::ofstream::out | std::ofstream::trunc);
  out << "digraph poseidon_db {\n";
  for (auto& n : nodes_->as_vec()) {
    out << '\t' << "n" << n.id()
        << " [label = \"" << n.id() << ":"
        << get_string(n.node_label) << "\"];" << std::endl;
  }
  for (auto& r : rships_->as_vec()) {
    out << '\t' << "n" << r.src_node << " -> n" << r.dest_node
        << " [label = \"" << get_string(r.rship_label) << "\"];" << std::endl;
  }
  out << "}";
  out.close();
}

void graph_db::copy_properties(node &n, const dirty_node_ptr& dn) {
  if (dn->properties_.empty())
    return;

  property_set::id_t pid;
  if (dn->updated()) {
    // create and append a log_property_record
    node_properties_->foreach_property_set(n.property_list, UNDO_CB);
    // we have to update the properties
    pid = node_properties_->update_pitems(n.id(), n.property_list, dn->properties_,
                                     dict_);
  } else {
    // the node was newly added - we have to add the properties
    // to the properties_ table
    // But we should log this. Otherwise, the slot might get be lost in
    // case of system failure.
    pid = node_properties_->add_pitems(n.id(), dn->properties_, dict_, UNDO_CB);
  }
  n.property_list = pid;
}

void graph_db::copy_properties(relationship &r, const dirty_rship_ptr& dr) {
  if (dr->properties_.empty())
    return;

  property_set::id_t pid;
  if (dr->updated()) {
    // create and append a log_property_record
    rship_properties_->foreach_property_set(r.property_list, UNDO_CB);
    // we have to update the properties
    pid = rship_properties_->update_pitems(r.id(), r.property_list, dr->properties_,
                                     dict_);
    /// spdlog::info("update node -> set properties to {}", pid);
  } else {
    pid = rship_properties_->add_pitems(r.id(), dr->properties_, dict_, UNDO_CB);
  }
  r.property_list = pid;
}

bool graph_db::has_valid_from_rships(node &n, xid_t xid) {
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    spdlog::debug("node #{}, try to get from rship {}", n.id(), relship_id);
    auto &relship = rships_->get(relship_id);
    if (relship.is_locked_by(xid)) {
      // because the relationship is locked we know that it was already updated
      // by us and we should look for the dirty object containing the new values
      assert(relship.has_dirty_versions());
      if (relship.has_valid_version(xid))
        return true;
    }
    else {
      return relship.is_valid_for(xid);
    }
    relship_id = relship.next_src_rship;
  }
  return false;
}

bool graph_db::has_valid_to_rships(node &n, xid_t xid) {
  auto relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    spdlog::debug("node #{}, try to get to rship {}", n.id(), relship_id);
    auto &relship = rships_->get(relship_id);
    if (relship.is_locked_by(xid)) {
      // because the relationship is locked we know that it was already updated
      // by us and we should look for the dirty object containing the new values
      assert(relship.has_dirty_versions());
      if (relship.has_valid_version(xid))
        return true;
    }
    else {
      return relship.is_valid_for(xid);
    }
    relship_id = relship.next_dest_rship;
  }
  return false;
}


p_item graph_db::get_property_value(const node &n, const std::string& pkey) {
  auto pc = dict_->lookup_string(pkey);
  return get_property_value(n, pc);
}

p_item graph_db::get_property_value(const node &n, dcode_t pcode) {
  return node_properties_->property_value(n.property_list, pcode);
}

p_item graph_db::get_property_value(const relationship &r, const std::string& pkey) {
  auto pc = dict_->lookup_string(pkey);
  return get_property_value(r, pc);
}

p_item graph_db::get_property_value(const relationship &r, dcode_t pcode) {
  return rship_properties_->property_value(r.property_list, pcode);
}
