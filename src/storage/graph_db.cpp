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

#include "graph_db.hpp"
#include "chunked_vec.hpp"
#include "parser.hpp"
#include "spdlog/spdlog.h"
#include "thread_pool.hpp"
#include <iostream>

struct scan_task {
  using range = std::pair<std::size_t, std::size_t>;
  scan_task(graph_db *gdb, node_list &n, std::size_t first, std::size_t last,
            graph_db::node_consumer_func c, transaction_ptr tp = nullptr)
      : graph_db_(gdb), nodes_(n), range_(first, last), consumer_(c), tx_(tp) {}

  void operator()() {
    xid_t xid = 0;
    if (tx_) { // we need the transaction pointer in thread-local storage
      current_transaction_ = tx_;
      xid = tx_->xid();
    }
    auto iter = nodes_.range(range_.first, range_.second);
    while (iter) {
#ifdef USE_TX
      auto &n = *iter;
      auto &nv = graph_db_->get_valid_node_version(n, xid);
      consumer_(nv);
#else
      consumer_(*iter);
#endif
      ++iter;
    }
  }

  graph_db *graph_db_;
  node_list &nodes_;
  range range_;
  graph_db::node_consumer_func consumer_;
  transaction_ptr tx_;
};

graph_db::graph_db(const std::string &db_name) {
  nodes_ = p_make_ptr<node_list>();
  rships_ = p_make_ptr<relationship_list>();
  properties_ = p_make_ptr<property_list>();
  dict_ = p_make_ptr<dict>();
  active_tx_ = new std::map<xid_t, transaction_ptr>();
  m_ = new std::mutex();
}

graph_db::~graph_db() {
  // we don't delete persistent objects here!
  for (auto tp : *active_tx_) {
    // TODO: abort all active transactions
  }
  delete active_tx_;
  delete m_;
#ifdef USE_TX
  current_transaction_.reset();
#endif
}

void graph_db::runtime_initialize() {
  nodes_->runtime_initialize();
  rships_->runtime_initialize();
  // make sure the dictionary is initialized
  dict_->initialize();
  // recreate volatile objects: active_tx_ table and mutex
  active_tx_ = new std::map<xid_t, transaction_ptr>();
  oldest_xid_ = 0;
  m_ = new std::mutex();
}

transaction_ptr graph_db::begin_transaction() {
  if (current_transaction_)
    throw invalid_nested_transaction();
  auto tx = std::make_shared<transaction>();
  current_transaction_ = tx;
  std::lock_guard<std::mutex> guard(*m_);
  active_tx_->insert({tx->xid(), tx});
  // spdlog::info("begin transaction {}", tx->xid());
  return tx;
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

  // process dirty_nodes list
  for (node *nptr : tx->dirty_nodes()) {
    // let's get the dirty_node object added or updated by this transaction
    dirty_node_ptr dn = nptr->get_dirty_version(xid);
    if (!dn->updated()) {
      // case #1: we have added a new node to node_list, but its properties
      // are stored in a dirty_node object in this case, we simply copy the
      // properties to property_list and release the lock
      // spdlog::info("commit INSERT transaction {}: copy properties", xid);
      copy_properties(*nptr, dn);
      // set bts/cts
      nptr->set_timestamps(xid, INF);
      // we can already delete the object from the dirty version list
      nptr->remove_dirty_version(xid);
    } else {
      // case #2: we have updated a node, thus copy both properties
      // and node version to the main tables
      /// spdlog::info("commit UPDATE transaction {}: copy properties", xid);
      // update node (label)
      nptr->node_label = dn->elem_.node_label;
      copy_properties(*nptr, dn);
      /// spdlog::info("COMMIT UPDATE: set new={},{} @{}", xid, INF,
      ///             (unsigned long)nptr);
      nptr->set_timestamps(xid, INF);
      /// spdlog::info("COMMIT UPDATE: set old.cts={} @{}", xid,
      ///             (unsigned long)&(dn->node_));
      // we can already delete the object from the dirty version list
      // and unlock the original version
      nptr->remove_dirty_version(xid);
    }
    // release the lock of the old versions
    for (auto &dn : *nptr->dirty_list) {
      if (dn->elem_.txn_id == xid)
        dn->elem_.unlock();
    }
    // finally, release the lock of the persistent object and initiate the
    // garbage collection
    nptr->unlock();
    nptr->gc(oldest_xid_);
  }
  // process dirty_rships list
  for (relationship *rptr : tx->dirty_relationships()) {
    // let's get the dirty_rship object added or updated by this transaction
    dirty_rship_ptr dr = rptr->get_dirty_version(xid);
    if (!dr->updated()) {
      // case #1: we have added a new relationship to relationship_list, but
      // its properties are stored in a dirty_rship object in this case, we
      // simply copy the properties to property_list and release the lock
      copy_properties(*rptr, dr);
      // set bts/cts
      rptr->set_timestamps(xid, INF);
      // we can already delete the object from the dirty version list
      rptr->remove_dirty_version(xid);
    } else {
      // case #2: we have updated a relationship, thus copy both properties
      // and relationship version to the main tables
      // update relationship (label)
      rptr->rship_label = dr->elem_.rship_label;
      copy_properties(*rptr, dr);
      rptr->set_timestamps(xid, INF);
      // we can already delete the object from the dirty version list
      // and unlock the original version
      rptr->remove_dirty_version(xid);
    }
    // release the lock of the old versions
    for (auto &dr : *rptr->dirty_list) {
      if (dr->elem_.txn_id == xid)
        dr->elem_.unlock();
    }
    // finally, release the lock of the persistent object and initiate the
    // garbage collection
    rptr->unlock();
    rptr->gc(oldest_xid_);
  }

  // remove transaction from the active transaction set
  // std::lock_guard<std::mutex> guard(*m_);
  // active_tx_->erase(tx->xid());
  current_transaction_.reset();
  return true;
}

bool graph_db::abort_transaction() {
  check_tx_context();
  auto tx = current_transaction();
  auto xid = tx->xid();

  {
    // remove transaction from the active transaction set
    std::lock_guard<std::mutex> guard(*m_);
    active_tx_->erase(xid);
    oldest_xid_ = !active_tx_->empty() ? active_tx_->begin()->first : xid;
  }
  // TODO: if we have added a node or relationship then
  // we have to delete it again from the nodes_ or rships_ tables.
  for (node *nptr : tx->dirty_nodes()) {
    /// spdlog::info("remove dirty node for tx {} and node #{}", xid,
    /// nptr->id());
    nptr->remove_dirty_version(xid);
    nptr->unlock();
    nodes_->remove(nptr->id());
  }

  for (relationship *rptr : tx->dirty_relationships()) {
    /// spdlog::info("remove dirty relationship for tx {}", xid);
    rptr->remove_dirty_version(xid);
    rptr->unlock();
    rships_->remove(rptr->id());
  }

  // std::lock_guard<std::mutex> guard(*m_);
  // active_tx_->erase(xid);
  current_transaction_.reset();
  return true;
}

void graph_db::print_mem_usage() {
  std::size_t nchunks =
      nodes_->num_chunks() + rships_->num_chunks() + properties_->num_chunks();
  std::cout << "Memory usage: " << nchunks * DEFAULT_CHUNK_SIZE / 1024 << " KiB"
            << std::endl;
}

node::id_t graph_db::add_node(const std::string &label,
                              const properties_t &props, bool append_only) {
#ifdef USE_TX
  check_tx_context();
  xid_t txid = current_transaction()->xid();
#else
  xid_t txid = 0;
#endif
  auto type_code = dict_->insert(label);
  auto node_id = append_only ? nodes_->append(node(type_code), txid)
                             : nodes_->add(node(type_code), txid);

  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

#ifdef USE_TX
  // handle properties
  auto dirty_list = properties_->build_dirty_property_list(props, dict_);
  auto &dv = n.add_dirty_version(
      std::make_shared<dirty_node>(n, dirty_list, false /* insert */));
  dv->elem_.set_dirty();
  current_transaction()->add_dirty_object(&n);
#else
  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        append_only ? properties_->append_node_properties(node_id, props, dict_)
                    : properties_->add_node_properties(node_id, props, dict_);
    n.property_list = pid;
  }
#endif

  return node_id;
}

node::id_t graph_db::import_node(const std::string &label,
                                 const properties_t &props) {
  auto type_code = dict_->insert(label);
  auto node_id = nodes_->append(node(type_code), 0);

  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        properties_->append_node_properties(node_id, props, dict_);
    n.property_list = pid;
  }

  return node_id;
}

relationship::id_t graph_db::add_relationship(node::id_t from_id,
                                              node::id_t to_id,
                                              const std::string &label,
                                              const properties_t &props,
                                              bool append_only) {
#ifdef USE_TX
  check_tx_context();
  xid_t txid = current_transaction()->xid();
#else
  xid_t txid = 0;
#endif
  auto &from_node = nodes_->get(from_id);
  auto &to_node = nodes_->get(to_id);
  auto type_code = dict_->insert(label);
  auto rid =
      append_only
          ? rships_->append(relationship(type_code, from_id, to_id), txid)
          : rships_->add(relationship(type_code, from_id, to_id), txid);

  auto &r = rships_->get(rid);

#ifdef USE_TX
  auto dirty_list = properties_->build_dirty_property_list(props, dict_);
  auto &rv = r.add_dirty_version(
      std::make_shared<dirty_rship>(r, dirty_list, false /* insert */));
  rv->elem_.set_dirty();
  current_transaction()->add_dirty_object(&r);

#else
  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        append_only
            ? properties_->append_relationship_properties(rid, props, dict_)
            : properties_->add_relationship_properties(rid, props, dict_);
    r.property_list = pid;
  }
#endif
  // update the list of relationships for each of both nodes
  if (from_node.from_rship_list == UNKNOWN)
    from_node.from_rship_list = rid;
  else {
    r.next_src_rship = from_node.from_rship_list;
    from_node.from_rship_list = rid;
  }

  if (to_node.to_rship_list == UNKNOWN)
    to_node.to_rship_list = rid;
  else {
    r.next_dest_rship = to_node.to_rship_list;
    to_node.to_rship_list = rid;
  }
  return rid;
}

relationship::id_t graph_db::import_relationship(node::id_t from_id,
                                                 node::id_t to_id,
                                                 const std::string &label,
                                                 const properties_t &props) {

  auto &from_node = nodes_->get(from_id);
  auto &to_node = nodes_->get(to_id);
  auto type_code = dict_->insert(label);
  auto rid = rships_->append(relationship(type_code, from_id, to_id), 0);

  auto &r = rships_->get(rid);

  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        properties_->append_relationship_properties(rid, props, dict_);
    r.property_list = pid;
  }
  // update the list of relationships for each of both nodes
  if (from_node.from_rship_list == UNKNOWN)
    from_node.from_rship_list = rid;
  else {
    r.next_src_rship = from_node.from_rship_list;
    from_node.from_rship_list = rid;
  }

  if (to_node.to_rship_list == UNKNOWN)
    to_node.to_rship_list = rid;
  else {
    r.next_dest_rship = to_node.to_rship_list;
    to_node.to_rship_list = rid;
  }
  return rid;
}

node &graph_db::get_valid_node_version(node &n, xid_t xid) {
  if (n.is_locked_by(xid)) {
    // spdlog::info("[tx {}] node #{} is locked by {}", xid, n.id(), n.txn_id);
    // because the node is locked we know that it was already updated by us
    // and we should look for the dirty object containing the new values
    assert(n.has_dirty_versions());
    return n.find_valid_version(xid)->elem_;
  }
  // or (2) is unlocked and xid is in [bts,cts]
  if (!n.is_locked()) {
    // spdlog::info("node_by_id: node #{} is unlocked: [{}, {}] <=> {}", n.id(),
    //             n.bts, n.cts, xid);
    return n.is_valid(xid) ? n : n.find_valid_version(xid)->elem_;
  }

  // TODO: node is locked by another transaction?!
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
    return r.is_valid(xid) ? r : r.find_valid_version(xid)->elem_;
  }

  // TODO: relationship is locked by another transaction?!
  throw unknown_id();
}

node &graph_db::node_by_id(node::id_t id) {
#ifdef USE_TX
  check_tx_context();
  auto xid = current_transaction()->xid();
  /// spdlog::info("[{}] try to fetch node #{}", xid, id);
  auto &n = nodes_->get(id);
  return get_valid_node_version(n, xid);
#else
  return nodes_->get(id);
#endif
}

relationship &graph_db::rship_by_id(relationship::id_t id) {
#ifdef USE_TX
  check_tx_context();
  auto xid = current_transaction()->xid();
  auto &r = rships_->get(id);
  return get_valid_rship_version(r, xid);
#else
  return rships_->get(id);
#endif
}

node_description graph_db::get_node_description(const node &n) {
  auto label = dict_->lookup_code(n.node_label);
  properties_t props;
#ifdef USE_TX
  check_tx_context();
  auto xid = current_transaction()->xid();
  // spdlog::info("get_node_description @{}", (unsigned long)&n);
  if (n.is_dirty()) {
    // spdlog::info(
    //    "get_node_description - is_dirty tx = {} -- has_dirty_versions = {}",
    //    xid, n.has_dirty_versions());
    // if n is a dirty node then we get the property values directly from the
    // p_item list
    auto dn = n.find_valid_version(xid);
    // spdlog::info("got dirty version!!");
    props = properties_->build_properties_from_pitems(dn->properties_, dict_);
  } else {
    // spdlog::info("get_node_description - not dirty");
    // otherwise from the properties_ table
    props = properties_->all_properties(n.property_list, dict_);
  }
#else
  props = properties_->all_properties(n.property_list, dict_);
#endif
  return node_description{n.id(), std::string(label), props};
}

rship_description graph_db::get_rship_description(const relationship &r) {
  auto label = dict_->lookup_code(r.rship_label);
  properties_t props;
#ifdef USE_TX
  check_tx_context();
  auto xid = current_transaction()->xid();
  if (r.is_dirty()) {
    // if n is a dirty relationship then we get the property values directly
    // from the p_item list
    auto dr = r.find_valid_version(xid);
    props = properties_->build_properties_from_pitems(dr->properties_, dict_);
  } else {
    // otherwise from the properties_ table
    props = properties_->all_properties(r.property_list, dict_);
  }
#else
  props = properties_->all_properties(r.property_list, dict_);
#endif
  return rship_description{r.id(), r.from_node_id(), r.to_node_id(),
                           std::string(label), props};
}

const char *graph_db::get_relationship_label(const relationship &r) {
  return dict_->lookup_code(r.rship_label);
}

void graph_db::update_node(node &n, const properties_t &props,
                           const std::string &label) {
  auto lc = label.empty() ? 0 : dict_->insert(label);
#ifdef USE_TX
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  if (!n.try_lock(txid))
    throw transaction_abort();

  // first, we make a copy of the original node which is stored in
  // the dirty list
  std::list<p_item> pitems =
      properties_->build_dirty_property_list(n.id(), n.property_list);
  // cts is set to txid
  auto &oldv = n.add_dirty_version(std::make_shared<dirty_node>(n, pitems));
  oldv->elem_.set_timestamps(n.bts, txid);
  oldv->elem_.set_dirty();

  // ... and create another copy as the new version
  pitems = properties_->apply_updates(pitems, props, dict_);
  auto &newv = n.add_dirty_version(std::make_shared<dirty_node>(n, pitems));
  newv->elem_.set_timestamps(txid, INF);
  newv->elem_.set_dirty();
  if (lc > 0)
    newv->elem_.node_label = lc;

  current_transaction()->add_dirty_object(&n);
#else
  if (lc > 0)
    n.node_label = lc;
  auto rid =
      properties_->update_properties(n.id(), n.property_list, props, dict_);
  if (rid != n.property_list) {
    auto &n2 = nodes_->get(n.id());
    n2.property_list = rid;
  }
#endif
}

void graph_db::update_relationship(relationship &r, const properties_t &props,
                                   const std::string &label) {
  auto lc = label.empty() ? 0 : dict_->insert(label);
#ifdef USE_TX
  // acquire lock and create a dirty object
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  if (!r.try_lock(txid))
    throw transaction_abort();

  // first, we make a copy of the original node which is stored in
  // the dirty list
  std::list<p_item> pitems =
      properties_->build_dirty_property_list(r.id(), r.property_list);
  // cts is set to txid
  auto &oldv = r.add_dirty_version(std::make_shared<dirty_rship>(r, pitems));
  oldv->elem_.set_timestamps(r.bts, txid);
  oldv->elem_.set_dirty();

  // ... and create another copy as the new version
  pitems = properties_->apply_updates(pitems, props, dict_);
  auto &newv = r.add_dirty_version(std::make_shared<dirty_rship>(r, pitems));
  newv->elem_.set_timestamps(txid, INF);
  newv->elem_.set_dirty();
  if (lc > 0)
    newv->elem_.rship_label = lc;

  current_transaction()->add_dirty_object(&r);
#else
  if (lc > 0)
    r.rship_label = lc;
  auto rid =
      properties_->update_properties(r.id(), r.property_list, props, dict_);
  if (rid != r.property_list) {
    auto &r2 = rships_->get(r.id());
    r2.property_list = rid;
  }
#endif
}

std::size_t graph_db::import_nodes_from_csv(const std::string &label,
                                            const std::string &filename,
                                            char delim, mapping_t &m) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open())
    return 0;

  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns; // name of all fields
  int id_column = -1;               // field no of :ID

  for (auto &row : parser) {
    if (num == 0) {
      // process the header
      std::size_t i = 0;
      for (auto &field : row) {
        auto pos = field.find(":ID");
        if (pos != std::string::npos) {
          // <name>:ID is a special field
          id_column = i;
          columns.push_back(field.substr(0, pos));
        } else
          columns.push_back(field);
        i++;
      }
      assert(id_column >= 0);
    } else {
      properties_t props;
      auto i = 0;
      std::string id_label;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        auto &col = columns[i++];
        if (!col.empty() && !field.empty()) {
          props.insert({col, field});
        }
      }
      auto id = import_node(label, props);
      m.insert({id_label, id});
      // std::cout << "mapping: " << id_label << " -> " << id << std::endl;
    }
    num++;
  }

  return num;
}

std::size_t graph_db::import_relationships_from_csv(const std::string &filename,
                                                    char delim,
                                                    const mapping_t &m) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open())
    return 0;

  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns;
  int start_col = -1, end_col = -1, type_col = -1;

  for (auto &row : parser) {
    if (num == 0) {
      auto i = 0;
      // process header
      for (auto &field : row) {
        columns.push_back(field);
        if (field == ":START_ID")
          start_col = i;
        else if (field == ":END_ID")
          end_col = i;
        else if (field == ":TYPE")
          type_col = i;
        i++;
      }
      assert(start_col >= 0);
      assert(end_col >= 0);
      assert(type_col >= 0);
    } else {
      mapping_t::const_iterator it = m.find(row[start_col]);
      if (it == m.end())
        continue;
      node::id_t from_node = it->second;

      it = m.find(row[end_col]);
      if (it == m.end())
        continue;
      node::id_t to_node = it->second;

      auto &label = row[type_col];

      properties_t props;
      auto i = 0;
      for (auto &field : row) {
        if (i != start_col && i != end_col && i != type_col) {
          auto &col = columns[i];
          if (!field.empty()) {
            props.insert({col, field});
          }
        }
        i++;
      }
      import_relationship(from_node, to_node, label, props);
      // std::cout << "add rship: " << row[start_col] << "(" << from_node <<
      // ")->"
      //    << row[end_col] << "(" << to_node << ")" << std::endl;
    }
    num++;
  }

  return num;
}

const char *graph_db::get_string(dcode_t c) { return dict_->lookup_code(c); }

dcode_t graph_db::get_code(const std::string &s) {
  return dict_->lookup_string(s);
}

void graph_db::dump() {
  nodes_->dump();
  rships_->dump();
}

void graph_db::nodes_by_label(const std::string &label,
                              node_consumer_func consumer) {
#ifdef USE_TX
  check_tx_context();
  xid_t txid = current_transaction()->xid();
#endif
  auto lc = dict_->lookup_string(label);
  for (auto &n : nodes_->as_vec()) {
#ifdef USE_TX
    auto &nv = get_valid_node_version(n, txid);
    if (nv.node_label == lc) {
      consumer(nv);
    }
#else
    if (n.node_label == lc)
      consumer(n);
#endif
  }
}

void graph_db::parallel_nodes(node_consumer_func consumer) {
#ifdef USE_TX
  check_tx_context();
#endif
  const int nchunks = 100;
  spdlog::debug("Start parallel query with {} threads",
                nodes_->num_chunks() / nchunks + 1);

  std::vector<std::future<void>> res;
  res.reserve(nodes_->num_chunks() / nchunks + 1);
  thread_pool pool;
  std::size_t start = 0, end = nchunks - 1;
  while (start < nodes_->num_chunks()) {
    res.push_back(pool.submit(
        scan_task(this, *nodes_, start, end, consumer, current_transaction_)));
    start = end + 1;
    end += nchunks;
  }
  // std::cout << "waiting ..." << std::endl;
  for (auto &f : res)
    f.get();
}

void graph_db::nodes(node_consumer_func consumer) {
#ifdef USE_TX
  check_tx_context();
  xid_t txid = current_transaction()->xid();
#endif

  for (auto &n : nodes_->as_vec()) {
#ifdef USE_TX
    auto &nv = get_valid_node_version(n, txid);
    consumer(nv);
#else
    consumer(n);
#endif
  }
}

void graph_db::nodes_where(const std::string &pkey, p_item::predicate_func pred,
                           node_consumer_func consumer) {
  auto pc = dict_->lookup_string(pkey);
  properties_->foreach_node(pc, pred, [&](offset_t nid) {
    auto &n = this->node_by_id(nid);
    consumer(n);
  });
}

void graph_db::relationships_by_label(const std::string &label,
                                      rship_consumer_func consumer) {
#ifdef USE_TX
  check_tx_context();
  xid_t txid = current_transaction()->xid();
#endif

  auto lc = dict_->lookup_string(label);
  for (auto &r : rships_->as_vec()) {
#ifdef USE_TX
    auto &rv = get_valid_rship_version(r, txid);
    if (rv.rship_label == lc)
      consumer(rv);
#else
    if (r.rship_label == lc)
      consumer(r);
#endif
  }
}

void graph_db::foreach_from_relationship_of_node(const node &n,
                                                 rship_consumer_func consumer) {
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = rship_by_id(relship_id);
    consumer(relship);
    relship_id = relship.next_src_rship;
  }
}

void graph_db::foreach_variable_from_relationship_of_node(
    const node &n, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  rship_queue.push_back(std::make_pair(n.from_rship_list, 1));

  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;
    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = rship_by_id(relship_id);

    if (hops >= min)
      consumer(relship);

    // scan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_src_rship, hops));

    auto &dest = node_by_id(relship.dest_node);
    rship_queue.push_back(std::make_pair(dest.from_rship_list, hops + 1));
  }
}

void graph_db::foreach_variable_from_relationship_of_node(
    const node &n, dcode_t lcode, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  rship_queue.push_back(std::make_pair(n.from_rship_list, 1));

  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;
    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = rship_by_id(relship_id);

    if (relship.rship_label == lcode)
      continue;

    if (hops >= min)
      consumer(relship);

    // scan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_src_rship, hops));

    auto &dest = node_by_id(relship.dest_node);
    rship_queue.push_back(std::make_pair(dest.from_rship_list, hops + 1));
  }
}

void graph_db::foreach_variable_to_relationship_of_node(
    const node &n, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  // the queue of relationships to be considered plus the number of hops to
  // them
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  // initialize the queue with the first relationship
  rship_queue.push_back(std::make_pair(n.to_rship_list, 1));

  // as long we have something to traverse
  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;

    // end of relationships list or too many hops
    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = rship_by_id(relship_id);

    if (hops >= min)
      consumer(relship);

    // we proceed with the next relationship
    rship_queue.push_back(std::make_pair(relship.next_dest_rship, hops));

    // scan recursively: get the node and the first outgoing relationship of
    // this node and add it to the queue
    auto &src = node_by_id(relship.src_node);
    rship_queue.push_back(std::make_pair(src.to_rship_list, hops + 1));
  }
}

void graph_db::foreach_variable_to_relationship_of_node(
    const node &n, dcode_t lcode, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  rship_queue.push_back(std::make_pair(n.to_rship_list, 1));

  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;
    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = rship_by_id(relship_id);

    if (relship.rship_label == lcode)
      continue;

    if (hops >= min)
      consumer(relship);

    // Tscan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_dest_rship, hops));

    auto &src = node_by_id(relship.src_node);
    rship_queue.push_back(std::make_pair(src.to_rship_list, hops + 1));
  }
}

void graph_db::foreach_from_relationship_of_node(const node &n,
                                                 const std::string &label,
                                                 rship_consumer_func consumer) {
  auto lc = dict_->lookup_string(label);
  foreach_from_relationship_of_node(n, lc, consumer);
}

void graph_db::foreach_from_relationship_of_node(const node &n, dcode_t lcode,
                                                 rship_consumer_func consumer) {
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = rship_by_id(relship_id);
    if (relship.rship_label == lcode)
      consumer(relship);
    relship_id = relship.next_src_rship;
  }
}

void graph_db::foreach_to_relationship_of_node(const node &n,
                                               rship_consumer_func consumer) {
  auto relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = rship_by_id(relship_id);
    consumer(relship);
    relship_id = relship.next_dest_rship;
  }
}

void graph_db::foreach_to_relationship_of_node(const node &n,
                                               const std::string &label,
                                               rship_consumer_func consumer) {
  auto lc = dict_->lookup_string(label);
  foreach_to_relationship_of_node(n, lc, consumer);
}

void graph_db::foreach_to_relationship_of_node(const node &n, dcode_t lcode,
                                               rship_consumer_func consumer) {
  auto relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = rship_by_id(relship_id);
    if (relship.rship_label == lcode)
      consumer(relship);
    relship_id = relship.next_dest_rship;
  }
}

bool graph_db::is_node_property(const node &n, const std::string &pkey,
                                p_item::predicate_func pred) {
  auto pc = dict_->lookup_string(pkey);
  return is_node_property(n, pc, pred);
}

bool graph_db::is_node_property(const node &n, dcode_t pcode,
                                p_item::predicate_func pred) {
  auto val = properties_->property_value(n.property_list, pcode);
  return val.empty() ? false : pred(val);
}

bool graph_db::is_relationship_property(const relationship &r,
                                        const std::string &pkey,
                                        p_item::predicate_func pred) {
  auto pc = dict_->lookup_string(pkey);
  return is_relationship_property(r, pc, pred);
}

bool graph_db::is_relationship_property(const relationship &r, dcode_t pcode,
                                        p_item::predicate_func pred) {
  auto val = properties_->property_value(r.id(), pcode);
  return val.empty() ? false : pred(val);
}

void graph_db::copy_properties(node &n, dirty_node_ptr dn) {
  if (dn->properties_.empty())
    return;

  if (dn->updated()) {
    // we have to update the properties
    property_set::id_t pid = properties_->update_pitems(n.id(), n.property_list,
                                                        dn->properties_, dict_);
    n.property_list = pid;
  } else {
    // the node was newly added - we have to add the properties
    // to the properties_ table
    property_set::id_t pid =
        properties_->add_pitems(n.id(), dn->properties_, dict_);
    n.property_list = pid;
  }
}

void graph_db::copy_properties(relationship &r, dirty_rship_ptr dr) {
  if (dr->properties_.empty())
    return;

  if (dr->updated()) {
    // we have to update the properties
    property_set::id_t pid = properties_->update_pitems(r.id(), r.property_list,
                                                        dr->properties_, dict_);
    /// spdlog::info("update node -> set properties to {}", pid);
    r.property_list = pid;
  } else {
    // the relationship was newly added - we have to add the properties
    // to the properties_ table
    property_set::id_t pid =
        properties_->add_pitems(r.id(), dr->properties_, dict_);
    r.property_list = pid;
  }
}
