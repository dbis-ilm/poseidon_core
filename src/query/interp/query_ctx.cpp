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
#include "query_ctx.hpp"
#include "query_pipeline.hpp"
#include "query_printer.hpp"
#include "thread_pool.hpp"

scan_task::scan_task(graph_db_ptr gdb, std::size_t first, std::size_t last, query_ctx::node_consumer_func c, 
  transaction_ptr tp, std::size_t start_pos) : graph_db_(gdb), range_(first, last), consumer_(c), tx_(tp), start_pos_(start_pos) {}

void scan_task::scan(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, query_ctx::node_consumer_func consumer) {
    xid_t xid = 0;
    if (tx) { // we need the transaction pointer in thread-local storage
	    current_transaction_ = tx;
	    xid = tx->xid();				    
    }
    auto iter = gdb->get_nodes()->range(first, last);
    while (iter) {
	    auto &n = *iter;
	    if (n.is_valid()) {
	      auto &nv = gdb->get_valid_node_version(n, xid);
		    consumer(nv);
	  }
	  ++iter;
  }
}

std::function<void(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, 
  query_ctx::node_consumer_func consumer)> scan_task::callee_ = &scan_task::scan;

void scan_task::operator()() {
   callee_(tx_, graph_db_, range_.first, range_.second, consumer_);
}

//-----------------------------------------------------------------------------------------------

scan_task_with_label::scan_task_with_label(graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
  query_ctx::node_consumer_func c, transaction_ptr tp, std::size_t start_pos) : graph_db_(gdb), range_(first, last), label_(label),
  consumer_(c), tx_(tp), start_pos_(start_pos) {}

void scan_task_with_label::scan(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
  query_ctx::node_consumer_func consumer) {
    xid_t xid = 0;
    if (tx) { // we need the transaction pointer in thread-local storage
	    current_transaction_ = tx;
	    xid = tx->xid();				    
    }
    auto iter = gdb->get_nodes()->range(first, last);
    while (iter) {
	    auto &n = *iter;
	    if (n.is_valid()) {
	      auto &nv = gdb->get_valid_node_version(n, xid);
        if (nv.node_label == label)
  		    consumer(nv);
	  }
	  ++iter;
  }
}

std::function<void(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, dcode_t label,
  query_ctx::node_consumer_func consumer)> scan_task_with_label::callee_ = &scan_task_with_label::scan;

void scan_task_with_label::operator()() {
   callee_(tx_, graph_db_, range_.first, range_.second, label_, consumer_);
}

//-----------------------------------------------------------------------------------------------

query_ctx::~query_ctx() {
}
 
void query_ctx::_nodes_by_label(graph_db *gdb, const std::string &label,
                              node_consumer_func consumer) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();
  auto lc = gdb->dict_->lookup_string(label);
  // spdlog::info("_nodes_by_label: '{}' -> {}", label, lc);
  for (auto &n : gdb->nodes_->as_vec()) {

    if (n.is_valid()) {
      auto &nv = gdb->get_valid_node_version(n, txid);
      if (nv.node_label == lc) {
        consumer(nv);
      }
    }
  }
}

void query_ctx::nodes_by_label(const std::string &label,
                              node_consumer_func consumer) {
    _nodes_by_label(gdb_.get(), label, consumer);
}

void query_ctx::nodes_by_label(const std::vector<std::string> &labels,
                              node_consumer_func consumer) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  std::vector<dcode_t> codes(labels.size());
  for (auto i = 0u; i < labels.size(); i++) 
    codes[i] = gdb_->dict_->lookup_string(labels[i]);
  for (auto &n : gdb_->nodes_->as_vec()) {

    if (n.is_valid()) {
      auto &nv = gdb_->get_valid_node_version(n, txid);
      for (auto &lc : codes) {
        if (nv.node_label == lc) {
          consumer(nv);
          break;
        }
      }
    }
  }
}

void query_ctx::parallel_nodes(node_consumer_func consumer) {
  check_tx_context();
  std::vector<std::future<void>> res;
  thread_pool pool;

  const int nchunks = 1;
  spdlog::debug("Start parallel query with {} threads",
                gdb_->nodes_->num_chunks() / nchunks + 1);

  res.reserve(gdb_->nodes_->num_chunks() / nchunks + 1);
  std::size_t start = 0, end = nchunks - 1;
  while (start < gdb_->nodes_->num_chunks()) {
    res.push_back(pool.submit(
        scan_task(gdb_, start, end, consumer, current_transaction_)));
    start = end + 1;
    end += nchunks;
  }
 
  // std::cout << "waiting ..." << std::endl;
  for (auto &f : res) {
    f.get();
  }
}

void query_ctx::parallel_nodes(const std::string &label, node_consumer_func consumer) {
  check_tx_context();
  std::vector<std::future<void>> res;
  thread_pool pool;
  auto lc = gdb_->dict_->lookup_string(label);

  const int nchunks = 5;
  spdlog::info("Start parallel query with {} threads",
                gdb_->nodes_->num_chunks() / nchunks + 1);

  res.reserve(gdb_->nodes_->num_chunks() / nchunks + 1);
  std::size_t start = 0, end = nchunks - 1;
  while (start < gdb_->nodes_->num_chunks()) {
    res.push_back(pool.submit(
        scan_task_with_label(gdb_, start, end, lc, consumer, current_transaction_)));
    start = end + 1;
    end += nchunks;
  }
 
  // std::cout << "waiting ..." << std::endl;
  for (auto &f : res) {
    f.get();
  }
}

void query_ctx::nodes(node_consumer_func consumer) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  for (auto &n : gdb_->nodes_->as_vec()) {
    // spdlog::info("#{} ===> {},{} | {}", n.id(), short_ts(n.bts), short_ts(n.cts), short_ts(txid));
    if (n.is_valid()) {
      try {
        auto &nv = gdb_->get_valid_node_version(n, txid);
        consumer(nv);
      } catch (unknown_id& exc) { /* ignore */ }
    }
  }
}

void query_ctx::nodes_where(const std::string &pkey, p_item::predicate_func pred,
                           node_consumer_func consumer) {
  auto pc = gdb_->dict_->lookup_string(pkey);
  gdb_->node_properties_->foreach(pc, pred, [&](offset_t nid) {
    auto &n = gdb_->node_by_id(nid);
    consumer(n);
  });
}

void query_ctx::relationships_by_label(const std::string &label,
                                      rship_consumer_func consumer) {
  check_tx_context();
  xid_t txid = current_transaction()->xid();

  auto lc = gdb_->dict_->lookup_string(label);
  for (auto &r : gdb_->rships_->as_vec()) {
    auto &rv = gdb_->get_valid_rship_version(r, txid);
    if (rv.rship_label == lc)
      consumer(rv);
  }
}

void query_ctx::foreach_from_relationship_of_node(const node &n,
                                                 rship_consumer_func consumer) {
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = gdb_->rship_by_id(relship_id);
    if (relship.is_valid())
      consumer(relship);
    relship_id = relship.next_src_rship;
  }
}

void query_ctx::foreach_variable_from_relationship_of_node(
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

    auto &relship = gdb_->rship_by_id(relship_id);

    if (hops >= min)
      consumer(relship);

    // scan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_src_rship, hops));

    auto &dest = gdb_->node_by_id(relship.dest_node);
    rship_queue.push_back(std::make_pair(dest.from_rship_list, hops + 1));
  }
}

void query_ctx::foreach_variable_from_relationship_of_node(
    const node &n, dcode_t lcode, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  std::set<relationship::id_t> rship_set;
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  rship_queue.push_back(std::make_pair(n.from_rship_list, 1));

  // keep track of potential n-hop rship matches starting 
  // with 1-hop rship matches (n = 1)

  // count all potential 1-hop rship matches
  auto n_hop_rship_cnt = 0;
  auto n_hop_rship_id = n.from_rship_list; 
  while (n_hop_rship_id != UNKNOWN){
    n_hop_rship_cnt++;
    n_hop_rship_id = gdb_->rship_by_id(n_hop_rship_id).next_src_rship;
  }
  std::size_t mr_n_hop = 1;
  auto mr_n_hop_rship_id = n.from_rship_list;
  
  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;
    
    // keep track of the most recently accessed rship and update count 
    if (hops == mr_n_hop){
      mr_n_hop_rship_id = relship_id;
      n_hop_rship_cnt--;
    }

    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = gdb_->rship_by_id(relship_id);

    // just about to exit the while loop
    if (rship_queue.empty() && (relship.rship_label != lcode)){
      // check if any potential n-hop rship match still exists 
      if (n_hop_rship_cnt > 0) {
        mr_n_hop_rship_id = gdb_->rship_by_id(mr_n_hop_rship_id).next_src_rship;
        rship_queue.push_back(std::make_pair(mr_n_hop_rship_id, mr_n_hop));
        continue;
      } 
      // recursively check if any potential (n+1)-hop rship exists
      else if (relship.next_src_rship != UNKNOWN){
        rship_queue.push_back(std::make_pair(relship.next_src_rship, hops));

        // keep track of potential (n+1)-hop rship matches 

        // count all potential (n+1)-hop rship matches
        n_hop_rship_cnt = 0;
        n_hop_rship_id = relship.next_src_rship;
        while (n_hop_rship_id != UNKNOWN){
          n_hop_rship_cnt++;
          n_hop_rship_id = gdb_->rship_by_id(n_hop_rship_id).next_src_rship;
        }
        mr_n_hop = hops;
        mr_n_hop_rship_id = relship.next_src_rship;
        continue;
      }
      // finally exit the while loop if no potential rship exists
      else 
        continue;
    }
    
    if (relship.rship_label != lcode)
      continue;

    if (hops >= min) {
      if (rship_set.find(relship.id()) != rship_set.end())
        continue;
      rship_set.insert(relship.id());
      consumer(relship);
    }

    // scan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_src_rship, hops));

    auto &dest = gdb_->node_by_id(relship.dest_node);
    auto path_rship_id = dest.from_rship_list;
    rship_queue.push_back(std::make_pair(path_rship_id, hops + 1));
    // rship_queue might not be empty after path_rship_id is processed
    while (path_rship_id != UNKNOWN){
      path_rship_id = gdb_->rship_by_id(path_rship_id).next_src_rship;
      rship_queue.push_back(std::make_pair(path_rship_id, hops + 1));
    }
  }
}

void query_ctx::foreach_variable_to_relationship_of_node(
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

    auto &relship = gdb_->rship_by_id(relship_id);

    if (hops >= min)
      consumer(relship);

    // we proceed with the next relationship
    rship_queue.push_back(std::make_pair(relship.next_dest_rship, hops));

    // scan recursively: get the node and the first outgoing relationship of
    // this node and add it to the queue
    auto &src = gdb_->node_by_id(relship.src_node);
    rship_queue.push_back(std::make_pair(src.to_rship_list, hops + 1));
  }
}

void query_ctx::foreach_variable_to_relationship_of_node(
    const node &n, dcode_t lcode, std::size_t min, std::size_t max,
    rship_consumer_func consumer) {
  std::set<relationship::id_t> rship_set;
  std::list<std::pair<relationship::id_t, std::size_t>> rship_queue;
  rship_queue.push_back(std::make_pair(n.to_rship_list, 1));

  // keep track of potential n-hop rship matches starting 
  // with 1-hop rship matches (n = 1)

  // count all potential 1-hop rship matches
  auto n_hop_rship_cnt = 0;
  auto n_hop_rship_id = n.to_rship_list; 
  while (n_hop_rship_id != UNKNOWN){
    n_hop_rship_cnt++;
    n_hop_rship_id = gdb_->rship_by_id(n_hop_rship_id).next_dest_rship;
  }
  std::size_t mr_n_hop = 1;
  auto mr_n_hop_rship_id = n.to_rship_list;
  
  while (!rship_queue.empty()) {
    auto p = rship_queue.front();
    rship_queue.pop_front();
    auto relship_id = p.first;
    auto hops = p.second;
    
    // keep track of the most recently accessed rship and update count 
    if (hops == mr_n_hop){
      mr_n_hop_rship_id = relship_id;
      n_hop_rship_cnt--;
    }

    if (relship_id == UNKNOWN || hops > max)
      continue;

    auto &relship = gdb_->rship_by_id(relship_id);

    // just about to exit the while loop
    if (rship_queue.empty() && (relship.rship_label != lcode)){
      // check if any potential n-hop rship match still exists 
      if (n_hop_rship_cnt > 0) {
        mr_n_hop_rship_id = gdb_->rship_by_id(mr_n_hop_rship_id).next_dest_rship;
        rship_queue.push_back(std::make_pair(mr_n_hop_rship_id, mr_n_hop));
        continue;
      } 
      // recursively check if any potential (n+1)-hop rship exists
      else if (relship.next_dest_rship != UNKNOWN){
        rship_queue.push_back(std::make_pair(relship.next_dest_rship, hops));

        // keep track of potential (n+1)-hop rship matches 

        // count all potential (n+1)-hop rship matches
        n_hop_rship_cnt = 0;
        n_hop_rship_id = relship.next_dest_rship;
        while (n_hop_rship_id != UNKNOWN){
          n_hop_rship_cnt++;
          n_hop_rship_id = gdb_->rship_by_id(n_hop_rship_id).next_dest_rship;
        }
        mr_n_hop = hops;
        mr_n_hop_rship_id = relship.next_dest_rship;
        continue;
      }
      // finally exit the while loop if no potential rship exists
      else 
        continue;
    }
    
    if (relship.rship_label != lcode)
      continue;

    if (hops >= min) {
      if (rship_set.find(relship.id()) != rship_set.end())
        continue;
      rship_set.insert(relship.id());
      consumer(relship);
    }

    // scan recursively!!
    rship_queue.push_back(std::make_pair(relship.next_dest_rship, hops));

    auto &src = gdb_->node_by_id(relship.src_node);
    auto path_rship_id = src.to_rship_list;
    rship_queue.push_back(std::make_pair(path_rship_id, hops + 1));
    // rship_queue might not be empty after path_rship_id is processed
    while (path_rship_id != UNKNOWN){
      path_rship_id = gdb_->rship_by_id(path_rship_id).next_dest_rship;
      rship_queue.push_back(std::make_pair(path_rship_id, hops + 1));
    }
  }
}

void query_ctx::foreach_from_relationship_of_node(const node &n,
                                                 const std::string &label,
                                                 rship_consumer_func consumer) {
  auto lc = gdb_->dict_->lookup_string(label);
  foreach_from_relationship_of_node(n, lc, consumer);
}

void query_ctx::foreach_from_relationship_of_node(const node &n, dcode_t lcode,
                                                 rship_consumer_func consumer) {
  auto relship_id = n.from_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = gdb_->rship_by_id(relship_id);
    if (relship.rship_label == lcode)
      consumer(relship);
    relship_id = relship.next_src_rship;
  }
}

void query_ctx::foreach_to_relationship_of_node(const node &n,
                                               rship_consumer_func consumer) {
  auto relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = gdb_->rship_by_id(relship_id);
    if (relship.is_valid())
      consumer(relship);
    relship_id = relship.next_dest_rship;
  }
}

void query_ctx::foreach_to_relationship_of_node(const node &n,
                                               const std::string &label,
                                               rship_consumer_func consumer) {
  auto lc = gdb_->dict_->lookup_string(label);
  foreach_to_relationship_of_node(n, lc, consumer);
}

void query_ctx::foreach_to_relationship_of_node(const node &n, dcode_t lcode,
                                               rship_consumer_func consumer) {
  auto relship_id = n.to_rship_list;
  while (relship_id != UNKNOWN) {
    auto &relship = gdb_->rship_by_id(relship_id);
    if (relship.rship_label == lcode)
      consumer(relship);
    relship_id = relship.next_dest_rship;
  }
}

bool query_ctx::is_node_property(const node &n, const std::string &pkey,
                                p_item::predicate_func pred) {
  auto pc = gdb_->dict_->lookup_string(pkey);
  return is_node_property(n, pc, pred);
}

bool query_ctx::is_node_property(const node &n, dcode_t pcode,
                                p_item::predicate_func pred) {
  auto val = gdb_->node_properties_->property_value(n.property_list, pcode);
  return val.empty() ? false : pred(val);
}

bool query_ctx::is_relationship_property(const relationship &r,
                                        const std::string &pkey,
                                        p_item::predicate_func pred) {
  auto pc = gdb_->dict_->lookup_string(pkey);
  return is_relationship_property(r, pc, pred);
}

bool query_ctx::is_relationship_property(const relationship &r, dcode_t pcode,
                                        p_item::predicate_func pred) {
  auto val = gdb_->rship_properties_->property_value(r.id(), pcode);
  return val.empty() ? false : pred(val);
}

void query_ctx::start(query_ctx& qctx, std::initializer_list<query_pipeline *> queries) {
  for (auto &q : queries) {
    q->start(qctx);
  }
}

void query_ctx::print_plans(std::initializer_list<query_pipeline *> queries, std::ostream& os) {
    std::vector<qop_node_ptr> trees;
    for (auto &q : queries) {
        auto qop_tree = build_qop_tree(q->plan_head_);
        trees.push_back(qop_tree.first);
    }

    std::list<qop_node_ptr> bin_ops;
    for (auto& t : trees) {
      collect_binary_ops(t, bin_ops);
    }
    // merge trees
    for (auto i = 1u; i < trees.size(); i++) {
        merge_qop_trees(trees[0], trees[i], bin_ops);
    }
    os << ">>---------------------------------------------------------------------->>\n";
    trees[0]->print(os);
    print_plan_helper(os, trees[0], "");
    os << "<<----------------------------------------------------------------------<<\n";
}
