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


#include <algorithm>
#include <iostream>
#include <numeric>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/hana.hpp>

#include "qop_analytics.hpp"
#include "profiling.hpp"

#ifdef USE_GUNROCK
#include "gunrock_bfs.hpp"
#include "gunrock_sssp.hpp"
#include "gunrock_pr.hpp"
#endif


void shortest_path_opr::dump(std::ostream &os) const {
  os << "shortest_path_opr([]) - " << PROF_DUMP;
}

void shortest_path_opr::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  auto start = boost::get<node *>(v[start_stop_.first])->id();
  auto stop = boost::get<node *>(v[start_stop_.second])->id();

  path_visitor pv = [&](node &n, const path &p) { return; }; // TODO

  if (all_spaths_) {
    std::list<path_item> spaths;
    all_unweighted_shortest_paths(ctx, start, stop, bidirectional_, rpred_, pv, spaths);
    for (auto &path : spaths) {
      auto res = v;
      array_t nids(path.get_path());
      res.push_back(query_result(nids));
      consume_(ctx, res);
    }
    PROF_POST(spaths.size());
  }
  else {
    path_item spath;
    bool found = unweighted_shortest_path(ctx, start, stop, bidirectional_,
                                          rpred_, pv, spath);
    if (found) {
      auto res = v;
      array_t nids(spath.get_path());
      res.push_back(query_result(nids));
      consume_(ctx, res);
      PROF_POST(1);
    }
    else PROF_POST(0);
  }
}

/* ------------------------------------------------------------------------ */

void weighted_shortest_path_opr::dump(std::ostream &os) const {
  os << "weighted_shortest_path_opr([]) - " << PROF_DUMP;
}

void weighted_shortest_path_opr::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  auto start = boost::get<node *>(v[start_stop_.first])->id();
  auto stop = boost::get<node *>(v[start_stop_.second])->id();

  path_visitor pv = [&](node &n, const path &p) { return; }; // TODO

  if (all_spaths_) {
    std::list<path_item> spaths;
    all_weighted_shortest_paths(ctx, start, stop, bidirectional_,
                            rpred_, rweight_, pv, spaths);
    for (auto &path : spaths) {
      auto res = v;
      double w = path.get_weight();
      res.push_back(query_result(w));
      consume_(ctx, res);
    }
    PROF_POST(spaths.size());
  }
  else {
    path_item spath;
    bool found = weighted_shortest_path(ctx, start, stop, bidirectional_,
                            rpred_, rweight_, pv, spath);
    if (found) {
      auto res = v;
      double w = spath.get_weight();
      res.push_back(query_result(w));
      consume_(ctx, res);
      PROF_POST(1);
    }
    else PROF_POST(0);
  }
}

/* ------------------------------------------------------------------------ */

void k_weighted_shortest_path_opr::dump(std::ostream &os) const {
  os << "k_weighted_shortest_path_opr([]) - " << PROF_DUMP;
}

void k_weighted_shortest_path_opr::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  auto start = boost::get<node *>(v[start_stop_.first])->id();
  auto stop = boost::get<node *>(v[start_stop_.second])->id();

  std::vector<path_item> spaths;
  path_visitor pv = [&](node &n, const path &p) { return; }; // TODO
  k_weighted_shortest_path(ctx, start, stop, k_, bidirectional_,
                          rpred_, rweight_, pv, spaths);
  for (auto &path : spaths) {
    auto res = v;
    double w = path.get_weight();
    res.push_back(query_result(w));
    consume_(ctx, res);
  }
  PROF_POST(spaths.size());
}

#ifdef USE_GUNROCK
/* ------------------------------------------------------------------------ */

void gunrock_bfs_opr::dump(std::ostream &os) const {
  os << "gunrock_bfs_opr([]) - " << PROF_DUMP;
}

void gunrock_bfs_opr::process(graph_db_ptr &gdb, const qr_tuple &v) {
  PROF_PRE;
  auto start_node_id = boost::get<node *>(v[start_])->id();

  bfs_result bfs_res;
  // auto exec_time =
  //   gunrock_bfs_csr(gdb, start_node_id, bidirectional_, bfs_res, false);

  qr_tuple res; // TODO finish up query

  PROF_POST(0);
  consume_(gdb, res);
}

/* ------------------------------------------------------------------------ */

void gunrock_sssp_opr::dump(std::ostream &os) const {
  os << "gunrock_sssp_opr([]) - " << PROF_DUMP;
}

void gunrock_sssp_opr::process(graph_db_ptr &gdb, const qr_tuple &v) {
  PROF_PRE;
  auto start_node_id = boost::get<node *>(v[start_])->id();

  sssp_result sssp_res;
  // auto exec_time =
  //   gunrock_weighted_sssp_csr(gdb, start_node_id, bidirectional_,
  //                                           rweight_, sssp_res, false);

  qr_tuple res; // TODO finish up query

  PROF_POST(0);
  consume_(gdb, res);
}

/* ------------------------------------------------------------------------ */

void gunrock_pr_opr::dump(std::ostream &os) const {
  os << "gunrock_pr_opr([]) - " << PROF_DUMP;
}

void gunrock_pr_opr::process(graph_db_ptr &gdb, const qr_tuple &v) {
  PROF_PRE;

  pr_result pr_res;
  // auto exec_time =
  //   gunrock_weighted_sssp_csr(gdb, bidirectional_, pr_res, false);

  qr_tuple res; // TODO finish up query

  PROF_POST(0);
  consume_(gdb, res);
}
#endif

/* ------------------------------------------------------------------------ */

void csr_data::dump(std::ostream &os) const {
  os << "csr_data([]) - " << PROF_DUMP;
}

void csr_data::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;

  auto n = pos_ == std::numeric_limits<std::size_t>::max()
                ? boost::get<node *>(v.back())
                : boost::get<node *>(v[pos_]);

  auto offset = 0;
  std::vector<uint64_t> neighbour_ids;
  std::vector<double> rship_weights;

  ctx.foreach_from_relationship_of_node(*n, [&](auto &r) {
    neighbour_ids.push_back(r.to_node_id());
    rship_weights.push_back(weight_func_(r));
    offset++;
  });

  if (bidirectional_) {
    ctx.foreach_to_relationship_of_node(*n, [&](auto &r) {
      neighbour_ids.push_back(r.from_node_id());
      rship_weights.push_back(weight_func_(r));
      offset++;
    });
  }

  auto res = v;
  auto nid = n->id();
  res.push_back(nid);
  res.push_back(offset);
  for (auto i = 0; i < offset; i++) {
    res.push_back(neighbour_ids[i]);
    res.push_back(rship_weights[i]);
  }

  consume_(ctx, res);

  PROF_POST(1);
}
