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
