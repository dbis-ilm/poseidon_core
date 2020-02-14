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

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/hana.hpp>

#include "qop.hpp"

/* ------------------------------------------------------------------------ */

void scan_nodes::start(graph_db_ptr &gdb) {
  if (label.empty())
    gdb->parallel_nodes([&](node &n) { consume_(gdb, {&n}); });
  else
    gdb->nodes_by_label(label, [&](node &n) { consume_(gdb, {&n}); });
  // TODO: in case of calling parallel_nodes we should handle this differently
  qop::default_finish(gdb);
}

void scan_nodes::dump(std::ostream &os) const {
  os << "scan_nodes([" << label << "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void foreach_from_relationship::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto n = boost::get<node *>(v.back());
  if (lcode == 0)
    lcode = gdb->get_code(label);

  gdb->foreach_from_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    consume_(gdb, v2);
  });
}

void foreach_from_relationship::dump(std::ostream &os) const {
  os << "foreach_from_relationship([" << label << "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void foreach_variable_from_relationship::process(graph_db_ptr &gdb,
                                                 const qr_tuple &v) {
  auto n = boost::get<node *>(v.back());
  if (lcode == 0)
    lcode = gdb->get_code(label);

  gdb->foreach_variable_from_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        consume_(gdb, v2);
      });
}

void foreach_variable_from_relationship::dump(std::ostream &os) const {
  os << "foreach_variable_from_relationship([" << label << ", (" << min_range
     << "," << max_range << ")])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void foreach_to_relationship::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto n = boost::get<node *>(v.back());
  if (lcode == 0)
    lcode = gdb->get_code(label);
  gdb->foreach_to_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    consume_(gdb, v2);
  });
}

void foreach_to_relationship::dump(std::ostream &os) const {
  os << "foreach_to_relationship([" << label << "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */
void foreach_variable_to_relationship::process(graph_db_ptr &gdb,
                                               const qr_tuple &v) {
  auto n = boost::get<node *>(v.back());
  if (lcode == 0)
    lcode = gdb->get_code(label);

  gdb->foreach_variable_to_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        consume_(gdb, v2);
      });
}

void foreach_variable_to_relationship::dump(std::ostream &os) const {
  os << "foreach_variable_to_relationship([" << label << ", (" << min_range
     << "," << max_range << ")])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void is_property::dump(std::ostream &os) const {
  os << "is_property([" << property << "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void is_property::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto n = v.back();
  if (pcode == 0)
    pcode = gdb->get_code(property);

  if (n.type() == typeid(node *)) {
    if (gdb->is_node_property(*(boost::get<node *>(n)), pcode, predicate))
      consume_(gdb, v);
  } else if (n.type() == typeid(relationship *)) {
    if (gdb->is_relationship_property(*(boost::get<relationship *>(n)), pcode,
                                      predicate))
      consume_(gdb, v);
  }
}

/* ------------------------------------------------------------------------ */

void node_has_label::dump(std::ostream &os) const {
  os << "node_has_label([" << label << "])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void node_has_label::process(graph_db_ptr &gdb, const qr_tuple &v) {
  if (lcode == 0)
    lcode = gdb->get_code(label);
  auto n = boost::get<node *>(v.back());
  if (n->node_label == lcode) {
    consume_(gdb, v);
  }
}

/* ------------------------------------------------------------------------ */

void get_from_node::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto rship = boost::get<relationship *>(v.back());
  auto v2 = append(v, query_result(&(gdb->node_by_id(rship->src_node))));
  consume_(gdb, v2);
}

void get_from_node::dump(std::ostream &os) const {
  os << "get_from_node()=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void get_to_node::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto rship = boost::get<relationship *>(v.back());
  auto v2 = append(v, query_result(&(gdb->node_by_id(rship->dest_node))));
  consume_(gdb, v2);
}

void get_to_node::dump(std::ostream &os) const {
  os << "get_to_node()=>";
  if (subscriber_)
    subscriber_->dump(os);
}

/* ------------------------------------------------------------------------ */

void printer::dump(std::ostream &os) const { os << "printer()"; }

void printer::process(graph_db_ptr &gdb, const qr_tuple &v) {
  auto my_visitor = boost::hana::overload(
      [&](node *n) { std::cout << gdb->get_node_description(*n); },
      [&](relationship *r) { std::cout << gdb->get_relationship_label(*r); },
      [&](int i) { std::cout << i; }, [&](double d) { std::cout << d; },
      [&](const std::string &s) { std::cout << s; });
  for (auto &ge : v) {
    boost::apply_visitor(my_visitor, ge);
    std::cout << " ";
  }
  std::cout << "\n";
}

/* ------------------------------------------------------------------------ */

void limit_result::dump(std::ostream &os) const {
  os << "limit([" << num_ << "])";
  if (subscriber_)
    subscriber_->dump(os);
}

void limit_result::process(graph_db_ptr &gdb, const qr_tuple &v) {
  if (processed_ < num_) {
    consume_(gdb, v);
    processed_++;
  }
}

/* ------------------------------------------------------------------------ */

void order_by::dump(std::ostream &os) const {
  os << "order_by([])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void order_by::process(graph_db_ptr &gdb, const qr_tuple &v) {
  results_.append(v);
}

void order_by::finish(graph_db_ptr &gdb) {
  if (cmp_func_ != nullptr)
    results_.sort(cmp_func_);
  else
    results_.sort(sort_spec_);
  for (auto &v : results_.data) {
    consume_(gdb, v);
  }
  finish_(gdb);
}

/* ------------------------------------------------------------------------ */

void result_set::wait() {
  std::unique_lock<std::mutex> lock(m);
  cond_var.wait(lock, [&] { return ready.load(); });
}

void result_set::notify() {
  std::lock_guard<std::mutex> lock(m);
  ready = true;
  cond_var.notify_one();
}

bool result_set::operator==(const result_set &other) const {
  return data == other.data;
}

bool result_set::qr_compare(const qr_tuple &qr1, const qr_tuple &qr2,
                            const sort_spec &spec) {
  // TODO
  return true;
}

void result_set::sort(const sort_spec &spec) {
  data.sort([&](const qr_tuple &v1, const qr_tuple &v2) {
    return qr_compare(v1, v2, spec);
  });
}

void result_set::sort(
    std::function<bool(const qr_tuple &, const qr_tuple &)> cmp) {
  data.sort(cmp);
}

std::ostream &operator<<(std::ostream &os, const result_set &rs) {
  auto my_visitor = boost::hana::overload(
      [&](node *n) { /*os << gdb->get_node_description(*n); */ },
      [&](relationship *r) { /* os << gdb->get_relationship_label(*r); */ },
      [&](int i) { os << i; }, [&](double d) { os << d; },
      [&](const std::string &s) { os << s; });

  for (const qr_tuple &qv : rs.data) {
    os << "{ ";

    auto i = 0u;
    for (const auto &qr : qv) {
      boost::apply_visitor(my_visitor, qr);
      if (++i < qv.size())
        os << ", ";
    }
    os << " }" << std::endl;
  }
  return os;
}

/* ------------------------------------------------------------------------ */

void collect_result::dump(std::ostream &os) const {
  os << "collect_result()";
  if (subscriber_)
    subscriber_->dump(os);
}

void collect_result::process(graph_db_ptr &gdb, const qr_tuple &v) {
  // we transform node and relationship into their string representations ...
  qr_tuple res(v.size());

  auto my_visitor = boost::hana::overload(
      [&](node *n) { return gdb->get_node_description(*n).to_string(); },
      [&](relationship *r) {
        return gdb->get_rship_description(*r).to_string();
      },
      [&](int i) { return std::to_string(i); },
      [&](double d) { return std::to_string(d); },
      [&](const std::string &s) { return s; });
  for (std::size_t i = 0; i < v.size(); i++) {
    res[i] = boost::apply_visitor(my_visitor, v[i]);
  }

  results_.data.push_back(res);
}

void collect_result::finish(graph_db_ptr &gdb) { results_.notify(); }

/* ------------------------------------------------------------------------ */

projection::projection(const expr_list &exprs) : exprs_(exprs) {
  // we build a mapping table where for each expression variable refering to a
  // property a new index is created
  auto it =
      std::max_element(exprs_.begin(), exprs_.end(),
                       [](expr &e1, expr &e2) { return e1.vidx < e2.vidx; });
  nvars_ = it->vidx + 1;
  npvars_ = 0;
  var_map_.resize(nvars_);
  for (auto &ex : exprs_) {
    if (ex.func != nullptr) {
      var_map_[ex.vidx] = nvars_ + ex.vidx;
      npvars_++;
    } else
      var_map_[ex.vidx] = 0;
  }
  /*
  std::ostringstream os;
  os << "var_map_[";
  for (auto v : var_map_)
    os << " " << v;
  os << " ]";
  spdlog::info("{}", os.str());
  */
}

void projection::dump(std::ostream &os) const {
  os << "project([";
  for (auto &ex : exprs_) {
    os << " " << ex.vidx;
    if (ex.func != nullptr)
      os << "+";
  }
  os << " ])=>";
  if (subscriber_)
    subscriber_->dump(os);
}

void projection::process(graph_db_ptr &gdb, const qr_tuple &v) {
  // First, we build a list of all node_/rship_description objects which appear
  // in the query result. This list is used as a cache for property functions.
  // TODO: only for variables where properties are accessed!
  std::vector<projection::pr_result> pv(v.size() + npvars_);
  for (auto i = 0u; i < v.size(); i++) {
    pv[i] = v[i];
    if (var_map_[i] == 0)
      continue;
    // spdlog::info("projection::process: var={}", i);
    if (v[i].type() == typeid(node *)) {
      auto n = boost::get<node *>(v[i]);
      pv[var_map_[i]] = gdb->get_node_description(*n);
    } else if (v[i].type() == typeid(relationship *)) {
      auto r = boost::get<relationship *>(v[i]);
      pv[var_map_[i]] = gdb->get_rship_description(*r);
    }
  }

  // Then, we process all projection functions...
  qr_tuple res(exprs_.size());
  for (auto i = 0u; i < exprs_.size(); i++) {
    auto &ex = exprs_[i];
    if (ex.func != nullptr)
      res[i] = ex.func(pv[var_map_[ex.vidx]]);
    else
      res[i] = builtin::forward(pv[ex.vidx]);
  }

  consume_(gdb, res);
}

/* --------------------------------------------------------------------- */

namespace builtin {

query_result forward(projection::pr_result &pv) {
  if (pv.type() == typeid(node *)) {
    return boost::get<node *>(pv);
  } else if (pv.type() == typeid(relationship *)) {
    return boost::get<relationship *>(pv);
  } else if (pv.type() == typeid(int)) {
    return boost::get<int>(pv);
  } else if (pv.type() == typeid(double)) {
    return boost::get<double>(pv);
  } else if (pv.type() == typeid(std::string)) {
    return boost::get<std::string &>(pv);
  }
  spdlog::info("builtin::forward: unexpected type: {}", pv.type().name());
  return query_result(0);
}

int int_property(projection::pr_result &pv, /*std::size_t vidx, */
                 const std::string &key) {
  //  auto &pv = vec[vidx];
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return get_property<int>(nd.properties, key);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return get_property<int>(rd.properties, key);
  }
  return 0;
}

double double_property(projection::pr_result &pv, /* std::size_t vidx,*/
                       const std::string &key) {
  // auto &pv = vec[vidx];
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return get_property<double>(nd.properties, key);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return get_property<double>(rd.properties, key);
  }
  return 0.0;
}

std::string string_property(projection::pr_result &pv, /*std::size_t vidx, */
                            const std::string &key) {
  // auto &pv = vec[vidx];
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return get_property<std::string>(nd.properties, key);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return get_property<std::string>(rd.properties, key);
  }

  return "";
}

std::string string_rep(projection::pr_result &res /*, std::size_t vidx*/) {
  auto my_visitor =
      boost::hana::overload([&](node_description &n) { return n.to_string(); },
                            [&](rship_description &r) { return r.to_string(); },
                            [&](node *n) { return std::string(""); },
                            [&](relationship *r) { return std::string(""); },
                            [&](int i) { return std::to_string(i); },
                            [&](double d) { return std::to_string(d); },
                            [&](const std::string &s) { return s; });
  return boost::apply_visitor(my_visitor, res);
}

int to_int(const std::string &s) { return std::stoi(s); }

static boost::gregorian::date_facet *df =
    new boost::gregorian::date_facet{"%Y-%m-%d"};

static boost::posix_time::time_facet *dtf =
    new boost::posix_time::time_facet{"%Y-%m-%d %H:%M:%S"};

std::string int_to_datestring(int v) {
  auto d = boost::posix_time::from_time_t(v).date();
  std::ostringstream os;
  os.imbue(std::locale{std::cout.getloc(), df});
  os << d;
  return os.str();
}

int datestring_to_int(const std::string &d) {
  using namespace boost::posix_time;
  boost::gregorian::date dt = boost::gregorian::from_simple_string(d);
  static ptime epoch(boost::gregorian::date(1970, 1, 1));
  time_duration::sec_type secs =
      (ptime(dt, seconds(0)) - epoch).total_seconds();
  return time_t(secs);
}

std::string int_to_dtimestring(int v) {
  auto d = boost::posix_time::from_time_t(v);
  std::ostringstream os;
  //os.imbue(std::locale{std::cout.getloc(), dtf});
  os << d;
  return os.str();
}

int dtimestring_to_int(const std::string &dt) {
  using namespace boost::posix_time;
  ptime pdt = time_from_string(dt);
  static ptime epoch(boost::gregorian::date(1970, 1, 1));
  time_duration::sec_type secs = (pdt - epoch).total_seconds();
  return time_t(secs);
}

} // namespace builtin
