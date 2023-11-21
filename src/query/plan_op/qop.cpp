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

#include <fmt/ostream.h>

#include "qop.hpp"
#include "qop_builtins.hpp"
#include "profiling.hpp"

#include "expr_interpreter.hpp"

using namespace boost::posix_time;

result_set::sort_spec_list sort_spec_;

/* ------------------------------------------------------------------------ */

p_item get_property_value(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop) {
  auto qv = v[var];
  p_item res;
  switch (qv.which()) {
    case node_ptr_type: // node *
      {
        auto nptr = boost::get<node *>(qv);
        res = ctx.gdb_->get_property_value(*nptr, prop);
      }
      break;
    case rship_ptr_type: // relationship *
      {
        auto rptr = boost::get<relationship *>(qv);
        res = ctx.gdb_->get_property_value(*rptr, prop);
      }
      break;
    default:
      // return null
      break;
  }
  return res;
}

template <>
int get_property_value<int>(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop) {
  int res = 0;
  auto qv = get_property_value(ctx, v, var, prop);
  switch (qv.typecode()) {
    case p_item::p_int:
      res = qv.get<int>();
      break;
    case p_item::p_double:
      res = (int)qv.get<double>();
      break;
    case p_item::p_uint64:
      res = (int)qv.get<uint64_t>();
      break;
    default:
      break;
  }
  return res;
}

template <>
uint64_t get_property_value<uint64_t>(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop) {
  uint64_t res = 0;
  auto qv = get_property_value(ctx, v, var, prop);
  switch (qv.typecode()) {
    case p_item::p_int:
      res = qv.get<int>();
      break;
    case p_item::p_double:
      res = (uint64_t)qv.get<double>();
      break;
    case p_item::p_uint64:
      res = qv.get<uint64_t>();
      break;
    default:
      break;
  }
  return res;
}

template <>
double get_property_value<double>(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop) {
  double res = 0;
  auto qv = get_property_value(ctx, v, var, prop);
  switch (qv.typecode()) {
    case p_item::p_int:
      res = (double)qv.get<int>();
      break;
    case p_item::p_double:
      res = qv.get<double>();
      break;
    case p_item::p_uint64:
      res = (double)qv.get<uint64_t>();
      break;
    default:
      break;
  }
  return res;
}

template <>
std::string get_property_value<std::string>(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop) {
  std::string res;
  auto qv = get_property_value(ctx, v, var, prop);
  switch (qv.typecode()) {
    case p_item::p_dcode:
      res = ctx.gdb_->get_string(qv.get<dcode_t>());
      break;
    case p_item::p_int:
      res = std::to_string(qv.get<int>());
      break;
    case p_item::p_double:
      res = std::to_string(qv.get<double>());
      break;
    case p_item::p_uint64:
      res = std::to_string(qv.get<uint64_t>());
      break;
    case p_item::p_ptime:
    case p_item::p_unused:
    // TODO
      break;
  }
  return res;
}

/* ------------------------------------------------------------------------ */

void is_property::dump(std::ostream &os) const {
  os << "is_property([" << property << "]) - " << PROF_DUMP;
}

void is_property::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  bool success = false;
  auto n = v.back();
  if (pcode == 0)
    pcode = ctx.gdb_->get_code(property);

  if (n.type() == typeid(node *)) {
    if (ctx.is_node_property(*(boost::get<node *>(n)), pcode, predicate)) {
      consume_(ctx, v);
      success = true;
    }
  } else if (n.type() == typeid(relationship *)) {
    if (ctx.is_relationship_property(*(boost::get<relationship *>(n)), pcode,
                                      predicate)) {
      consume_(ctx, v);
      success = true;
    }
  }
  PROF_POST(success ? 1 : 0);
}

/* ------------------------------------------------------------------------ */

void node_has_label::dump(std::ostream &os) const {
  os << "node_has_label([" << label << "]) - " << PROF_DUMP;
}

void node_has_label::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  bool success = false;
  auto n = boost::get<node *>(v.back());
  if (labels.empty()) {
    if (lcode == 0)
      lcode = ctx.gdb_->get_code(label);
    if (n->node_label == lcode) {
      consume_(ctx, v);
      success = true;
    }
  }
  else {
    for (auto &label : labels) {
      lcode = ctx.gdb_->get_code(label);
      if (n->node_label == lcode) {
        consume_(ctx, v);
        success = true;
        break;
      }
    }
  }
  PROF_POST(success ? 1 : 0);
}

/* ------------------------------------------------------------------------ */

void get_from_node::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  auto rship = boost::get<relationship *>(v.back());
  auto v2 = append(v, query_result(&(ctx.gdb_->node_by_id(rship->src_node))));
  consume_(ctx, v2);
  PROF_POST(1);
}

void get_from_node::dump(std::ostream &os) const {
  os << "get_from_node() - " << PROF_DUMP;

}

/* ------------------------------------------------------------------------ */

void get_to_node::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  auto rship = boost::get<relationship *>(v.back());
  auto v2 = append(v, query_result(&(ctx.gdb_->node_by_id(rship->dest_node))));
  consume_(ctx, v2);
  PROF_POST(1);
}

void get_to_node::dump(std::ostream &os) const {
  os << "get_to_node()" << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */

template <> struct fmt::formatter<ptime> : ostream_formatter {};
template <> struct fmt::formatter<node_description> : ostream_formatter {};
template <> struct fmt::formatter<rship_description> : ostream_formatter {};

void printer::dump(std::ostream &os) const { os << "printer()"; }

void printer::process(query_ctx &ctx, const qr_tuple &v) {
  if (ntuples_ == 0) {
    std::cout << "+";
    for (auto i = 0u; i < v.size(); i++)
      std::cout << fmt::format("{0:-^{1}}+", "", 20);
    std::cout << "\n";
    output_width_ = 21 * v.size() + 1;
  }
  ntuples_++;
  auto my_visitor = boost::hana::overload(
      [&](const node_description& n) { std::cout << fmt::format(" {:<18} |", n); },
      [&](const rship_description& r) { std::cout << fmt::format(" {:<18} |", r); },
      [&](node *n) { std::cout << fmt::format(" {:<18} |", ctx.gdb_->get_node_description(n->id())); },
      [&](relationship *r) { std::cout << fmt::format(" {:<18} |", ctx.gdb_->get_relationship_label(*r)); },
      [&](int i) { std::cout << fmt::format(" {:>18} |", i); }, 
      [&](double d) { std::cout << fmt::format(" {:>18f} |", d); },
      [&](const std::string &s) { std::cout << fmt::format(" {:<18.18} |", s); },
      [&](uint64_t ll) { std::cout << fmt::format(" {:>18} |", ll); },
      [&](null_t n) { std::cout << fmt::format(" {:>18} |", "NULL"); },
      [&](array_t arr) {
        std::cout << "[ ";
        for (auto elem : arr.elems)
          std::cout << elem << " ";
        std::cout << " ]"; },
      [&](ptime dt) { std::cout << fmt::format(" {:<18.18} |", dt); });
  std::cout << "|";
  for (auto &ge : v) {
    boost::apply_visitor(my_visitor, ge);
  }
  std::cout << "\n";
}

void printer::finish(query_ctx &ctx) {
  auto s = fmt::format("{} tuple(s) returned. ", ntuples_);
  std::cout << "+-- " << s;
  for (int i = output_width_ - s.length() - 5; i > 0; i--) 
    std::cout << "-";
  std::cout << "+\n";
}

/* ------------------------------------------------------------------------ */

void limit_result::dump(std::ostream &os) const {
  os << "limit([" << num_ << "]) - " << PROF_DUMP;
}

void limit_result::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  if (processed_.load() < num_) {
    processed_.fetch_add(1);
    consume_(ctx, v);
    PROF_POST(1);
  }
  else
    PROF_POST(0);
}

/* ------------------------------------------------------------------------ */

std::function<bool(const qr_tuple &, const qr_tuple &)> order_by::cmp_func_ = 0;

void order_by::dump(std::ostream &os) const {
  os << "order_by([";
  if (! sort_spec_.empty()) {
    for (auto& sspec : sort_spec_) {
      os << " " << sspec.vidx << ":" << sspec.s_order;
    }
  }
  os << " ]) - " << PROF_DUMP;
}

void order_by::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  results_.append(v);
  PROF_POST(1);
}

void order_by::finish(query_ctx &ctx) {
  PROF_PRE0;
  if (cmp_func_ != nullptr)
    results_.sort(ctx, cmp_func_);
  else
    results_.sort(ctx, sort_spec_);
  for (auto &v : results_.data) {
    consume_(ctx, v);
  }
  finish_(ctx);
  PROF_POST(0);
}


/* ------------------------------------------------------------------------ */

void distinct_tuples::dump(std::ostream &os) const {
  os << "distinct_tuples() - " << PROF_DUMP;
}

void distinct_tuples::process(query_ctx &ctx, const qr_tuple &v) {
  std::string key = "";
  for (const auto& qres : v) {
    if (qres.type() == typeid(std::string)) {
      key += boost::get<std::string>(qres);
    } else if (qres.type() == typeid(uint64_t)) {
      key += std::to_string(boost::get<uint64_t>(qres));
    } else if (qres.type() == typeid(ptime)) {
      key += to_iso_extended_string(boost::get<ptime>(qres));
    } else if (qres.type() == typeid(node *)) {
      key += std::to_string(boost::get<node *>(qres)->id());
    } else if (qres.type() == typeid(relationship *)) {
      key += std::to_string(boost::get<relationship *>(qres)->id());
    } else if (qres.type() == typeid(int)) {
      key += std::to_string(boost::get<int>(qres));
    } else if (qres.type() == typeid(double)) {
      key += std::to_string(boost::get<double>(qres));
    } else if (qres.type() == typeid(null_val)) {
      key += std::string("NULL");
    } else if (qres.type() == typeid(array_t)) {
      auto arr = boost::get<array_t>(qres).elems;
      for (auto a : arr)
        key += std::to_string(a);
    }
  }

  std::lock_guard<std::mutex> lock(m_);
  if (keys_.find(key) == keys_.end()) {
    keys_.insert(key); // TODO optimize with integer value representation
    consume_(ctx, v);
  }
}

/* ------------------------------------------------------------------------ */

void filter_tuple::dump(std::ostream &os) const {
  os << "filter_tuple([";
  if (ex_) 
    os << ex_->dump();
  os << "]) - " << PROF_DUMP;
}

void filter_tuple::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  bool tp = ex_ ? interpret_expression(ctx, ex_, v) : pred_func1_(v);
  if (tp) {
    consume_(ctx, v);
    PROF_POST(1);
  }
  else PROF_POST(0);
}

/* ------------------------------------------------------------------------ */

void union_all_op::dump(std::ostream &os) const { // TODO
  os << "union_all() - " << PROF_DUMP;
}

void union_all_op::process_left(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  /*
  if (init_) {
    for (auto &r : res_)
      consume_(ctx, r);
    init_ = false;
  }
  */
  consume_(ctx, v);
  PROF_POST(1);
}

void union_all_op::process_right(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  // res_.push_back(v);
  consume_(ctx, v);
  PROF_POST(1);
}

void union_all_op::finish(query_ctx &ctx) { 
  if (++phases_ > 1)
    qop::default_finish(ctx); 
}

/* ------------------------------------------------------------------------ */

void collect_result::dump(std::ostream &os) const {
  os << "collect_result() - " << PROF_DUMP;
}


void collect_result::process(query_ctx &ctx, const qr_tuple &v) {
  std::lock_guard<std::mutex> lock(collect_mtx);
  PROF_PRE;
  // we transform node and relationship into their string representations ...
  qr_tuple res(v.size());
  auto my_visitor = boost::hana::overload(
      [&](const node_description& n) { return n.to_string(); },
      [&](const rship_description& r) { return r.to_string(); },
      [&](node *n) { return ctx.gdb_->get_node_description(n->id()).to_string(); },
      [&](relationship *r) {
        return ctx.gdb_->get_rship_description(r->id()).to_string();
      },
      [&](int i) { return std::to_string(i); },
      [&](double d) { return std::to_string(d); },
      [&](const std::string &s) { return s; },
      [&](uint64_t ll) { return std::to_string(ll); },
      [&](null_t n) { return std::string("NULL"); },
      [&](array_t arr) {
        auto astr = std::string("[ ");
        for (auto elem : arr.elems)
          astr += (std::to_string(elem) + std::string(" "));
        astr += std::string("]");
        return astr; },
      [&](ptime dt) { return to_iso_extended_string(dt); });
  for (std::size_t i = 0; i < v.size(); i++) {
    res[i] = boost::apply_visitor(my_visitor, v[i]);
  }

  results_.data.push_back(res);
  PROF_POST(1);
}

void collect_result::finish(query_ctx &ctx) { 
  results_.notify(); 
}

/* ------------------------------------------------------------------------ */

void end_pipeline::dump(std::ostream &os) const { os << "end_pipeline()"; }

void end_pipeline::process() { return; }

/* ------------------------------------------------------------------------ */

projection::projection(const expr_list &exprs, std::vector<projection_expr>& prexpr) : exprs_(exprs), prexpr_(prexpr) {
  type_ = qop_type::project;
  init_expr_vars();
}

projection::projection(const expr_list &exprs) : exprs_(exprs) {
  type_ = qop_type::project;
  init_expr_vars();
}

void projection::init_expr_vars() {
  if (exprs_.empty())
    return;
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
      accessed_vars_.insert(ex.vidx);
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
  spdlog::info("{}, accessed_vars_={}", os.str(), accessed_vars_.size());
  */
}

void projection::dump(std::ostream &os) const {
  os << "project([";
  for (auto &ex : exprs_) {
    os << " $" << ex.vidx;
    if (ex.func != nullptr)
      os << ".func";
  }
  os << " ]) - " << PROF_DUMP;
}

void projection::process(query_ctx &ctx, const qr_tuple &v) {
  // First, we build a list of all node_/rship_description objects which appear
  // in the query result. This list is used as a cache for property functions.
  PROF_PRE;
  auto i = 0;
  auto num_accessed_vars = accessed_vars_.size();
  std::vector<query_result> pv(num_accessed_vars * 2);
  for (auto index : accessed_vars_) {
    pv[i] = v[index];
    if (var_map_[index] == 0)
      continue;
    if (v[index].type() == typeid(node *)) {
      auto n = boost::get<node *>(v[index]);
      pv[num_accessed_vars + i] = ctx.gdb_->get_node_description(n->id());
    } else if (v[index].type() == typeid(relationship *)) {
      auto r = boost::get<relationship *>(v[index]);
      pv[num_accessed_vars + i] = ctx.gdb_->get_rship_description(r->id());
    }
    else {
      pv[num_accessed_vars + i]  = v[index]; // null_val;
    }
    var_map_[index] = num_accessed_vars + i; // we update mapping table
    i++;
  }

  // Then, we process all projection functions...
  qr_tuple res(exprs_.size());
  for (auto i = 0u; i < exprs_.size(); i++) {
    auto &ex = exprs_[i];
    // spdlog::info("projection::process: pv={}, i={}, vidx={} --> {}", pv.size(), i, ex.vidx, var_map_[ex.vidx]);
    try {
      if (ex.func != nullptr) {
        res[i] = ex.func(ctx, pv[var_map_[ex.vidx]]);
      }
      else {
        query_result fwd = v[ex.vidx];
        res[i] = builtin::forward(fwd);
      }
    } catch (unknown_property& exc) { }
  }

  consume_(ctx, res);
  PROF_POST(1);
}

/* --------------------------------------------------------------------- */
