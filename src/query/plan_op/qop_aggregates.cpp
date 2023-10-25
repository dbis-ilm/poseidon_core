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

#include "qop_aggregates.hpp"


int aggregate::get_int_value(query_ctx &ctx, const qr_tuple& v, const expr& ex) {
  return ex.property.empty() ? boost::get<int>(v[ex.var]) 
              : get_property_value<int>(ctx, v, ex.var, ex.property);

}

double aggregate::get_double_value(query_ctx &ctx, const qr_tuple& v, const expr& ex) {
  if (! ex.property.empty())
    return get_property_value<double>(ctx, v, ex.var, ex.property);
  auto qv = v[ex.var];
  if (qv.which() == int_type)
    return (double)boost::get<int>(qv); 
  return boost::get<double>(qv);
}

std::string aggregate::get_string_value(query_ctx &ctx, const qr_tuple& v, const expr& ex) {
  return ex.property.empty() ? boost::get<std::string>(v[ex.var]) 
              : get_property_value<std::string>(ctx, v, ex.var, ex.property);
}

uint64_t aggregate::get_uint64_value(query_ctx &ctx, const qr_tuple& v, const expr& ex) {
  return ex.property.empty() ? boost::get<uint64_t>(v[ex.var]) 
              : get_property_value<uint64_t>(ctx, v, ex.var, ex.property);
}


void aggregate::init_aggregates() {
  for (auto i = 0u; i < aggr_exprs_.size(); i++) {
    auto& ex = aggr_exprs_[i];
    switch (ex.func) {
      case expr::f_count:
        aggr_vals_[i] = 0;
        break;
      case expr::f_avg:
        aggr_vals_[i] = std::make_pair<double,int>(0, 0);
        break;
      case expr::f_sum:
        aggr_vals_[i] = (ex.aggr_type == 2 ? 0 : 0.0);
        break;
      case expr::f_min:
        if (ex.aggr_type == int_type)
          aggr_vals_[i] = std::numeric_limits<int>::max();
        else if (ex.aggr_type == double_type)
          aggr_vals_[i] = std::numeric_limits<double>::max();
        else if (ex.aggr_type == string_type)
          aggr_vals_[i] = std::string("~~~~~~~~~~~~~~~");
        else if (ex.aggr_type == uint64_type)
          aggr_vals_[i] = (uint64_t)std::numeric_limits<uint64_t>::max();
        break;
      case expr::f_max:
        if (ex.aggr_type == int_type)
          aggr_vals_[i] = std::numeric_limits<int>::min();
        else if (ex.aggr_type == double_type)
          aggr_vals_[i] = std::numeric_limits<double>::min();
        else if (ex.aggr_type == string_type)
          aggr_vals_[i] = std::string("                ");
        else if (ex.aggr_type == uint64_type)
          aggr_vals_[i] = (uint64_t)std::numeric_limits<uint64_t>::min();
        break;
      default:
        break;
    }
  }
}

void aggregate::dump(std::ostream &os) const {
  os << "aggregate([ ";
  for (auto& ex : aggr_exprs_) {
    switch (ex.func) {
      case expr::f_count:
        os << "count(";
        break;
      case expr::f_sum:
        os << "sum(";
        break;
      case expr::f_min:
        os << "min(";
        break;
      case expr::f_max:
        os << "max(";
        break;
      case expr::f_avg:
        os << "avg(";
        break;
      // TODO
      default:
        break;
    }
    os << ex.var << "." << ex.property << ") ";
  }
  os << "]) - " << PROF_DUMP;
}

void aggregate::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  for (auto i = 0u; i < aggr_exprs_.size(); i++) {
    auto& ex = aggr_exprs_[i];
    switch (ex.func) {
      case expr::f_count:
        aggr_vals_[i] = boost::get<int>(aggr_vals_[i]) + 1;
        break;
      case expr::f_sum:
        if (ex.aggr_type == int_type)
          aggr_vals_[i] = boost::get<int>(aggr_vals_[i]) + get_int_value(ctx, v, ex);
        else if (ex.aggr_type == double_type)
          aggr_vals_[i] = boost::get<double>(aggr_vals_[i]) + get_double_value(ctx, v, ex);
        break;
      case expr::f_min:
        if (ex.aggr_type == int_type)
          aggr_vals_[i] = std::min(boost::get<int>(aggr_vals_[i]), get_int_value(ctx, v, ex));
        else if (ex.aggr_type == double_type)
          aggr_vals_[i] = std::min(boost::get<double>(aggr_vals_[i]), get_double_value(ctx, v, ex));
        else if (ex.aggr_type == string_type)
          aggr_vals_[i] = std::min(boost::get<std::string>(aggr_vals_[i]), get_string_value(ctx, v, ex));
        else if (ex.aggr_type == uint64_type)
          aggr_vals_[i] = std::min(boost::get<uint64_t>(aggr_vals_[i]), get_uint64_value(ctx, v, ex));
        break;
      case expr::f_max:
        if (ex.aggr_type == int_type)
          aggr_vals_[i] = std::max(boost::get<int>(aggr_vals_[i]), get_int_value(ctx, v, ex));
        else if (ex.aggr_type == double_type)
          aggr_vals_[i] = std::max(boost::get<double>(aggr_vals_[i]), get_double_value(ctx, v, ex));
        else if (ex.aggr_type == string_type)
          aggr_vals_[i] = std::max(boost::get<std::string>(aggr_vals_[i]), get_string_value(ctx, v, ex));
        else if (ex.aggr_type == uint64_type)
          aggr_vals_[i] = std::max(boost::get<uint64_t>(aggr_vals_[i]), get_uint64_value(ctx, v, ex));
        break;
      case expr::f_avg:
        {
          auto p = boost::get<std::pair<double, int>>(aggr_vals_[i]);
          p.first += aggregate::get_double_value(ctx, v, ex);
          p.second++;
          aggr_vals_[i] = p;
          break;
        }
      default:
        break;
    }
  }
  PROF_POST(0);
}

void aggregate::finish(query_ctx &ctx) {
  PROF_PRE0;
  qr_tuple v(aggr_exprs_.size());
  for (auto i = 0u; i < aggr_exprs_.size(); i++) {
    auto& ex = aggr_exprs_[i];
    switch (ex.func) {
      case expr::f_count:
        v[i] = boost::get<int>(aggr_vals_[i]);
        break;
      case expr::f_sum:
        if (ex.aggr_type == int_type)
          v[i] = boost::get<int>(aggr_vals_[i]);
        else if (ex.aggr_type == double_type)
          v[i] = boost::get<double>(aggr_vals_[i]);
        break;
      case expr::f_avg:
        {
          auto& p = boost::get<std::pair<double, int>>(aggr_vals_[i]);
          v[i] = p.first / p.second;
        }
        break;
      case expr::f_min:
      case expr::f_max:
        if (ex.aggr_type == int_type)
          v[i] = boost::get<int>(aggr_vals_[i]);
        else if (ex.aggr_type == double_type)
          v[i] = boost::get<double>(aggr_vals_[i]);
        else if (ex.aggr_type == string_type)
          v[i] = boost::get<std::string>(aggr_vals_[i]);
        else if (ex.aggr_type == uint64_type)
          v[i] = boost::get<uint64_t>(aggr_vals_[i]);
        break;
      // TODO
      default:
        v[i] = 0;
    }
  }  
  consume_(ctx, v);
  finish_(ctx);
  PROF_POST(1);
}

void group_by::dump(std::ostream &os) const {
 os << "group_by([ ";
  for (auto& ex : aggr_exprs_) {
    switch (ex.func) {
      case expr::f_count:
        os << "count(";
        break;
      case expr::f_sum:
        os << "sum(";
        break;
      case expr::f_min:
        os << "min(";
        break;
      case expr::f_max:
        os << "max(";
        break;
      case expr::f_avg:
        os << "avg(";
        break;
      // TODO
      default:
        break;
    }
    os << "$" << ex.var;
    if (!ex.property.empty()) os << "." << ex.property;
    os << ") ";
  }
  os << "],[";
  for (auto& grp : groups_) {
    os << "$" << grp.var;
    if (! grp.property.empty()) os << "." << grp.property;
    os << " ";
  }
  os << "]) - " << PROF_DUMP;
}

uint64_t group_by::hasher(query_ctx &ctx, const qr_tuple& v) {
  uint64_t h = 0u;
  for (auto& g : groups_) {
    auto elem = v[g.var];
    switch (g.grp_type) {
      case node_ptr_type:
        break;
      case rship_ptr_type:
        break;
      case int_type:
      {
        auto i = (!g.property.empty() ?  aggregate::get_int_value(ctx, v, expr(g.var, g.property)) : boost::get<int>(elem));
        h ^= i + 0x9e3779b9 + (h << 6) + (h >> 2);
        break;
      }
      case uint64_type:
      {
        auto u = (!g.property.empty() ?  aggregate::get_uint64_value(ctx, v, expr(g.var, g.property)) : boost::get<uint64_t>(elem));
        h ^= u + 0x9e3779b9 + (h << 6) + (h >> 2);
        break;
      }
      case double_type:
      {
        auto d = (!g.property.empty() ?  aggregate::get_double_value(ctx, v, expr(g.var, g.property)) : boost::get<double>(elem));
        h ^= std::hash<double>{}(d) + 0x9e3779b9 + (h << 6) + (h >> 2);
        break;
      }
      case string_type:
      {
        auto s = (!g.property.empty() ?  aggregate::get_string_value(ctx, v, expr(g.var, g.property)) : boost::get<std::string>(elem));
        h ^= std::hash<std::string>{}(s) + 0x9e3779b9 + (h << 6) + (h >> 2);
        break;
      }
      case node_descr_type:
      case rship_descr_type:
      case ptime_type:
      case array_type:
      default:
        // TODO
        break;
    }
  }
  return h;
}

void group_by::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  aggr_vals_t aval;

  // find corresponding group
  auto key = hasher(ctx, v);
  auto it = aggr_vals_.find(key);
  if (it == aggr_vals_.end()) {
    aval = init_aggregates();
    qr_tuple v2;
    for (auto& g : groups_) {
      v2.insert(std::end(v2), v[g.var]);
    }
    group_keys_.emplace(key, v2);
  }
  else
    aval = it->second;
    
  // update aggregate
  update_aggregates(ctx, aval, v);
  aggr_vals_[key] = aval;

  PROF_POST(0);
}

void group_by::finish(query_ctx &ctx) {
  PROF_PRE0;
  int num = 0;

  for (auto it = aggr_vals_.begin(); it != aggr_vals_.end(); it++) {
    qr_tuple v;
    
    auto it2 = group_keys_.find(it->first);
    assert(it2 != group_keys_.end());
    auto& tup = it2->second;
    for (auto i = 0u; i < tup.size(); i++) {
      if (!groups_[i].property.empty()) {
        auto& g = groups_[i];
        switch (g.grp_type) {
          case int_type:
            v.push_back(aggregate::get_int_value(ctx, tup, expr(i, g.property)));
            break;
          case double_type:
            v.push_back(aggregate::get_double_value(ctx, tup, expr(i, g.property)));
            break;
          case string_type:
            v.push_back(aggregate::get_string_value(ctx, tup, expr(i, g.property)));
            break;
          case uint64_type:
            v.push_back(aggregate::get_uint64_value(ctx, tup, expr(i, g.property)));
            break;
          default:
            break;
        }
      }
      else
        v.push_back(tup[i]);
    } 

    for (auto i = 0u; i < aggr_exprs_.size(); i++) {
      auto& ex = aggr_exprs_[i];
      switch (ex.func) {
        case expr::f_count:
          v.push_back(boost::get<int>(it->second[i]));
          break;
        case expr::f_sum:
          if (ex.aggr_type == int_type)
            v.push_back(boost::get<int>(it->second[i]));
          else if (ex.aggr_type == double_type)
            v.push_back(boost::get<double>(it->second[i]));
          break;
        case expr::f_avg:
          {
            auto& p = boost::get<std::pair<double, int>>(it->second[i]);
            v.push_back(p.first / p.second);
          }
          break;
        case expr::f_min:
        case expr::f_max:
          if (ex.aggr_type == int_type)
            v.push_back(boost::get<int>(it->second[i]));
          else if (ex.aggr_type == double_type)
            v.push_back(boost::get<double>(it->second[i]));
          else if (ex.aggr_type == string_type)
            v.push_back(boost::get<std::string>(it->second[i]));
          else if (ex.aggr_type == uint64_type)
            v.push_back(boost::get<uint64_t>(it->second[i]));
          break;
        // TODO
        default:
          v.push_back(0);
      }
    }  
    consume_(ctx, v);
    num++;
  }
  finish_(ctx);
  PROF_POST(num);
}


group_by::aggr_vals_t group_by::init_aggregates() {
  aggr_vals_t aval(aggr_exprs_.size());

  for (auto i = 0u; i < aggr_exprs_.size(); i++) {
    auto& ex = aggr_exprs_[i];
    switch (ex.func) {
      case expr::f_count:
        aval[i] = 0;
        break;
      case expr::f_avg:
        aval[i] = std::make_pair<double,int>(0, 0);
        break;
      case expr::f_sum:
        if (ex.aggr_type == double_type)
          aval[i] = (double)0.0;
        else
          aval[i] = (int)0;
        break;
      case expr::f_min:
        if (ex.aggr_type == int_type)
          aval[i] = std::numeric_limits<int>::max();
        else if (ex.aggr_type == double_type)
          aval[i] = std::numeric_limits<double>::max();
        else if (ex.aggr_type == string_type)
          aval[i] = std::string("~~~~~~~~~~~~~~~");
        else if (ex.aggr_type == uint64_type)
          aval[i] = (uint64_t)std::numeric_limits<uint64_t>::max();
        break;
      case expr::f_max:
        if (ex.aggr_type == int_type)
          aval[i] = std::numeric_limits<int>::min();
        else if (ex.aggr_type == double_type)
          aval[i] = std::numeric_limits<double>::min();
        else if (ex.aggr_type == string_type)
          aval[i] = std::string("                ");
        else if (ex.aggr_type == uint64_type)
          aval[i] = (uint64_t)std::numeric_limits<uint64_t>::min();
        break;
      default:
        break;
    }
  }
  return aval;
}

void group_by::update_aggregates(query_ctx &ctx, group_by::aggr_vals_t& aval, const qr_tuple& v) {
 for (auto i = 0u; i < aggr_exprs_.size(); i++) {
    auto& ex = aggr_exprs_[i];

    switch (ex.func) {
      case expr::f_count:
        aval[i] = boost::get<int>(aval[i]) + 1;
        break;
      case expr::f_sum:
        if (ex.aggr_type == int_type)
          aval[i] = boost::get<int>(aval[i]) + aggregate::get_int_value(ctx, v, ex);
        else if (ex.aggr_type == double_type)
          aval[i] = boost::get<double>(aval[i]) + aggregate::get_double_value(ctx, v, ex);
        break;
      case expr::f_min:
        if (ex.aggr_type == int_type)
          aval[i] = std::min(boost::get<int>(aval[i]), aggregate::get_int_value(ctx, v, ex));
        else if (ex.aggr_type == double_type)
          aval[i] = std::min(boost::get<double>(aval[i]), aggregate::get_double_value(ctx, v, ex));
        else if (ex.aggr_type == string_type)
          aval[i] = std::min(boost::get<std::string>(aval[i]), aggregate::get_string_value(ctx, v, ex));
        else if (ex.aggr_type == uint64_type)
          aval[i] = std::min(boost::get<uint64_t>(aval[i]), aggregate::get_uint64_value(ctx, v, ex));
        break;
      case expr::f_max:
        if (ex.aggr_type == int_type)
          aval[i] = std::max(boost::get<int>(aval[i]), aggregate::get_int_value(ctx, v, ex));
        else if (ex.aggr_type == double_type)
          aval[i] = std::max(boost::get<double>(aval[i]), aggregate::get_double_value(ctx, v, ex));
        else if (ex.aggr_type == string_type)
          aval[i] = std::max(boost::get<std::string>(aval[i]), aggregate::get_string_value(ctx, v, ex));
        else if (ex.aggr_type == uint64_type)
          aval[i] = std::max(boost::get<uint64_t>(aval[i]), aggregate::get_uint64_value(ctx, v, ex));
        break;
      case expr::f_avg:
          {
            auto p = boost::get<std::pair<double, int>>(aval[i]);
            p.first += aggregate::get_double_value(ctx, v, ex);
            p.second++;
            aval[i] = p;
            break;
          }
      default:
        break;
    }
  }  
}
