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

#include <boost/hana.hpp>

#include "qop_builtins.hpp"

using namespace boost::posix_time;

namespace builtin {

query_result forward(query_result &pv) {
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
  } else if (pv.type() == typeid(uint64_t)) {
    return boost::get<uint64_t>(pv);
  } else if (pv.type() == typeid(node_description)) {
    auto &nd = boost::get<node_description>(pv);
    return nd.to_string();
  } else if (pv.type() == typeid(ptime)) {
    return boost::get<ptime>(pv);
  } else if (pv.type() == typeid(array_t)) {
    return boost::get<array_t>(pv);
  } else if (pv.type() == typeid(null_t)) {
      return null_val;
  }
  spdlog::info("builtin::forward: unexpected type: {}", pv.type().name());
  return null_val;
}


bool has_property(query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return nd.has_property(key);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return rd.has_property(key);
  }
  return false;
}

bool has_label(query_result &pv, const std::string &l) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return nd.label == l;
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return rd.label == l;
  }
  return false;
}

query_result get_label(query_result& pv) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<node_description &>(pv);
    return query_result(nd.label);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<rship_description &>(pv);
    return query_result(rd.label);
  }
}

query_result int_property(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    auto o = get_property<int>(nd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    auto o = get_property<int>(rd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  }
  return null_val;
}

query_result double_property(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    auto o = get_property<double>(nd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    auto o = get_property<double>(rd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  }
  return null_val;
}

query_result string_property(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    auto o = get_property<std::string>(nd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    auto o = get_property<std::string>(rd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  }

  return null_val;
}

query_result uint64_property(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    auto o = get_property<uint64_t>(nd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    auto o = get_property<uint64_t>(rd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  }
  return null_val;
}

query_result  ptime_property(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    auto o = get_property<ptime>(nd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    auto o = get_property<ptime>(rd.properties, key);
    return o.has_value() ? query_result(o.value()) : query_result(null_val);
  }
  return null_val;
}

query_result pr_date(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    if (nd.has_property(key)) {
      auto o = get_property<ptime>(nd.properties, key);
      return o.has_value() ? query_result(to_iso_extended_string(o.value().date()))
        : query_result(null_val);
    }
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    if (rd.has_property(key)) {
      auto o = get_property<ptime>(rd.properties, key);
      return o.has_value() ? query_result(to_iso_extended_string(o.value().date()))
        : query_result(null_val);
    }
  }
  return null_val;
}

query_result pr_year(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    if (nd.has_property(key)) {
      auto o = get_property<ptime>(nd.properties, key);
      if (o.has_value()) {
        auto dt = to_iso_extended_string(o.value());
        auto yr = dt.substr(0, dt.find("-"));
        return query_result(std::stoi(yr));
      }
      return query_result(null_val);
    }
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    if (rd.has_property(key)) {
      auto o = get_property<ptime>(rd.properties, key);
      if (o.has_value()) {
        auto dt = to_iso_extended_string(o.value());
        auto yr = dt.substr(0, dt.find("-"));
        return query_result(std::stoi(yr));
      }
      return query_result(null_val);
    }
  }
  return null_val;
}

query_result pr_month(const query_result &pv, const std::string &key) {
  if (pv.type() == typeid(node_description &)) {
    auto nd = boost::get<const node_description &>(pv);
    if (nd.has_property(key)) {
      auto o = get_property<ptime>(nd.properties, key);
      if (o.has_value()) {
        auto dt = to_iso_extended_string(o.value());
        auto mo = dt.substr(5, 2);
        return query_result(std::stoi(mo));
      }
      return query_result(null_val);
    }
  } else if (pv.type() == typeid(rship_description &)) {
    auto rd = boost::get<const rship_description &>(pv);
    if (rd.has_property(key)) {
      auto o = get_property<ptime>(rd.properties, key);
      if (o.has_value()) {
        auto dt = to_iso_extended_string(o.value());
        auto mo = dt.substr(5, 2);
        return query_result(std::stoi(mo));
      }
      return query_result(null_val);
    }
  }
  return null_val;
}

std::string string_rep(query_result &res) {
  auto my_visitor =
      boost::hana::overload([&](node_description &n) { return n.to_string(); },
                            [&](rship_description &r) { return r.to_string(); },
                            [&](node *n) { return std::string(""); },
                            [&](relationship *r) { return std::string(""); },
                            [&](int i) { return std::to_string(i); },
                            [&](double d) { return std::to_string(d); },
                            [&](null_t n) { return std::string("NULL"); },
                            [&](array_t arr) {
                              auto astr = std::string("[ ");
                              for (auto elem : arr.elems)
                                astr += std::to_string(elem);
                              astr += std::string(" ]");
                              return astr; },
                            [&](const std::string &s) { return s; },
                            [&](uint64_t ll) { return std::to_string(ll); },
                            [&](ptime dt) { return to_iso_extended_string(dt); } );
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

std::string int_to_datestring(const query_result& v) {
  assert(v.type() == typeid(int));
 return int_to_datestring(boost::get<int>(v));
}

int datestring_to_int(const std::string &d) {
  boost::gregorian::date dt = boost::gregorian::from_simple_string(d);
  static ptime epoch(boost::gregorian::date(1970, 1, 1));
  time_duration::sec_type secs =
      (ptime(dt, seconds(0)) - epoch).total_seconds();
  return time_t(secs);
}

std::string int_to_dtimestring(int v) {
  auto d = boost::posix_time::from_time_t(v);
  std::ostringstream os;
  // os.imbue(std::locale{std::cout.getloc(), dtf});
  os << d;
  return os.str();
}

std::string int_to_dtimestring(const query_result& v) {
  assert(v.type() == typeid(int));
 return int_to_dtimestring(boost::get<int>(v));
}

int dtimestring_to_int(const std::string &dt) {
  ptime pdt = time_from_string(dt);
  static ptime epoch(boost::gregorian::date(1970, 1, 1));
  time_duration::sec_type secs = (pdt - epoch).total_seconds();
  return time_t(secs);
}

bool is_null(const query_result& pv) { return pv.type() == typeid(null_t); }

} // namespace builtin

std::variant<builtin_func1, builtin_func2> get_builtin_function(const std::string& fname, unsigned int nparams) {
  if (fname == "label")
    return builtin::get_label;
}
