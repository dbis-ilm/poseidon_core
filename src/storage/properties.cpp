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

#include "spdlog/spdlog.h"
#include <boost/hana.hpp>
#include <iostream>
#include <regex>

#include "properties.hpp"

static std::regex float_expr("-?[0-9]+\\.[0-9]+");

static std::regex int_expr("(-?[1-9][0-9]*)|0");

static std::regex date_expr("^((19[7-9][0-9])|(20[0-9][0-9]))-" // YYYY-MM-DD
                            "(0[1-9]|1[012])-"
                            "(0[1-9]|[12][0-9]|3[01])$");

static std::regex dtime_expr("^((19[7-9][0-9])|(20[0-9][0-9]))-" // YYYY-MM-DDTHH:MM:SS:ZZZZ
                            "(0[1-9]|1[012])-"
                            "(0[1-9]|[12][0-9]|3[01])T"
                            "([01][0-9]|2[0-3]):"
                            "([0-5][0-9]):"
                            "([0-5][0-9])."
                            "[0-9]{3}[+]"
                            "(([0-9]{4})|([0-9]{2}:[0-9]{2}))$");

bool is_quoted_string(const std::string &s) {
  return (s[0] == '"' && s[s.length() - 1] == '"') ||
         (s[0] == '\'' && s[s.length() - 1] == '\'');
}

bool is_float(const std::string &s) { return std::regex_match(s, float_expr); }

bool is_int(const std::string &s) { return std::regex_match(s, int_expr); }

bool is_date(const std::string &s) { return std::regex_match(s, date_expr); }

bool is_dtime(const std::string &s) { return std::regex_match(s, dtime_expr); }

/* --------------------------------------------------------------------- */
using namespace boost::posix_time;

template <> double p_item::get<double>() const {
  assert(P_DOUBLE_VAL(flags_));
  return *(reinterpret_cast<const double *>(value_));
}

template <> int p_item::get<int>() const {
  assert(P_INT_VAL(flags_));
  return *(reinterpret_cast<const int *>(value_));
}

template <> dcode_t p_item::get<dcode_t>() const {
  assert(P_DICT_VAL(flags_));
  return *(reinterpret_cast<const dcode_t *>(value_));
}

template <> uint64_t p_item::get<uint64_t>() const {
  assert(P_UINT64_VAL(flags_));
  return *(reinterpret_cast<const uint64_t *>(value_));
}

template <> ptime p_item::get<ptime>() const {
  assert(P_PTIME_VAL(flags_));
  return *(reinterpret_cast<const ptime *>(value_));
}

template <> void p_item::set<double>(double v) {
  P_SET_VAL(flags_, p_double);
  memcpy(&value_, &v, sizeof(double));
}

template <> void p_item::set<int>(int v) {
  P_SET_VAL(flags_, p_int);
  memset(&value_, 0, 8);
  memcpy(&value_, &v, sizeof(int));
}

template <> void p_item::set<dcode_t>(dcode_t v) {
  P_SET_VAL(flags_, p_dcode);
  memset(&value_, 0, 8);
  memcpy(&value_, &v, sizeof(dcode_t));
}

template <> void p_item::set<uint64_t>(uint64_t v) {
  P_SET_VAL(flags_, p_uint64);
  memset(&value_, 0, 8);
  memcpy(&value_, &v, sizeof(uint64_t));
}

template <> void p_item::set<ptime>(ptime v) {
  P_SET_VAL(flags_, p_ptime);
  memset(&value_, 0, 8);
  memcpy(&value_, &v, sizeof(ptime));
}

p_item::p_item(dcode_t k, p_item::p_typecode tc, const std::any &v) : key_(k), flags_(0) {
  switch(tc) {
    case p_int: set<int>(std::any_cast<int>(v)); break;
    case p_double: set<double>(std::any_cast<double>(v)); break;
    case p_dcode: set<dcode_t>(std::any_cast<dcode_t>(v)); break;
    case p_uint64: set<uint64_t>(std::any_cast<uint64_t>(v)); break;
    case p_ptime: 
    case p_date: set<ptime>(std::any_cast<ptime>(v)); break;
    default: break;
  }  
}


p_item::p_item(dcode_t k, p_item::p_typecode tc, const std::string& v,dict_ptr &dict) : key_(k), flags_(0) {

	  switch(tc) {
	    case p_int    : set<int>(std::stoi(v)); 	    break;
	    case p_double : set<double>(std::stod(v));   	break;
	    case p_dcode  : set<dcode_t>(dict->insert(v));  break;
	    case p_uint64 : set<uint64_t>(std::stoull(v));  break;
	    case p_ptime  : set<ptime>(boost::posix_time::time_from_string([&](){std::string s=v; s[s.find("T")] = ' '; 
	                    return s.substr(0, s.find("+"));}())); break;
	    case p_date   : set<ptime>(boost::posix_time::ptime([&](){ return boost::gregorian::from_simple_string(v);}(),
	    		     boost::posix_time::seconds(0)));break;
	    default: break;
	  }
}


p_item::p_item(dcode_t k, double v) : key_(k), flags_(0) { set<double>(v); }
p_item::p_item(dcode_t k, int v) : key_(k), flags_(0) { set<int>(v); }
p_item::p_item(dcode_t k, dcode_t v) : key_(k), flags_(0) { set<dcode_t>(v); }
p_item::p_item(dcode_t k, uint64_t v) : key_(k), flags_(0) { set<uint64_t>(v); }
p_item::p_item(dcode_t k, boost::posix_time::ptime v) : key_(k), flags_(0) { set<ptime>(v); }

p_item::p_item(const std::string &k, const std::any &v, dict_ptr &dct)
    : p_item(v, dct) {
  key_ = dct->insert(k);
}

p_item::p_item(dcode_t k, const std::any &v, dict_ptr &dct) : p_item(v, dct) {
  key_ = k;
}

p_item::p_item(const std::any &v, dict_ptr &dct) : key_(0), flags_(0) {
  P_SET_VAL(flags_, p_unused);

  if (v.type() == typeid(uint64_t)){
    set<uint64_t>(std::any_cast<uint64_t>(v));
    return;
  }
  try {
    std::string s = std::any_cast<std::string>(v);
    if (is_quoted_string(s))
      set<dcode_t>(dct->insert(s));
    else if (is_int(s))
      set<int>((int)std::stoi(s));
    else if (is_float(s))
      set<double>((double)std::stod(s));
    else if (is_date(s)){
      boost::gregorian::date dt = boost::gregorian::from_simple_string(s);
      set<ptime>(ptime(dt, seconds(0)));
    }
    else if (is_dtime(s)){
      s[s.find("T")] = ' ';
      auto dts = s.substr(0, s.find("+"));
      set<ptime>(time_from_string(dts));
    }
    else 
      set<dcode_t>(dct->insert(s));
    return;
  } catch (std::bad_any_cast &e) {
    // do nothing, just continue
  }
  try {
    ptime dt = std::any_cast<ptime>(v);
    set<ptime>(dt);
    return;
  } catch (std::bad_any_cast &e) {
    // do nothing, just continue
  }
  try {
    double d = std::any_cast<double>(v);
    set<double>(d);
    return;
  } catch (std::bad_any_cast &e) {
    // do nothing, just continue
  }
  try {
    set<int>(std::any_cast<int>(v));
  } catch (std::bad_any_cast &e) {
    spdlog::info("ERROR: Cannot get or set int value.");
  }
}

p_item &p_item::operator=(const p_item &p) {
  flags_ = p.flags_;
  key_ = p.key_;
  memcpy(value_, p.value_, 8);
  return *this;
}

bool p_item::equal(int i) const {
  if (P_INT_VAL(flags_))
    return get<int>() == i;
  else if (P_DOUBLE_VAL(flags_))
    return get<double>() == i;
  throw invalid_typecast();
}

bool p_item::equal(double d) const {
  if (P_INT_VAL(flags_))
    return get<int>() == d;
  else if (P_DOUBLE_VAL(flags_))
    return get<double>() == d;
  throw invalid_typecast();
}

bool p_item::equal(dcode_t c) const {
  if (P_DICT_VAL(flags_))
    return get<dcode_t>() == c;
  throw invalid_typecast();
}

bool p_item::equal(uint64_t ll) const {
  if (P_UINT64_VAL(flags_))
    return get<uint64_t>() == ll;
  throw invalid_typecast();
}

bool p_item::equal(boost::posix_time::ptime dt) const {
  if (P_PTIME_VAL(flags_))
    return get<ptime>() == dt;
  throw invalid_typecast();
}

uint64_t p_item::get_raw() const {
  return *(reinterpret_cast<const uint64_t *>(value_));
}
  
std::ostream& operator<< (std::ostream& os, const p_item& pi) {
  os << "(" << static_cast<unsigned int>(pi.flags_) << "|" << pi.key_ << "|" << pi.get_raw() << ")";
  return os;
}

/* --------------------------------------------------------------------- */

#if 0
property_set::id_t property_list::add_properties(offset_t nid,
                                                      const properties_t &props,
                                                      dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      if (properties_.is_full())
        properties_.resize(1);
      auto id = properties_.first_available();
      assert(id != UNKNOWN);

      properties_.store_at(id,
                           property_set(nid, std::move(pil), next_id));

      next_id = id;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
}

property_set::id_t property_list::add_pitems(offset_t nid,
                                             const std::list<p_item> &props,
                                             dict_ptr &dct, 
                                             std::function<void(offset_t)> callback) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      std::unique_lock<std::mutex> ulock(m); 
      auto pr = properties_.store(property_set(nid, std::move(pil), next_id), callback);
      ulock.unlock();  
      next_id = pr.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
}

property_set::id_t property_list::update_pitems(offset_t nid, offset_t id,
                                                const std::list<p_item> &props,
                                                dict_ptr &dct) {
  // we know that props contains all previous properties
  // thus, we can overwrite all existing property_sets starting with id
  property_set::id_t head_id = id;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      if (id != UNKNOWN) {
        auto &old = properties_.at(id);
        old.items = pil;
        id = old.next;
        /// spdlog::info("next id: {}", id);
        pidx = 0;
        pil.fill(p_item(0));
      } else {
        // we have some additional properties which do not fit on the
        // current properties_ entries -> let's create a new one
        if (properties_.is_full())
          properties_.resize(1);
        auto new_id = properties_.first_available();
        assert(new_id != UNKNOWN);

        properties_.store_at(new_id,
                             property_set(nid, std::move(pil), head_id));

        head_id = new_id;
        pidx = 0;
        pil.fill(p_item(0));
      }
    }
  }
  return head_id;
}

property_set::id_t
property_list::append_properties(offset_t nid, const properties_t &props,
                                      dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;

  pil.fill(p_item(0));
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
}

property_set::id_t property_list::append_typed_properties(offset_t nid, 
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist, 
                              const std::vector<std::any>& values) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0;
  // spdlog::info("append_typed_properties: {}, {}", values.size(), keys.size());

  pil.fill(p_item(0));
  for (auto i = 0u; i < keys.size(); i++) {
    if (values[i].empty()) // we don't add empty properties
      continue;
    // spdlog::info("property @{} <- {}, type={}", pidx, i, typelist[i]);
    pil[pidx++] = p_item(keys[i], typelist[i], values[i]);
    if (i == keys.size() - 1 || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
}


property_set::id_t property_list::append_typed_properties(offset_t nid,
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist,
							                const std::vector<std::string>& values,
                              dict_ptr &dict) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0;
  // spdlog::info("append_typed_properties: {}, {}", values.size(), keys.size());

  pil.fill(p_item(0));
  for (auto i = 0u; i < keys.size(); i++) {
    if (values[i].empty()) // we don't add empty properties
      continue;
    // spdlog::info("property @{} <- {}, type={}", pidx, i, typelist[i]);
    pil[pidx++] = p_item(keys[i], typelist[i], values[i], dict);
    if (i == keys.size() - 1 || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item(0));
    }
  }
  return next_id;
}


p_item property_list::property_value(offset_t id, dcode_t pkey) {
  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
      if (item.key() == pkey)
        return item;
    }
    pset_id = p.next;
  }
  return p_item();
}

void property_list::foreach(dcode_t pkey, p_item::predicate_func pred,
                                 std::function<void(offset_t)> f) {
  for (auto &p : properties_) {
      for (auto &item : p.items) {
        if (item.key() == pkey && pred(item))
          f(p.owner);
      }
  }
}

properties_t property_list::all_properties(offset_t id, dict_ptr &dct) {
  properties_t pmap;
  if (properties_.capacity() <= id)
    return pmap;

  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
      auto s = dct->lookup_code(item.key());
      // spdlog::info("property: {} - {}", s, item.typecode());
      switch (item.typecode()) {
      case p_item::p_int:
        pmap.insert({s, item.get<int>()});
        break;
      case p_item::p_double:
        pmap.insert({s, item.get<double>()});
        break;
      case p_item::p_uint64:
        pmap.insert({s, item.get<uint64_t>()});
        break;
      case p_item::p_dcode: {
        auto s2 = dct->lookup_code(item.get<dcode_t>());
        pmap.insert({s, std::string(s2)});
        break;
      }
      case p_item::p_ptime:
        pmap.insert({s, item.get<ptime>()});
        break;
      case p_item::p_unused:
        // spdlog::info("{} -> p_unused", s);
        break;
      }
    }
    pset_id = p.next;
  }
  return pmap;
}

void property_list::foreach_property_set(offset_t id, property_list::foreach_cb_func cb) {
  // std::cout << "foreach_property_set..." << std::endl;
  offset_t pset_id = id;
  try {
    while (pset_id != UNKNOWN) {
      auto &p = properties_.at(pset_id);
      cb(pset_id, p.items, p.next);
      pset_id = p.next;
    }
  } catch (unknown_id& exc) {
    throw unknown_property();
  }
}

void property_list::foreach_property(std::function<void(const p_item& pi)> f) {
  for (const auto& ps : properties_) {
    for (const auto& p : ps.items) {
      if (!P_UNUSED(p.flags_))
        f(p);
    }
  }
}

property_set::id_t property_list::update_properties(offset_t nid, offset_t id,
                                                    const properties_t &props,
                                                    dict_ptr &dct) {
  // first, we build the list of properties to be updated
  using todo_list_t = std::list<std::pair<dcode_t, std::any>>;
  todo_list_t todo_list;
  property_set::id_t next_id = id;

  for (auto &pv : props) {
    auto dc = dct->insert(pv.first);
    todo_list.push_back(std::make_pair(dc, pv.second));
  }

  offset_t item = id;
  // next, we process property_set-wise
  while (!todo_list.empty()) {
    assert(item != UNKNOWN);
    auto &pset = properties_.at(item);
    for (auto i = 0u; i < pset.items.size(); i++) {
      todo_list_t::iterator prop =
          std::find_if(todo_list.begin(), todo_list.end(),
                       [&](auto p) { return p.first == pset.items[i].key(); });
      if (prop != todo_list.end()) {
        pset.items[i] = p_item(prop->first, prop->second, dct);
        todo_list.erase(prop);
      }
    }
    if (!todo_list.empty()) {
      // fetch the next property_set record
      item = pset.next;
      // if this was the last one, let's stop here
      if (item == UNKNOWN)
        break;
    }
  }

  // finally, we have to add the remaining properties
  if (!todo_list.empty()) {
    property_set::p_item_list pil;
    std::size_t pidx = 0, n = 0;
    for (auto &kv : todo_list) {
      pil[pidx++] = p_item(kv.first, kv.second, dct);
      if (++n == todo_list.size() || pidx == pil.max_size()) {
        auto p = properties_.append(
            property_set(nid, std::move(pil), next_id));
        next_id = p.first;
        pidx = 0;
        pil.fill(p_item(0));
      }
    }
  }

  return next_id;
}

property_set &property_list::get(property_set::id_t id){
  if (properties_.capacity() <= id)
    throw unknown_id();
  auto &pset = properties_.at(id);
  return pset;
}

void property_list::remove(property_set::id_t id) {
  // std::cout << "property_list::remove: " << id << std::endl;
  if (properties_.capacity() <= id)
    throw unknown_id();
  properties_.erase(id);
}

void property_list::remove_properties(property_set::id_t id) {
  // std::cout << "property_list::remove_properties: " << id << std::endl;
  if (id == UNKNOWN)
    return;
    
  if (properties_.capacity() <= id)
    throw unknown_property();

  auto pset_id = id;
  while (pset_id != UNKNOWN) {
    auto pid = pset_id;
    auto &p = properties_.at(pset_id);
    pset_id = p.next;
    properties_.erase(pid);
  }
}

std::list<p_item>
property_list::build_dirty_property_list(const properties_t &props,
                                         dict_ptr &dct) {
  std::list<p_item> p_item_list;
  for (auto &kv : props) {
    p_item_list.push_back(p_item(kv.first, kv.second, dct));
  }
  return p_item_list;
}

std::list<p_item> property_list::build_dirty_property_list(
   // offset_t nid, /* remove unused parameters */
     offset_t id /*, const properties_t &props, dict_ptr &dct*/) {
  // we copy the current p_items from the properties_ table
  std::list<p_item> p_item_list;
  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
        // Optimization: Insert only valid keys to avoid holes in Prpperty list and avoid resource leak.
        if(item.key_ != 0)  
      p_item_list.push_back(item);
    }
    pset_id = p.next;
  }
  return p_item_list;
}

std::list<p_item> &property_list::apply_updates(std::list<p_item> &pitems,
                                                const properties_t &props,
                                                dict_ptr &dct) {
  // update p_item_list with properties
  std::list<p_item> todo_list;
  for (auto &kv : props) {
    p_item pnew(kv.first, kv.second, dct);
    bool updated = false;
    for (auto &pi : pitems) {
      if (pi.key() == pnew.key()) {
        pi = pnew;
        updated = true;
        break;
      }
    }
    if (!updated)
      todo_list.push_back(pnew);
  }
  // append the elements from todo_list
  pitems.splice(pitems.end(), todo_list);
  return pitems;
}

properties_t
property_list::build_properties_from_pitems(const std::list<p_item> &pitems,
                                            dict_ptr &dct) {
  properties_t pmap;

  for (auto &item : pitems) {
    auto s = dct->lookup_code(item.key());
    switch (item.typecode()) {
    case p_item::p_int:
      pmap.insert({s, item.get<int>()});
      break;
    case p_item::p_double:
      pmap.insert({s, item.get<double>()});
      break;
    case p_item::p_uint64:
      pmap.insert({s, item.get<uint64_t>()});
      break;
    case p_item::p_dcode: {
      auto s2 = dct->lookup_code(item.get<dcode_t>());
      pmap.insert({s, std::string(s2)});
      break;
    }
    case p_item::p_ptime:
      pmap.insert({s, item.get<ptime>()});
      break;
    case p_item::p_unused:
      break;
    }
  }
  return pmap;
}

void property_list::dump() {
  std::cout << "------- PROPERTY SETS -------" << std::endl;
  for (const auto& ps : properties_) {
    std::cout << "owner=" 
      << ps.owner 
      << ", next=" << uint64_to_string(ps.next) 
      << ", items=[";
    for (auto i = 0u; i < 3; i++) {
      std::cout << std::hex << ps.items[i].key_ << ",";
    }
    std::cout << "]" << std::endl;
  }
}

#endif