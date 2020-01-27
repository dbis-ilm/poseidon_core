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

static std::regex float_expr("[0-9]+\\.[0-9]+");

static std::regex int_expr("(-?[1-9][0-9]*)|0");

bool is_quoted_string(const std::string &s) {
  return (s[0] == '"' && s[s.length() - 1] == '"') ||
         (s[0] == '\'' && s[s.length() - 1] == '\'');
}

bool is_float(const std::string &s) { return std::regex_match(s, float_expr); }

bool is_int(const std::string &s) { return std::regex_match(s, int_expr); }

/* --------------------------------------------------------------------- */

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

template <> void p_item::set<double>(double v) {
  P_SET_VAL(flags_, p_double);
  memcpy(&value_, &v, sizeof(double));
}

template <> void p_item::set<int>(int v) {
  P_SET_VAL(flags_, p_int);
  memcpy(&value_, &v, sizeof(int));
}

template <> void p_item::set<dcode_t>(dcode_t v) {
  P_SET_VAL(flags_, p_dcode);
  memcpy(&value_, &v, sizeof(dcode_t));
}

p_item::p_item(dcode_t k, double v) : flags_(0), key_(k) { set<double>(v); }
p_item::p_item(dcode_t k, int v) : flags_(0), key_(k) { set<int>(v); }
p_item::p_item(dcode_t k, dcode_t v) : flags_(0), key_(k) { set<dcode_t>(v); }

p_item::p_item(const std::string &k, const boost::any &v, dict_ptr &dct)
    : p_item(v, dct) {
  key_ = dct->insert(k);
}

p_item::p_item(dcode_t k, const boost::any &v, dict_ptr &dct) : p_item(v, dct) {
  key_ = k;
}

p_item::p_item(const boost::any &v, dict_ptr &dct) : flags_(0), key_(0) {
  P_SET_VAL(flags_, p_unused);
  try {
    std::string s = boost::any_cast<std::string>(v);
    if (is_quoted_string(s))
      set<dcode_t>(dct->insert(s));
    else if (is_int(s))
      set<int>((int)std::stoi(s));
    else if (is_float(s))
      set<double>((double)std::stod(s));
    else
      set<dcode_t>(dct->insert(s));
    return;
  } catch (boost::bad_any_cast &e) {
    // do nothing, just continue
  }
  try {
    double d = boost::any_cast<double>(v);
    set<double>(d);
    return;
  } catch (boost::bad_any_cast &e) {
    // do nothing, just continue
  }
  set<int>(boost::any_cast<int>(v));
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

/* --------------------------------------------------------------------- */

property_set::id_t property_list::add_node_properties(offset_t nid,
                                                      const properties_t &props,
                                                      dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      if (properties_.is_full())
        properties_.resize(1);
      auto id = properties_.first_available();
      assert(id != UNKNOWN);

      properties_.store_at(id,
                           property_set(nid, std::move(pil), next_id, true));

      next_id = id;
      pidx = 0;
      pil.fill(p_item());
    }
  }
  return next_id;
}

property_set::id_t property_list::add_pitems(offset_t nid,
                                             const std::list<p_item> &props,
                                             dict_ptr &dct, bool is_node) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      if (properties_.is_full())
        properties_.resize(1);
      auto id = properties_.first_available();
      assert(id != UNKNOWN);

      properties_.store_at(id,
                           property_set(nid, std::move(pil), next_id, is_node));

      next_id = id;
      pidx = 0;
      pil.fill(p_item());
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
  for (auto &pi : props) {
    pil[pidx++] = pi;
    if (++n == props.size() || pidx == pil.max_size()) {
      if (id != UNKNOWN) {
        auto &old = properties_.at(id);
        old.items = pil;
        id = old.next;
        /// spdlog::info("next id: {}", id);
        pidx = 0;
        pil.fill(p_item());
      } else {
        // we have some additional properties which do not fit on the
        // current properties_ entries -> let's create a new one
        if (properties_.is_full())
          properties_.resize(1);
        auto new_id = properties_.first_available();
        assert(new_id != UNKNOWN);

        properties_.store_at(new_id,
                             property_set(nid, std::move(pil), head_id, true));

        head_id = new_id;
        pidx = 0;
        pil.fill(p_item());
      }
    }
  }
  return head_id;
}

property_set::id_t
property_list::append_node_properties(offset_t nid, const properties_t &props,
                                      dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(nid, std::move(pil), next_id, true));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item());
    }
  }
  return next_id;
}

property_set::id_t property_list::add_relationship_properties(
    offset_t rid, const properties_t &props, dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      if (properties_.is_full())
        properties_.resize(1);
      auto id = properties_.first_available();
      assert(id != UNKNOWN);

      properties_.store_at(id,
                           property_set(rid, std::move(pil), next_id, false));

      next_id = id;
      pidx = 0;
      pil.fill(p_item());
    }
  }
  return next_id;
}

property_set::id_t property_list::append_relationship_properties(
    offset_t rid, const properties_t &props, dict_ptr &dct) {
  property_set::id_t next_id = UNKNOWN;
  property_set::p_item_list pil;
  std::size_t pidx = 0, n = 0;
  for (auto &kv : props) {
    pil[pidx++] = p_item(kv.first, kv.second, dct);
    if (++n == props.size() || pidx == pil.max_size()) {
      auto p =
          properties_.append(property_set(rid, std::move(pil), next_id, false));
      next_id = p.first;
      pidx = 0;
      pil.fill(p_item());
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

void property_list::foreach_node(dcode_t pkey, p_item::predicate_func pred,
                                 std::function<void(offset_t)> f) {
  for (auto &p : properties_) {
    if (p.is_node_owner()) {
      for (auto &item : p.items) {
        if (item.key() == pkey && pred(item))
          f(p.owner);
      }
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
      switch (item.typecode()) {
      case p_item::p_int:
        pmap.insert({s, item.get<int>()});
        break;
      case p_item::p_double:
        pmap.insert({s, item.get<double>()});
        break;
      case p_item::p_dcode: {
        auto s2 = dct->lookup_code(item.get<dcode_t>());
        pmap.insert({s, std::string(s2)});
        break;
      }
      case p_item::p_unused:
        break;
      }
    }
    pset_id = p.next;
  }
  return pmap;
}

property_set::id_t property_list::update_properties(offset_t nid, offset_t id,
                                                    const properties_t &props,
                                                    dict_ptr &dct) {
  // first, we build the list of properties to be updated
  using todo_list_t = std::list<std::pair<dcode_t, boost::any>>;
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
            property_set(nid, std::move(pil), next_id, true));
        next_id = p.first;
        pidx = 0;
        pil.fill(p_item());
      }
    }
  }

  return next_id;
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
    offset_t nid, offset_t id /*, const properties_t &props, dict_ptr &dct*/) {
  // we copy the current p_items from the properties_ table
  std::list<p_item> p_item_list;
  offset_t pset_id = id;
  while (pset_id != UNKNOWN) {
    auto &p = properties_.at(pset_id);
    for (auto &item : p.items) {
      p_item_list.push_back(item);
    }
    pset_id = p.next;
  }
#if 0
  // update p_item_list with properties
  std::list<p_item> todo_list;
  for (auto &kv : props) {
    p_item pnew(kv.first, kv.second, dct);
    bool updated = false;
    for (auto &pi : p_item_list) {
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
  p_item_list.splice(p_item_list.end(), todo_list);
#endif
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
    case p_item::p_dcode: {
      auto s2 = dct->lookup_code(item.get<dcode_t>());
      pmap.insert({s, std::string(s2)});
      break;
    }
    case p_item::p_unused:
      break;
    }
  }
  return pmap;
}
