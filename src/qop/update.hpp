/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef update_hpp_
#define update_hpp_

#include "qop.hpp"

/**
 * create_node represents a query operator for creating a new node. The operator
 * can act as a root operator (via start) are also as subscriber.
 */
struct create_node : public qop {
  create_node(const std::string &l) : label(l) {}
  create_node(const std::string &l, const properties_t &p) : label(l), props(p) {}
  ~create_node() = default;

  void dump(std::ostream &os) const override;

  virtual void start(graph_db_ptr &gdb) override;
  void process(graph_db_ptr &gdb, const qr_tuple &v) { /* TODO */
  }

  std::string label;
  properties_t props;
};

/**
 * create_relationship represents a query operator for creating a new
 * relationship. The operator connects the two last nodes if the query result.
 */
struct create_relationship : public qop {
  create_relationship(const std::string &l) : label(l) {}
  create_relationship(const std::string &l, const properties_t &p)
      : label(l), props(p) {}
  ~create_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(graph_db_ptr &gdb, const qr_tuple &v);

  std::string label;
  properties_t props;
};

/**
 * update_node represents a query operator for updating properties of a node
 * which is passed via consume parameters.
 */
struct update_node : public qop {
  update_node(std::size_t var, properties_t &p) : var_no_(var), props(p) {}
  update_node() : var_no_(0) {}
  ~update_node() = default;

  void dump(std::ostream &os) const override;

  void process(graph_db_ptr &gdb, const qr_tuple &v);

  std::size_t var_no_;
  properties_t props;
};

#endif