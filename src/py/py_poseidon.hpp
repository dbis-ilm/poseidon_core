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

#ifndef py_poseidon_hpp_
#define py_poseidon_hpp_

#include "defs.hpp"
#include "graph_db.hpp"
#include "query.hpp"

#include <boost/python.hpp>

namespace bp = boost::python;

/**
 * py_napping encapsulates the mapping table needed for importing
 * nodes and relationships from CVS files.
 */
struct py_mapping {
  /**
   * Constructor.
   */
  py_mapping();

  /**
   * Clear the table.
   */
  void reset();

  std::shared_ptr<graph_db::mapping_t> id_map; // the actual mapping table
};

struct py_graph {
  /**
   * Constructors and detructors.
   */
  py_graph();
  py_graph(const py_graph &pg);
  ~py_graph();

  bool open(const std::string &db_name);
  bool create(const std::string &db_name);

  /**
   * Transaction control.
   */
  void begin_transaction();
  void commit_transaction();
  void abort_transaction();

  /**
   * Create the mapping table object for CSV imports.
   */
  py_mapping create_mapping() { return py_mapping(); }

  /**
   * Import nodes for the given label from a CSV file.
   */
  int import_nodes(const std::string &nlabel, const std::string &fname,
                   py_mapping &pm);

  /**
   * Import relationships from a CSV file using the given mapping table.
   */
  int import_relationships(const std::string &fname, py_mapping &pm);

  dcode_t dict_code(const std::string &s) { return gdb_->get_code(s); }
  graph_db_ptr gdb_;
};

/**
 * py_query encapsulates a Poseidon query object. See the documentation
 * of the query class for details.
 */
struct py_query {
  py_query(py_graph pg);
  py_query(std::shared_ptr<query> q) : query_(q) {}

  py_query all_nodes(const std::string &label = "");
  py_query nodes_where(const std::string &label, const std::string &key,
                       bp::object pred);
  py_query print();
  py_query from_relationships(const std::string &label = "");
  py_query to_relationships(const std::string &label = "");
  py_query from_node(const std::string &label = "");
  py_query to_node(const std::string &label = "");
  py_query has_label(const std::string &label);
  py_query limit(std::size_t n);
  py_query crossjoin(py_query &pq);
  py_query createnode(const std::string &label, bp::dict kvargs);

  void start();

  void dump();

  std::shared_ptr<query> query_; // the actual query object
};

#endif