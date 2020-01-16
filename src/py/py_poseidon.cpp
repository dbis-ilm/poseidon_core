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

#include "py_poseidon.hpp"
#include "defs.hpp"
#include <iostream>

#ifdef USE_PMDK

#define POOL_SIZE ((unsigned long long)(1024 * 1024 * 40000ull)) // 4000 MiB

#define PMEM_PATH "/mnt/pmem0/poseidon/"

struct root {
  graph_db_ptr graph;
};

namespace nvm = pmem::obj;

nvm::pool<root> pop;
#endif

py_mapping::py_mapping() { id_map = std::make_shared<graph_db::mapping_t>(); }

void py_mapping::reset() { id_map->clear(); }

/* -------------------------------------------------------------------- */

py_graph::py_graph() {}

py_graph::~py_graph() {}

py_graph::py_graph(const py_graph &pg) { gdb_ = pg.gdb_; }

bool py_graph::open(const std::string &db_name) {
#ifdef USE_PMDK
  const auto path = PMEM_PATH + db_name;
  pop = nvm::pool<root>::open(path, db_name);
  auto q = pop.root();
  gdb_ = q->graph;
  gdb_->runtime_initialize();
#else
  // TODO: open pmem
  gdb_ = p_make_ptr<graph_db>();
#endif
  return true;
}

bool py_graph::create(const std::string &db_name) {
#ifdef USE_PMDK
  const auto path = PMEM_PATH + db_name;
  pop = nvm::pool<root>::create(path, db_name, POOL_SIZE);
  auto q = pop.root();
  nvm::transaction::run(pop, [&] { q->graph = p_make_ptr<graph_db>(); });
  gdb_ = q->graph;
  gdb_->runtime_initialize();
#else
  // TODO: create pmem
  gdb_ = p_make_ptr<graph_db>();
#endif
  return true;
}

void py_graph::begin_transaction() {
  // TODO: store tx somewhere
  auto tx = gdb_->begin_transaction();
}

void py_graph::commit_transaction() { gdb_->commit_transaction(); }

void py_graph::abort_transaction() { gdb_->abort_transaction(); }

int py_graph::import_nodes(const std::string &nlabel, const std::string &fname,
                           py_mapping &pm) {
  auto num = gdb_->import_nodes_from_csv(nlabel, fname, ',', *pm.id_map);
  return num;
}

int py_graph::import_relationships(const std::string &fname, py_mapping &pm) {
  auto num = gdb_->import_relationships_from_csv(fname, ',', *pm.id_map);
  return num;
}

/* -------------------------------------------------------------------- */

py_query::py_query(py_graph pg) { query_ = std::make_shared<query>(pg.gdb_); }

py_query py_query::all_nodes(const std::string &label) {
  query_->all_nodes(label);
  return py_query(query_);
}

py_query py_query::nodes_where(const std::string &label, const std::string &key,
                               bp::object pred) {
  query_->nodes_where(label, key,
                      [pred](const p_item &pi) { return pred(pi); });
  return py_query(query_);
}

py_query py_query::print() {
  query_->print();
  return py_query(query_);
}

py_query py_query::from_relationships(const std::string &label) {
  query_->from_relationships(label);
  return py_query(query_);
}

py_query py_query::to_relationships(const std::string &label) {
  query_->to_relationships(label);
  return py_query(query_);
}

py_query py_query::from_node(const std::string &label) {
  query_->from_node(label);
  return py_query(query_);
}

py_query py_query::to_node(const std::string &label) {
  query_->to_node(label);
  return py_query(query_);
}

py_query py_query::has_label(const std::string &label) {
  query_->has_label(label);
  return py_query(query_);
}

py_query py_query::limit(std::size_t n) {
  query_->limit(n);
  return py_query(query_);
}

py_query py_query::crossjoin(py_query &pq) {
  query_->crossjoin(*pq.query_.get());
  return py_query(query_);
}

py_query py_query::createnode(const std::string &label, bp::dict kvargs) {
  properties_t props;
  bp::list keys = kvargs.keys();

  for (int i = 0; i < len(keys); i++) {
    auto pkey = bp::extract<std::string>(keys[i]);
    // convert pobj -> pval
    auto pobj = kvargs[keys[i]];
    std::string tname =
        bp::extract<std::string>(pobj.attr("__class__").attr("__name__"));
    if (tname == "int") {
      int ival = bp::extract<int>(pobj);
      props.insert({pkey, ival});
    } else if (tname == "float") {
      double dval = bp::extract<double>(pobj);
      props.insert({pkey, dval});
    } else if (tname == "str") {
      std::string sval = bp::extract<std::string>(pobj);
      props.insert({pkey, sval});
    }
  }
  query_->create(label, props);
  return py_query(query_);
}

void py_query::dump() { query_->dump(std::cout); }

void py_query::start() {
  auto gdb = query_->get_graph_db();
  assert(gdb.get() != nullptr);
  auto tx = gdb->begin_transaction();
  query_->start();
  gdb->commit_transaction();
}

/* -------------------------------------------------------------------- */

// overload for methods with optional label parameters
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(all_nodes_overloads, all_nodes, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(from_relationships_overloads,
                                       from_relationships, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(to_relationships_overloads,
                                       to_relationships, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(from_node_overloads, from_node, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(to_node_overloads, to_node, 0, 1)

BOOST_PYTHON_MODULE(poseidon) {
  bp::class_<p_item>("p_item", bp::no_init)
      .def("dict_eq",
           static_cast<bool (p_item::*)(dcode_t) const>(&p_item::equal))
      .def("eq", static_cast<bool (p_item::*)(int) const>(&p_item::equal))
      .def("eq", static_cast<bool (p_item::*)(double) const>(&p_item::equal));

  bp::class_<py_mapping>("id_map", bp::no_init)
      .def("reset", &py_mapping::reset);

  bp::class_<py_graph>("graph_db")
      .def("open", &py_graph::open)
      .def("create", &py_graph::create)
      .def("mapping", &py_graph::create_mapping)
      .def("import_nodes", &py_graph::import_nodes)
      .def("import_relationships", &py_graph::import_relationships)
      .def("dict_code", &py_graph::dict_code)
      .def("begin", &py_graph::begin_transaction)
      .def("commit", &py_graph::commit_transaction)
      .def("abort", &py_graph::abort_transaction);

  bp::class_<py_query>("query", bp::init<py_graph>())
      .def("all_nodes", &py_query::all_nodes, all_nodes_overloads())
      .def("nodes_where", &py_query::nodes_where)
      .def("print", &py_query::print)
      .def("from_relationships", &py_query::from_relationships,
           from_relationships_overloads())
      .def("to_relationships", &py_query::to_relationships, to_node_overloads())
      .def("from_node", &py_query::from_node, from_node_overloads())
      .def("to_node", &py_query::to_node, to_node_overloads())
      .def("has_label", &py_query::has_label)
      .def("limit", &py_query::limit)
      .def("cross_join", &py_query::crossjoin)
      .def("create_node", &py_query::createnode)
      .def("start", &py_query::start)
      .def("dump", &py_query::dump);
}
