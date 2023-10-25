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
#include <pybind11/pybind11.h>
#include <pybind11/operators.h>
#include <pybind11/stl.h>
#include "defs.hpp"
#include <iostream>

properties_t dict_to_props(const py::dict& props) {
  properties_t node_props;
  for (auto item : props) {
    if (py::isinstance<py::str>(item.second)) {
      node_props[std::string(py::str(item.first))] = std::string(py::str(item.second)); 
    }
    else if (py::isinstance<py::int_>(item.second)) {
      node_props[std::string(py::str(item.first))] = std::any(item.second.cast<int>());
    }
    else if (py::isinstance<py::float_>(item.second)) {
      node_props[std::string(py::str(item.first))] = std::any(item.second.cast<double>());
    }
  }
  return node_props;
}

PYBIND11_MODULE(poseidon, m) {
    m.doc() = "poseidon graph database"; // optional module docstring

    m.def("open_pool", &graph_pool::open, py::arg("path"), py::arg("init") = false, "Opens the given graph pool.");

    m.def("create_pool", &graph_pool::create, py::arg("path"), py::arg("size") = 1024*1024*40000ull, 
      "Creates a new graph pool of the given size.");
  
    py::class_<graph_pool>(m, "GraphPool")
      .def("open_graph", &graph_pool::open_graph, py::arg("name"), "Opens the graph with the given name.")
      .def("create_graph", &graph_pool::create_graph, py::arg("name"), "Creates a new graph with the given name.")
      .def("drop_graph", &graph_pool::drop_graph, py::arg("name"), "Deletes the given graph.")
      .def("close", &graph_pool::close, "Closes the graph pool.");

    py::class_<graph_db, std::shared_ptr<graph_db> >(m, "Graph")
      .def("begin", &graph_db::begin_transaction, "Begins the transaction.")
      .def("commit", &graph_db::commit_transaction, "Commits the transaction.")
      .def("abort", &graph_db::abort_transaction, "Aborts the transaction.")
      .def("get_node", &graph_db::get_node_description)
      .def("create_node", [](graph_db& gdb, const std::string &label, const py::dict &props) {
          properties_t node_props = dict_to_props(props);
          return gdb.add_node(label, node_props);
        })
      .def("create_relationship", [](graph_db& gdb, node::id_t from_node, node::id_t to_node, const std::string &label, const py::dict &props) {
        properties_t rel_props = dict_to_props(props);
        return gdb.add_relationship(from_node, to_node, label, rel_props);
      })
      .def("get_to_relationships", [](graph_db& gdb, node::id_t to_node) {
        graph_db_ptr gptr(&gdb);
        query_ctx ctx(gptr);
        auto& n = gdb.node_by_id(to_node);
        std::vector<rship_description> rships;
        ctx.foreach_to_relationship_of_node(n, [&](relationship& r) {
          auto rel = gdb.get_rship_description(r.id());
          rships.push_back(rel);
        });
        return rships;
      });

      py::class_<node_description>(m, "Node") 
        .def_readonly("id", &node_description::id)
        .def_readonly("label", &node_description::label)
        .def("__repr__", &node_description::to_string);

      py::class_<rship_description>(m, "Relationship") 
        .def_readonly("id", &rship_description::id)
        .def_readonly("label", &rship_description::label)
        .def_readonly("to_node", &rship_description::to_id)
        .def_readonly("from_node", &rship_description::from_id)
        .def("__repr__", &rship_description::to_string);

}
