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

#include <boost/algorithm/string.hpp>

#include "graph_db.hpp"
#include "vec.hpp"
#include "parser.hpp"
#include "spdlog/spdlog.h"
#include "thread_pool.hpp"
#include <iostream>

std::any string_to_any(p_item::p_typecode tc, const std::string& s, dict_ptr &dict) {
  switch (tc) {
    case p_item::p_dcode: return std::any(dict->insert(s));
    case p_item::p_int: return std::any((int)std::stoi(s));
    case p_item::p_uint64: return std::any((uint64_t)std::stoll(s));
    case p_item::p_double: return std::any((double)std::stod(s));
    case p_item::p_date:
    {
        boost::gregorian::date dt = boost::gregorian::from_simple_string(s);
        return std::any(boost::posix_time::ptime(dt, boost::posix_time::seconds(0)));
    }
    case p_item::p_ptime: 
    {
        std::string s2 = s;
        s2[s.find("T")] = ' ';
        auto dts = s2.substr(0, s2.find("+"));
        return std::any(boost::posix_time::time_from_string(dts));
    }
      default: return std::any();
  }
}

std::pair<p_item::p_typecode, std::any> 
infer_datatype(const std::string& s, dict_ptr &dict) {
  if (is_quoted_string(s))
    return std::make_pair(p_item::p_dcode, std::any(dict->insert(s)));
  else if (is_int(s)) {
    int ival = 0;
    try {
      ival = std::stoi(s);
    }
    catch (std::out_of_range& exc) {
      try {
        return std::make_pair(p_item::p_uint64, std::any((u_int64_t)std::stoull(s)));  
      } catch (std::exception& exc) {
        spdlog::info("ERROR: cannot parse '{}': {}", s, exc.what());
      }
    }
    return std::make_pair(p_item::p_int, std::any(ival));
  }
  else if (is_float(s))
    return std::make_pair(p_item::p_double, std::any((double)std::stod(s)));
  else if (is_date(s)) {
    boost::gregorian::date dt = boost::gregorian::from_simple_string(s);
    return std::make_pair(p_item::p_date, std::any(boost::posix_time::ptime(dt, boost::posix_time::seconds(0))));
  }
  else if (is_dtime(s)) {
    std::string s2 = s;
    s2[s2.find("T")] = ' ';
    auto dts = s2.substr(0, s2.find("+"));
    return std::make_pair(p_item::p_ptime, std::any(boost::posix_time::time_from_string(dts)));
  }
  return std::make_pair(p_item::p_dcode, std::any(dict->insert(s)));
}

p_item::p_typecode
get_datatype(const std::string& s) {

    if (is_quoted_string(s))  return p_item::p_dcode;
    else if (is_int(s))       return p_item::p_int;
    else if (is_float(s))     return p_item::p_double;
    else if (is_date(s))      return p_item::p_date;
    else if (is_dtime(s))     return p_item::p_ptime;
    return p_item::p_dcode;
}



node::id_t graph_db::import_node(const std::string &label,
                                 const properties_t &props) {
  auto type_code = dict_->insert(label);
  auto node_id = nodes_->append(node(type_code), 0);

  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        node_properties_->append_properties(node_id, props, dict_);
    n.property_list = pid;
  }

  return node_id;
}

node::id_t graph_db::import_typed_node(dcode_t label, 
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist, 
                              const std::vector<std::any>& values) {
  auto node_id = nodes_->append(node(label), 0);
                            
  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

  // save properties
  if (!keys.empty()) {
    property_set::id_t pid =
        node_properties_->append_typed_properties(node_id, keys, typelist, values);
    n.property_list = pid;
  }

  return node_id;
}

node::id_t graph_db::import_typed_node(dcode_t label,
                              const std::vector<dcode_t> &keys,
                              const std::vector<p_item::p_typecode>& typelist,
				const std::vector<std::string>& values,dict_ptr &dict) {
  auto node_id = nodes_->append(node(label), 0);

  // we need the node object not only the id
  auto &n = nodes_->get(node_id);

  // save properties
  if (!keys.empty()) {
    property_set::id_t pid =
        node_properties_->append_typed_properties(node_id, keys, typelist, values, dict);
    n.property_list = pid;
  }

  return node_id;
}
relationship::id_t graph_db::import_relationship(node::id_t from_id,
                                                 node::id_t to_id,
                                                 const std::string &label,
                                                 const properties_t &props) {
  auto &from_node = nodes_->get(from_id);
  auto &to_node = nodes_->get(to_id);
  auto type_code = dict_->insert(label);
  auto rid = rships_->append(relationship(type_code, from_id, to_id), 0);

  auto &r = rships_->get(rid);

  // save properties
  if (!props.empty()) {
    property_set::id_t pid =
        rship_properties_->append_properties(rid, props, dict_);
    r.property_list = pid;
  }
  // update the list of relationships for each of both nodes
  if (from_node.from_rship_list == UNKNOWN)
    from_node.from_rship_list = rid;
  else {
    r.next_src_rship = from_node.from_rship_list;
    from_node.from_rship_list = rid;
  }

  if (to_node.to_rship_list == UNKNOWN)
    to_node.to_rship_list = rid;
  else {
    r.next_dest_rship = to_node.to_rship_list;
    to_node.to_rship_list = rid;
  }
  return rid;
}

relationship::id_t graph_db::import_typed_relationship(node::id_t from_id,
                                         node::id_t to_id,
                                         dcode_t label, 
                                         const std::vector<dcode_t> &keys,
                                         const std::vector<p_item::p_typecode>& typelist, 
                                         const std::vector<std::any>& values) {
  auto &from_node = nodes_->get(from_id);
  auto &to_node = nodes_->get(to_id);
  auto rid = rships_->append(relationship(label, from_id, to_id), 0);

  auto &r = rships_->get(rid);

  // save properties
  if (!keys.empty()) {
    property_set::id_t pid =
        rship_properties_->append_typed_properties(rid, keys, typelist, values);
    r.property_list = pid;
  }

  // update the list of relationships for each of both nodes
  if (from_node.from_rship_list == UNKNOWN)
    from_node.from_rship_list = rid;
  else {
    r.next_src_rship = from_node.from_rship_list;
    from_node.from_rship_list = rid;
  }

  if (to_node.to_rship_list == UNKNOWN)
    to_node.to_rship_list = rid;
  else {
    r.next_dest_rship = to_node.to_rship_list;
    to_node.to_rship_list = rid;
  }
  return rid;

}

relationship::id_t graph_db::import_typed_relationship(node::id_t from_id,
                                         node::id_t to_id,
                                         dcode_t label,
                                         const std::vector<dcode_t> &keys,
                                         const std::vector<p_item::p_typecode>& typelist,
					  const std::vector<std::string>& values,dict_ptr &dict) {
  // std::cout << "<" << std::flush;
  auto &from_node = nodes_->get(from_id);
  auto &to_node = nodes_->get(to_id);
  // std::cout << "x" << std::flush;
  auto rid = rships_->append(relationship(label, from_id, to_id), 0);

  auto &r = rships_->get(rid);

  // save properties
  if (!keys.empty()) {
    // std::cout << "o" << std::flush;
    property_set::id_t pid =
        rship_properties_->append_typed_properties(rid, keys, typelist, values, dict);
    r.property_list = pid;
  }

  // update the list of relationships for each of both nodes
  if (from_node.from_rship_list == UNKNOWN)
    from_node.from_rship_list = rid;
  else {
    r.next_src_rship = from_node.from_rship_list;
    from_node.from_rship_list = rid;
  }

  if (to_node.to_rship_list == UNKNOWN)
    to_node.to_rship_list = rid;
  else {
    r.next_dest_rship = to_node.to_rship_list;
    to_node.to_rship_list = rid;
  }
  // std::cout << ">" << std::flush;

  return rid;

}


std::size_t graph_db::import_nodes_from_csv(const std::string &label,
                                            const std::string &filename,
                                            char delim, mapping_t &m, std::mutex *mtx) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open())
    return 0;
    // throw file_not_found(filename);

  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns; // name of all fields
  int id_column = -1;               // field no of :ID

  for (auto &row : parser) {
    if (num == 0) {
      // process the header
      std::size_t i = 0;
      for (auto &field : row) {
        //auto pos = field.find(":ID"); // neo4j

        if (auto pos = field.find("id",0); pos != std::string::npos) {
          // <name>:ID is a special field // neo4j
          id_column = i;
          //columns.push_back(field.substr(0, pos)); // neo4j
           columns.push_back(field);
        } else
          columns.push_back(field);
        i++;
      }
      assert(id_column >= 0);
      num++;
    } else {
      properties_t props;
      auto i = 0;
      std::string id_label;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        auto &col = columns[i++];
        //if (!col.empty() && !field.empty()) {
        if (!col.empty() && !(field.empty() && col != "content")) {
          if (col == "id")
            props.insert({col, (uint64_t)std::stoll(field)});
          else
            props.insert({col, field});
        }
      }
      try {
        auto id = import_node(label, props);
        auto id_label_s = id_label + "_" + label;
        m.insert({id_label_s, id});
        num++;
      } catch (...) {
        spdlog::warn("importing node '{}' failed.", label);
      }
      // std::cout << "mapping: " << id_label << " -> " << id << std::endl;
    }

  }
  return num > 0 ? num-1 : 0;
}

std::size_t graph_db::import_typed_nodes_from_csv(const std::string &label,
                                            const std::string &filename,
                                            char delim, mapping_t &m, std::mutex *mtx) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open()) {
    spdlog::warn("cannot find node file '{}'", filename);
    return 0;
  }
    // throw file_not_found(filename);

  std::string id_label;
  auto label_code = dict_->insert(label);
  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns; // names of all fields
  int id_column = -1;               // field no of :ID

  std::vector<dcode_t> prop_names;
  std::vector<p_item::p_typecode> prop_types; 
  std::vector<bool> inferred;

  for (auto &row : parser) {
    if (num == 0) {
      /*
       * process the header
       */
      auto i = 0;
      for (auto &field : row) {
        //auto pos = field.find(":ID"); // neo4j
        // auto pos = field.find("id");
        // if (pos != std::string::npos) {
        if (field == "id") {
          // <name>:ID is a special field // neo4j
          id_column = i;
          //columns.push_back(field.substr(0, pos)); // neo4j
           columns.push_back(field);
        } else
          columns.push_back(field);
        i++;
      }
      assert(id_column >= 0);
      prop_names.resize(columns.size());
      prop_types.resize(columns.size());
      inferred.resize(columns.size(), false);
      for (auto j = 0u; j < columns.size(); j++) {
        prop_names[j] = dict_->insert(columns[j]);
      }
    } else if (num == 1) {
      /*
       * process the first row: infer the data types
       * Not all data types are inferred from first row
       */
      auto i = 0;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        // spdlog::info("record #{}: field #{} = '{}'", num-1, i, field);
        if (const auto& col {columns[i]}; !col.empty() && !field.empty()) {
          if (col == "id" || col == "phone") {
            prop_types[i] = p_item::p_uint64;     
          }
          else {   
              prop_types[i] = get_datatype(field);
          }
          inferred[i] = true;
        }  
        else {
          // the field is empty, let's assume a string value
           // spdlog::info("empty field #{} at record #{}", i, num-1);
            prop_types[i] = p_item::p_dcode;
        }     
        i++;
      }
      if (mtx != nullptr) 
        mtx->lock();
     auto id = import_typed_node(label_code, prop_names, prop_types, row, dict_);
      // fill mapping table
      auto id_label_s = id_label + "_" + label;
      m.insert({id_label_s, id});
      if (mtx != nullptr) 
        mtx->unlock();
    } else {
      std::string id_label;
      auto i = 0;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        // spdlog::info("record #{}: field #{} = '{}'", num-1, i, field);
        if (const auto& col {columns[i]}; !col.empty() && !field.empty()) {
          if (!inferred[i]){ // columns whose datatypes we have not yet inferred
        	  prop_types[i] = get_datatype(field);
            inferred[i] = true;
          }
        }   
         // spdlog::info("\t==> empty field #{} at record #{}", i, num-1);
     
        i++;
      }


      if (mtx != nullptr) 
        mtx->lock();
      // spdlog::info("import line #{}: ncolumns = {}", i, prop_values.size());
      auto id = import_typed_node(label_code, prop_names, prop_types, row, dict_);
      auto id_label_s = id_label + "_" + label;
      m.insert({id_label_s, id});
      if (mtx != nullptr) 
        mtx->unlock();

      // spdlog::info("node_id = {} ({})", id, id_label_s);
    }
  num++;
  }
  return num-1;
}

std::size_t graph_db::import_typed_n4j_nodes_from_csv(const std::string &label,
                                            const std::string &filename,
                                            char delim, mapping_t &m) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open()) {
    spdlog::warn("cannot find node file '{}'", filename);
    return 0;
  }
    // throw file_not_found(filename);

  std::string id_label;
  auto label_code = dict_->insert(label);
  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns; // names of all fields
  int id_column = -1;               // field no of :ID

  std::vector<dcode_t> prop_names;
  std::vector<p_item::p_typecode> prop_types; 
  std::vector<std::any> prop_values;
  std::vector<bool> inferred;

  for (auto &row : parser) {
    if (num == 0) {
      /*
       * process the header
       */
      auto i = 0;
      for (auto &field : row) {
        auto pos = field.find(":ID"); // neo4j
        if (pos != std::string::npos) {
          // <name>:ID is a special field // neo4j
          id_column = i;
          columns.push_back(field.substr(0, pos)); // neo4j
        } else
          columns.push_back(field);
        i++;
      }
      assert(id_column >= 0);
      prop_names.resize(columns.size());
      prop_types.resize(columns.size());
      prop_values.resize(columns.size());
      inferred.resize(columns.size(), false);
      for (auto j = 0u; j < columns.size(); j++) {
        prop_names[j] = dict_->insert(columns[j]);
      }
    } else if (num == 1) {
      /*
       * process the first row: infer the data types
       * Not all data types are inferred from first row
       */
      auto i = 0;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        // spdlog::info("record #{}: field #{} = '{}'", num-1, i, field);

        auto &col = columns[i];
        if (!col.empty() && !field.empty()) {
          if (col == "id") {
            prop_types[i] = p_item::p_uint64;
            prop_values[i] = std::any((uint64_t)std::stoll(field));
          }
          else {
            auto p2 = infer_datatype(field, dict_);
            prop_types[i] = p2.first;
            prop_values[i] = p2.second;
            inferred[i] = true;
          }
        }  
        else {
          // the field is empty, let's assume a string value
           // spdlog::info("empty field #{} at record #{}", i, num-1);
          prop_types[i] = p_item::p_dcode;
        }     
        i++;
      }
      auto id = import_typed_node(label_code, prop_names, prop_types, prop_values);
      // fill mapping table
      m.insert({id_label, id});
    } else {
      auto i = 0;
      std::string id_label;
      for (auto &field : row) {
        if (i == id_column)
          id_label = field;

        // spdlog::info("record #{}: field #{} = '{}'", num-1, i, field);
        auto &col = columns[i];
        if (!col.empty() && !field.empty()) {
          if (col == "id")
            prop_values[i] = std::any((uint64_t)std::stoll(field));
          else if (inferred[i])
            prop_values[i] = string_to_any(prop_types[i], field, dict_);
          else { // columns whose datatypes we have not yet inferred
            auto p2 = infer_datatype(field, dict_);
            prop_types[i] = p2.first;
            prop_values[i] = p2.second;
            inferred[i] = true;
          }
        }
        else {
           // spdlog::info("\t==> empty field #{} at record #{}", i, num-1);
           prop_values[i] = std::any();
        }
        i++;
      }
      // spdlog::info("import line #{}: ncolumns = {}", i, prop_values.size());
      auto id = import_typed_node(label_code, prop_names, prop_types, prop_values);
      m.insert({id_label, id});
    }
  num++;
  }
  return num-1;
}

graph_db::mapping_t::const_iterator node_id_from_field(const graph_db::mapping_t &m, 
   const std::string& str, const std::string &field) {
     std::string s(str);
     if (s[0] >= 'a' && s[0] <= 'z')
        s[0] -= 32;
      auto id_s = field + "_" + s;
      return m.find(id_s);
 }

std::size_t graph_db::import_relationships_from_csv(const std::string &filename,
                                                    char delim,
                                                    const mapping_t &m, std::mutex *mtx) {
  using namespace aria::csv;
  std::ifstream f(filename);
  if (!f.is_open())
    return 0;
    // throw file_not_found(filename);

  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> fp;
  boost::split(fp, filename, boost::is_any_of("/"));
  assert(fp.back().find(".csv", filename.size()-4) != std::string::npos);
  std::vector<std::string> fn;
  boost::split(fn, fp.back(), boost::is_any_of("_"));
  auto label = /*":" + */ fn[1];
  auto src_node = fn[0];
  auto des_node = fn[2];

  std::vector<std::string> columns;
  //int start_col = -1, end_col = -1, type_col = -1; // neo4j
  int start_col = 0, end_col = 1;

  for (auto &row : parser) {
    if (num == 0) {
      // process header
      for (auto &field : row) {
        columns.push_back(field);
        /*if (field == ":START_ID") // neo4j
          start_col = i;
        else if (field == ":END_ID")
          end_col = i;
        else if (field == ":TYPE")
          type_col = i;*/
      }
    } else {     
      mapping_t::const_iterator it = node_id_from_field(m, src_node, row[start_col]);
      if (it == m.end()) {
        spdlog::info("mapping not found for node id #{}-{}", src_node, row[start_col]);
        continue;
      }
      node::id_t from_node = it->second;
      
      it = node_id_from_field(m, des_node, row[end_col]);
      if (it == m.end()) {
        spdlog::info("mapping not found for node id #{}-{}", des_node, row[end_col]);
        continue;
      }
      node::id_t to_node = it->second;      
      //auto &label = row[type_col]; // neo4j

      properties_t props;
      auto i = 0;
      for (auto &field : row) {
        //if (i != start_col && i != end_col && i != type_col) {  // neo4j
        if (i != start_col && i != end_col) {
          auto &col = columns[i];
          if (!field.empty())
            props.insert({col, field});
        }
        i++;
      }
      import_relationship(from_node, to_node, label, props);
      // std::cout << "add rship: " << row[start_col] << "(" << from_node <<
      // ")->"
      //    << row[end_col] << "(" << to_node << ")" << std::endl;
    }
    num++;
  }

  return num-1;
}

std::size_t graph_db::import_typed_relationships_from_csv(const std::string &filename,
                                                    char delim,
                                                    const mapping_t &m, std::mutex *mtx) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open()) {
    spdlog::warn("cannot find relationship file '{}'", filename);
    return 0;
    // throw file_not_found(filename);
  }
  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> fp;
  boost::split(fp, filename, boost::is_any_of("/"));
  // spdlog::info("fp = {} : {}", fp.back(), fp.size());
  // assert(fp.back().find(".csv", fp.size()-4) != std::string::npos);
  std::vector<std::string> fn;
  boost::split(fn, fp.back(), boost::is_any_of("_"));
  auto label = /*":" + */fn[1];
  auto label_code = dict_->insert(label);
  auto src_node = fn[0];
  auto des_node = fn[2];

  std::vector<std::string> columns;
  int start_col = 0, end_col = 1;

  std::vector<dcode_t> prop_names;
  std::vector<p_item::p_typecode> prop_types; 
  std::vector<std::string> prop_values;

  for (auto &row : parser) {
    if (num == 0) {
      auto i = 0;
      // process header
      for (auto &field : row) {
        // ignore the src und dest id fields
        if (i != start_col && i != end_col)
          columns.push_back(field);
        i++;
      }
      prop_names.resize(columns.size());
      prop_types.resize(columns.size());
      prop_values.resize(columns.size());
      for (auto j = 0u; j < columns.size(); j++) {
        prop_names[j] = dict_->insert(columns[j]);
      }
      num++;
    } 
    else if (num == 1) {
      mapping_t::const_iterator it = node_id_from_field(m, src_node, row[start_col]);
      if (it == m.end())
        continue;
      node::id_t from_node = it->second;      

      it = node_id_from_field(m, des_node, row[end_col]);
      if (it == m.end())
        continue;
      node::id_t to_node = it->second;      

      auto i = 0, j = 0;
      for (auto &field : row) {
        if (i != start_col && i != end_col) {
          if (!field.empty()) {
        	  prop_types[j] = get_datatype(field);
        	  prop_values[j] = field;
            j++;
          }
        }
        i++;
      }
      if (mtx != nullptr)
        mtx->lock();
      try {
        import_typed_relationship(from_node, to_node, label_code, prop_names, 
    		    prop_types, prop_values, dict_);
        num++;
      } catch (...) {
        spdlog::warn("importing relationship '{}' {} -> {} failed", label, from_node, to_node);
      }
      if (mtx != nullptr)
        mtx->unlock();
    } 
    else {
     //std::cout << num << " " << std::flush;
      mapping_t::const_iterator it = node_id_from_field(m, src_node, row[start_col]);
      if (it == m.end())
        continue;
      node::id_t from_node = it->second;      

     //std::cout << "#" << std::flush;

      it = node_id_from_field(m, des_node, row[end_col]);
      if (it == m.end())
        continue;
      node::id_t to_node = it->second;      

     //std::cout << "@" << from_node << "|" << to_node << std::flush;

      if (mtx != nullptr)
        mtx->lock();
      try {
        import_typed_relationship(from_node, to_node, label_code, prop_names, 
                                prop_types, prop_values, dict_);
        num++;
      } catch (std::exception& exc) {
        spdlog::warn("importing relationship '{}' {} -> {} failed", label, from_node, to_node);
      }
      if (mtx != nullptr)
        mtx->unlock();
    }
    // std::cout << num << " " << std::flush;
    // std::cout << "$" << std::flush;
  }
  return num > 0 ? num-1 : 0;
}

std::size_t graph_db::import_typed_n4j_relationships_from_csv(const std::string &filename,
                                                    char delim,
                                                    const mapping_t &m, const std::string& rship_type) {
  using namespace aria::csv;

  std::ifstream f(filename);
  if (!f.is_open()) {
    spdlog::warn("cannot find relationship file '{}'", filename);
    return 0;
    // throw file_not_found(filename);
  }
  CsvParser parser = CsvParser(f).delimiter(delim);
  std::size_t num = 0;

  std::vector<std::string> columns;
  int start_col = -1, end_col = -1, type_col = -1; // neo4j

  std::vector<dcode_t> prop_names;
  std::vector<p_item::p_typecode> prop_types; 
  std::vector<std::any> prop_values;

  for (auto &row : parser) {
    if (num == 0) {
      auto i = 0;
      // process header
      for (auto &field : row) {
        columns.push_back(field);
        if (field.rfind(":START_ID", 0) == 0) // neo4j
          start_col = i;
        else if (field.rfind(":END_ID", 0) == 0)
          end_col = i;
        else if (field == ":TYPE")
          type_col = i;
        i++;
      }
      assert(start_col >= 0); // neo4j
      assert(end_col >= 0);
      if (rship_type.empty())
        assert(type_col >= 0);

      prop_names.resize(columns.size());
      prop_types.resize(columns.size());
      prop_values.resize(columns.size());
      for (auto j = 0u; j < columns.size(); j++) {
        prop_names[j] = dict_->insert(columns[j]);
      }
    } else if (num == 1) {
      mapping_t::const_iterator it = m.find(row[start_col]); 
      if (it == m.end()) {
        std::cout << "Cannot find " << row[start_col] << " in num #1" << std::endl;
        continue;
      }
      node::id_t from_node = it->second;      

      // if the type (label) of the relationship was given, we just use it 
      auto &label = rship_type.empty() ? row[type_col] : rship_type;
      auto label_code = dict_->insert(label);

      it = m.find(row[end_col]); 
      if (it == m.end())
        continue;
      node::id_t to_node = it->second;      

      auto i = 0, j = 0;
      for (auto &field : row) {
        if (i != start_col && i != end_col && i != type_col) {
          if (!field.empty()) {
            auto p2 = infer_datatype(field, dict_);
            prop_types[j] = p2.first;
            prop_values[j] = p2.second;
            j++;
          }
        }
        i++;
      }
      import_typed_relationship(from_node, to_node, label_code, prop_names, 
                                prop_types, prop_values);
    } else {
      mapping_t::const_iterator it = m.find(row[start_col]); // node_id_from_field(m, src_node, row[start_col]);
      if (it == m.end())
        continue;
      node::id_t from_node = it->second;      

      auto &label = rship_type.empty() ? row[type_col] : rship_type;
      auto label_code = dict_->insert(label);

      it = m.find(row[end_col]); // node_id_from_field(m, des_node, row[end_col]);
      if (it == m.end())
        continue;
      node::id_t to_node = it->second;      

      auto i = 0, j = 0;
      for (auto &field : row) {
        if (i != start_col && i != end_col && i != type_col) {  // neo4j
          if (!field.empty())
            prop_values[j] = string_to_any(prop_types[j], field, dict_);
          j++;
        }
        i++;
      }
      import_typed_relationship(from_node, to_node, label_code, prop_names, 
                                prop_types, prop_values);
    }
    num++;
  }

  return num-1;
}
