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

#include <chrono>
#include <iostream>
#include <filesystem>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/program_options.hpp>

#include "linenoise.hpp"
#include "fmt/chrono.h"
#include "query_proc.hpp"
#include "graph_db.hpp"
#include "graph_pool.hpp"

#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"


enum cmd_mode {
  undefined_mode,
  import_mode,
  script_mode,
  shell_mode
};

using namespace boost::program_options;

graph_pool_ptr pool;
graph_db_ptr graph;
std::unique_ptr<query_proc> qproc_ptr;

/**
 * Import data from the given list of CSV files. The list contains
 * not only the files names but also nodes/relationships as well as
 * the labels.
 */
bool import_csv_files(graph_db_ptr &gdb, std::string import_path, const std::vector<std::string> &files,
                      char delimiter, std::string format, bool strict) {
  graph_db::mapping_t id_mapping;

  for (auto s : files) {
    if (s.find("nodes:") != std::string::npos) {
      std::vector<std::string> result;
      boost::split(result, s, boost::is_any_of(":"));

      if (result.size() != 3) {
        std::cerr << "ERROR: unknown import option for nodes." << std::endl;
        return false;
      }

      std::size_t num = 0;
      auto file_name = import_path;
      if (!file_name.empty()) file_name += "/";
      file_name += result[2];

      auto start = std::chrono::steady_clock::now();
      if (format == "n4j") {
        num = gdb->import_typed_n4j_nodes_from_csv(result[1], file_name,
                                                   delimiter, id_mapping);
      }
      else {
        num = strict
          ? gdb->import_typed_nodes_from_csv(result[1], file_name, delimiter, id_mapping)
          : gdb->import_nodes_from_csv(result[1], file_name, delimiter, id_mapping);
      }
      auto end = std::chrono::steady_clock::now();

      auto time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      spdlog::info("{} '{}' nodes imported in {} msecs ({} items/s)", num, result[1], time, (int)((double)num/time * 1000.0));
    }
    else if (s.find("relationships:") != std::string::npos) {
      std::vector<std::string> result;
      boost::split(result, s, boost::is_any_of(":"));

      if (format == "n4j") {
        if (result.size() < 2 || result.size() > 3) {
          std::cerr << "ERROR: unknown import option for relationships."
                    << std::endl;
          return false;
        }
      }
      else if (result.size() != 3) {
          std::cerr << "ERROR: unknown import option for relationships."
                    << std::endl;
          return false;
      }

      std::size_t num = 0;
      auto file_name = import_path;
      if (!file_name.empty()) file_name += "/";

      auto start = std::chrono::steady_clock::now();
      if (format == "n4j") {
        file_name += result.back();
        auto rship_type = result.size() == 3 ? result[1] : "";
        num = gdb->import_typed_n4j_relationships_from_csv(file_name, delimiter, id_mapping, rship_type);
      }
      else {
        file_name += result[2];
        num = strict
         ? gdb->import_typed_relationships_from_csv(file_name, delimiter, id_mapping)
         : gdb->import_relationships_from_csv(file_name, delimiter, id_mapping);
      }
      auto end = std::chrono::steady_clock::now();

      auto time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (result.size() == 3) 
        spdlog::info("{} '{}' relationships imported in {} msecs ({} items/s)", num, result[1], time, (int)((double)num/time * 1000.0));
      else
        spdlog::info("{} relationships imported in {} msecs ({} items/s)", num, time, (int)((double)num/time * 1000.0));
    }
    else {
      std::cerr << "ERROR: unknown import (nodes or relationships expected)."
                << std::endl;
      return false;
    }
  }
  gdb->flush();
  return true;
}


/**
 * Remove leading and trailing whitespaces from the given string.
 */
static void trim(std::string &s) {
  s.erase(s.begin(), std::find_if_not(s.begin(), s.end(),
                                      [](char c) { return std::isspace(c); }));
  s.erase(std::find_if_not(s.rbegin(), s.rend(),
                           [](char c) { return std::isspace(c); })
              .base(),
          s.end());
}

/**
 * Print the query result to standard output.
 */
void print_result(qresult_iterator& qres) {
  std::cout << "Result: \n" << qres.result() << std::dec << qres.result_size() << " tuple(s)" << std::endl;
}

/**
 * Execute the query given as string. If qex_cc is set to true then the
 * query is compiled using LLVM, otherwise the query interpreter is used.
 */
void exec_query(const std::string &qstr, query_proc::mode qmode, bool print_plan) {
  try {
    auto start_qp = std::chrono::steady_clock::now();
    qproc_ptr->execute_and_output_query(qmode, qstr, print_plan);
    auto end_qp = std::chrono::steady_clock::now();

    std::chrono::duration<double> diff = end_qp - start_qp;
    fmt::print("Query executed in {}\n", diff); 
  } catch (std::exception& exc) {
    std::cerr << "Error in query execution: " << exc.what() << std::endl;
    qproc_ptr->abort_transaction();
  }
}


std::string read_from_file(const std::string& qfile) {
  std::string qstr, line;

  std::ifstream myfile(qfile);
  if (myfile.is_open()) {
    while (getline(myfile, line))
      qstr.append(line);
    myfile.close();
  } else {
    std::cerr << "File not found" << std::endl;
  }
  
  // spdlog::info("execute query {}", qstr);
  return qstr;
}

/**
 * Linenoise - autocompletion
 */
void query_completion(const char* buf, std::vector<std::string>& completions) {
    if (buf[0] == 'n' || buf[0] == 'N') {
        completions.push_back("NodeScan(");
    } else if (buf[0] == 'l' || buf[0] == 'L') {
        completions.push_back("Limit(");
    } else if (buf[0] == 's' || buf[0] == 'S') {
        completions.push_back("set");
    }
}

void print_stats(graph_db_ptr &gdb) {
  gdb->print_stats();
}

void sync_db(graph_db_ptr &gdb) {
  gdb->flush();
}

void print_object(graph_db_ptr &gdb, const std::string &cmd) {
  if (cmd.starts_with("node") && cmd.length() > 5) {
    auto s = cmd.substr(5);
    auto id = std::stoul(s);

    gdb->run_transaction([&]() {
      auto& n = gdb->node_by_id(id);
      std::cout << std::dec << "#" << n.id() 
                << ", label=" << gdb->get_dictionary()->lookup_code(n.node_label) << ", from="
                << uint64_to_string(n.from_rship_list) << ", to=" << uint64_to_string(n.to_rship_list) 
                << ", props=" << uint64_to_string(n.property_list) << std::endl;
      return true;
    });
  }
  else if (cmd.starts_with("rship")) {
    auto s = cmd.substr(6);
    auto id = std::stoul(s);
    gdb->run_transaction([&]() {
      auto& r = gdb->rship_by_id(id);
      std::cout << "#" << r.id() 
              << ", label = " << gdb->get_dictionary()->lookup_code(r.rship_label) << ", " << r.src_node
              << "->" << r.dest_node << ", next_src=" << uint64_to_string(r.next_src_rship) << ", next_dest="
              << uint64_to_string(r.next_dest_rship) << std::endl;
      return true;
    });
  }
  else
    std::cerr << "ERROR: invalid print command" << std::endl;
}

void show_help() {
  std::cout << "Available commands:\n"
            << "\thelp                             " << "show this help" << "\n"
            << "\tstring s                         " << "display the dictionary code of the string s" << "\n"
            << "\tcode c                           " << "display the string of the dictionary code c" << "\n"
            << "\tload <library>                   " << "load the given shared library" << "\n"
            << "\tstats                            " << "print database statistics" << "\n"
            << "\tsync                             " << "ensure that all pages are written to disk" << "\n"
            << "\tcreate index <label> <property>  " << "create an index for the given label/property" << "\n"
            << "\tdrop index <label> <property>    " << "delete the index for the given label/property" << "\n"
            << "\t@file                            " << "execute the query stored in the given file" << "\n"
            << "\texplain <query-expr>             " << "execute the given query and print the plan" << "\n"
            << "\t<query-expr>                     " << "execute the given query" << "\n"
            << "\tprint node|rship <id>            " << "print the raw data of the node/relationship with given id" << std::endl;
}

/**
 * Run an interactive shell for entering and executing queries.
 */
void run_shell(graph_db_ptr &gdb, query_proc::mode qmode) {
  const auto path = "history.txt";
  // Enable the multi-line mode
  linenoise::SetMultiLine(true);

  // Set max length of the history
  linenoise::SetHistoryMaxLen(1000);
  // Load history
  linenoise::LoadHistory(path);
  linenoise::SetCompletionCallback(query_completion);

  while (true) {
    std::string line;
    
    auto quit = linenoise::Readline("poseidon> ", line);

    if (quit) {
      std::cout << "Bye!" << std::endl;
      break;
    }

    trim(line);
    if (line.length() == 0)
      continue;

    if (line.rfind("@", 0) == 0) {
      auto query_string = read_from_file(line.substr(1));
      exec_query(query_string, qmode, false);
    } 
#if USE_LLVM    
    else if(line.rfind("set", 0) == 0) { // save sub-query: > q1:End(NodeScan("Person"))
      spdlog::info("Save query: {} as {}", line.substr(line.find(":") + 1), line.substr(0, line.find(":")).substr(4));
      // TODO: qlc.parse_and_save_plan(line.substr(0, line.find(":")).substr(4), line.substr(line.find(":") + 1));
    } else if(line.rfind("run", 0) == 0) { // run saved query plan -> run:q1
      spdlog::info("Execute query: {} ", line.substr(line.find(":") + 1));
      //qlc.exec_plan(line.substr(line.find(":") + 1), gdb);
      exec_query(line.substr(line.find(":") + 1), qmode, false);
    }
#endif
    else if (line.rfind("help", 0) == 0) {
      show_help();
    }
    else if (line.rfind("stats", 0) == 0) {
      print_stats(gdb);
    }
    else if (line.rfind("print", 0) == 0) {
      if (line.length() > 6) {
        auto s = line.substr(6);
        trim(s);
        print_object(gdb, s);
      }
    }
    else if (line.rfind("load", 0) == 0) {
      if (line.length() > 4) {
        auto lib_path = line.substr(4);
        trim(lib_path);
        spdlog::info("trying to load shared library '{}'", lib_path);
        try {
          if (qproc_ptr->load_library(lib_path)) {
            std::cout << "library '" << lib_path << "' loaded successfully." << std::endl;
          }
          else 
            std::cerr << "ERROR: cannot load library '" << lib_path << "'" << std::endl;
        } catch (std::exception& exc) {
            std::cerr << "ERROR: cannot load library '" << lib_path << "'" << std::endl;
        }
      }
    }
    else if (line.rfind("sync", 0) == 0) {
      sync_db(gdb);
    }    
    else if (line.rfind("string", 0) == 0) {
      // lookup_string
      if (line.length() > 6) {
        auto s = line.substr(6);
        trim(s);
        std::cout << "dict code for '" << s << "': " << gdb->get_dictionary()->lookup_string(s) << std::endl;
      }
    }
    else if (line.rfind("code", 0) == 0) {
      // lookup_code
      if (line.length() > 4) {
        auto s = line.substr(4);
        trim(s);
        std::cout << "dict string for '" << s << "': " << gdb->get_dictionary()->lookup_code(std::stoi(s)) << std::endl;
      }
    }
    else if (line.rfind("create index", 0) == 0) {
      if (line.length() > 12) {
        auto str = line.substr(12);
        trim(str);        
        std::vector<std::string> s;
        boost::split(s, str, boost::is_any_of(" "));
        if (s.size() == 2) {
          spdlog::info("create index {}-{}", s[0], s[1]);
          query_ctx ctx(gdb);
          ctx.run_transaction([&]() {
            if (!ctx.gdb_->has_index(s[0], s[1]))
              ctx.gdb_->create_index(s[0], s[1]);
            return true;
          });
	  gdb->flush();
        }
        else
          std::cerr << "ERROR: invalid command" << std::endl;
      }

    }
    else if (line.rfind("drop index", 0) == 0) {
      if (line.length() > 10) {
        auto str = line.substr(10);
        trim(str);        
        std::vector<std::string> s;
        boost::split(s, str, boost::is_any_of(" "));
        if (s.size() == 2) {
          spdlog::info("drop index {}-{}", s[0], s[1]);
          query_ctx ctx(gdb);
          ctx.run_transaction([&]() {
            if (ctx.gdb_->has_index(s[0], s[1]))
              ctx.gdb_->drop_index(s[0], s[1]);
            return true;
          });
        }
        else
          std::cout << "ERROR: invalid command" << std::endl;
      }      
    }
    else if (line.rfind("explain ", 0) == 0) {
      auto qstr = line.substr(8);
      exec_query(qstr, qmode, true);
    }
    else
      exec_query(line, qmode, false);

    // Add line to history
    linenoise::AddHistory(line.c_str());

    // Save history
    linenoise::SaveHistory(path);
  }
  gdb->flush();
  gdb->close_files();
}

std::string check_config_files(const std::string& fname) {
  std::filesystem::path cwd_config_file(fname);
  if (std::filesystem::exists(cwd_config_file))
    return cwd_config_file.string();
  
  std::string full_name = getenv("HOME") + std::string("/") + fname;
  std::filesystem::path home_config_file(full_name);
  if (std::filesystem::exists(home_config_file))
    return home_config_file.string();

  return "";
}

int main(int argc, char* argv[]) {
  std::string db_name, pool_path, query_file, import_path, dot_file, qmode_str, format = "ldbc";
  std::size_t bp_size = 0;
  std::vector<std::string> import_files;
  bool start_shell = false;
  query_proc::mode qmode = query_proc::Interpret; 
  char delim_character = ',';
  bool strict = false;
  cmd_mode mode = undefined_mode;

  auto console = spdlog::stdout_color_mt("poseidon");
  spdlog::set_default_logger(console);
  spdlog::info("Starting poseidon cli, Version {}", POSEIDON_VERSION);

  try {
    options_description desc{"Options"};
    desc.add_options()
      ("help,h", "Help")
        ("verbose,v", bool_switch()->default_value(false), "Verbose - show debug output")
        ("db,d", value<std::string>(&db_name)->required(), "Database name (required)")
        ("pool,p", value<std::string>(&pool_path)->required(), "Path to the PMem/file pool")
        ("buffersize,b", value<std::size_t>(&bp_size), "Size of the bufferpool (in pages)")
        ("output,o", value<std::string>(&dot_file), "Dump the graph to the given file (in DOT format)")
        ("strict", bool_switch()->default_value(true), "Strict mode - assumes that all columns contain values of the same type")
        ("delimiter", value<char>(&delim_character)->default_value('|'), "Character delimiter")
        ("format,f", value<std::string>(&format), "CSV format: n4j | gtpc | ldbc")
        ("import-path", value<std::string>(&import_path), "Directory containing import files")
        ("import", value<std::vector<std::string>>()->composing(),
        "Import files in CSV format (either nodes:<node type>:<filename> or "
        "relationships:<rship type>:<filename>")
        ("query,q", value<std::string>(&query_file), "Execute the query from the given file")
        ("shell,s", bool_switch()->default_value(false), "Start the interactive shell")
        ("qmode", value<std::string>(&qmode_str), "Query compile mode: llvm | interp (default) | adapt");

    variables_map vm;
    store(parse_command_line(argc, argv, desc), vm);
    auto config_name = check_config_files("poseidon.ini");
    // std::filesystem::path config_file(getenv("HOME") + std::string("/poseidon.ini"));
    if (! config_name.empty()) {
      spdlog::info("loading config from '{}'", config_name);
      std::ifstream ifs(config_name);
      store(parse_config_file(ifs, desc), vm);
    }

    if (vm.count("help")) {
      std::cout << "Poseidon Graph Database Version " << POSEIDON_VERSION << "\n" << desc << '\n';
      return -1;
    }

    notify(vm);

    if (vm.count("import_path"))
      import_path = vm["import_path"].as<std::string>();

    if (vm.count("import")) {
      import_files = vm["import"].as<std::vector<std::string>>();
      mode = import_mode;
    }
    if (vm.count("pool"))
      pool_path = vm["pool"].as<std::string>();

    if (vm.count("buffersize"))
      bp_size = vm["buffersize"].as<std::size_t>();

    if (vm.count("delimiter"))
      delim_character = vm["delimiter"].as<char>();

   if (vm.count("strict"))
      strict = vm["strict"].as<bool>();

    if (vm.count("format"))
      format = vm["format"].as<std::string>();

    if (format != "n4j" && format != "gtpc" && format != "ldbc") {
      std::cerr
          << "ERROR: choose format --n4j or --gtpc or --ldbc.\n";
      return -1;
    }

    if (vm.count("verbose"))
      if (vm["verbose"].as<bool>())
        spdlog::set_level(spdlog::level::debug);

    if (vm.count("shell"))
      start_shell = vm["shell"].as<bool>();

    if (vm.count("qmode")) {
      if (qmode_str != "llvm" && qmode_str != "interp" && qmode_str != "adapt") {
        std::cout << "ERROR: unknown query mode value: 'llvm' or 'interp' or 'adapt' expected.\n";
        return -1;
      }
      if (qmode_str == "llvm")
        qmode = query_proc::Compile;
      else if (qmode_str == "interp")
        qmode = query_proc::Interpret;
      else
        qmode = query_proc::Adaptive;
    }

    if (start_shell && !query_file.empty()) {
      std::cerr
          << "ERROR: options --shell and --query cannot be used together.\n";
      return -1;
    }
  } catch (const error &ex) {
    std::cerr << ex.what() << '\n';
    return -1;
  }

  if (access(pool_path.c_str(), F_OK) != 0) {
    spdlog::info("create poolset {}", pool_path);
    pool = graph_pool::create(pool_path);
    graph = pool->create_graph(db_name, bp_size);
  } else {
    spdlog::info("open poolset {}", pool_path);
    pool = graph_pool::open(pool_path, true);
    graph = pool->open_graph(db_name, bp_size);
  }

  if (!import_files.empty()) {
    spdlog::info("--------- Importing files ...");
    import_csv_files(graph, import_path, import_files, delim_character, format, strict);
    graph->print_stats();
  }

  if (!dot_file.empty())
    graph->dump_dot(dot_file);

  query_ctx ctx(graph);
  qproc_ptr = std::make_unique<query_proc>(ctx);

  if (!query_file.empty()) {
    mode = script_mode;
    // load the query from the file
    auto query_string = read_from_file(query_file);
    exec_query(query_string, qmode, false);
  }

  if (start_shell || mode == undefined_mode) {
    run_shell(graph, qmode);
  }

  graph->flush();
  graph->close_files();
}
 
