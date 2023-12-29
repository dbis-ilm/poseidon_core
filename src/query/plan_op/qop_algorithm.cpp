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

#include "qop_algorithm.hpp"

#include "algorithms.cpp"

algorithm_op::algorithm_op(const std::string& algo_name, algorithm_op::mode_t m, param_list& args) : 
  call_mode_(m), algorithm_name_(algo_name), args_(args), tuple_func_ptr_(nullptr), set_func_ptr_(nullptr) {
    // TODO: initialize function ptr
    if (call_mode_ == m_tuple) {
      auto it = tuple_algorithms_.find(algo_name);
      if (it != tuple_algorithms_.end()) {
        tuple_func_ptr_ = it->second;
      }  
    }
    else {
      auto it = set_algorithms_.find(algo_name);
      if (it != set_algorithms_.end()) {
        set_func_ptr_ = it->second;
      }  
    }
}

void algorithm_op::dump(std::ostream &os) const {
  os << "algorithm([ " << algorithm_name_ << ","
     << (call_mode_ == m_tuple ? "TUPLE" : "SET");
  for (auto& val : args_) {
    os << ", " << val;
  }
  os << "]) - " << PROF_DUMP;
}

void algorithm_op::process(query_ctx &ctx, const qr_tuple &v) {
  assert(! (tuple_func_ptr_ == nullptr  && set_func_ptr_ == nullptr));
  PROF_PRE;
  if (call_mode_ == m_tuple) {
    auto res = tuple_func_ptr_(ctx, v, args_);
    if (!res.empty()) {
      std::size_t n = 0;
      for (auto& tup : res) {
      auto v2 = concat(v, tup);
      consume_(ctx, v2);
      n++;
    }
      PROF_POST(n);
  }
  else {
      PROF_POST(0);
      return;
    }
  }
  else {
    input_.append(v);
    PROF_POST(0);
  }
}

void algorithm_op::finish(query_ctx &ctx) {
  PROF_PRE0;
std::size_t n = 0;
  if (call_mode_ == m_set) {
    auto res = set_func_ptr_(ctx, input_, args_);
    if (!res.empty()) {
      for (auto& tup : res) {
      consume_(ctx, tup);
n++;
      }
    }
  } 
  finish_(ctx);
  PROF_POST(n);
}
