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

#ifndef join_hpp_
#define join_hpp_

#include "qop.hpp"

/**
 * cross_join implements a binary operator for constructing the cartesian
 * product of the results provided by the two input query operators.
 */
struct cross_join : public qop {
  cross_join() = default;
  ~cross_join() = default;

  void dump(std::ostream &os) const override;

  void process_left(graph_db_ptr &gdb, const qr_tuple &v);
  void process_right(graph_db_ptr &gdb, const qr_tuple &v);

  void finish(graph_db_ptr &gdb);

private:
  std::list<qr_tuple> input_;
};

#endif