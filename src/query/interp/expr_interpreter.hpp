/*
 * Copyright (C) 2019-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef expr_interpreter_hpp_
#define expr_interpreter_hpp_

#include <iostream>
#include "qop.hpp"
#include "query_builder.hpp"
#include "query_ctx.hpp"
#include "expression.hpp"

std::ostream& operator<<(std::ostream& os, const query_result& qr);

bool interpret_expression(query_ctx& ctx, expr& ex, const qr_tuple& tup);

#endif