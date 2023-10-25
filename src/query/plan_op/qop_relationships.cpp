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

#include "qop_relationships.hpp"

void foreach_from_relationship::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  node *n = nullptr;
  if (npos == std::numeric_limits<int>::max())
    n = boost::get<node *>(v.back());
  else
    n = boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_from_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    consume_(ctx, v2);
    num++;
  });
  PROF_POST(num);
}

void foreach_from_relationship::dump(std::ostream &os) const {
  os << "foreach_from_relationship([" << label << "]) - " << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */

void foreach_variable_from_relationship::process(query_ctx &ctx,
                                                 const qr_tuple &v) {
  PROF_PRE;
  node *n = nullptr;
  if (npos == std::numeric_limits<int>::max())
    n = boost::get<node *>(v.back());
  else
    n = boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_variable_from_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        consume_(ctx, v2);
        num++;
      });
  PROF_POST(num);
}

void foreach_variable_from_relationship::dump(std::ostream &os) const {
  os << "foreach_variable_from_relationship([" << label << ", (" << min_range
     << "," << max_range << ")]) - " << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */

void foreach_all_relationship::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  node *n = npos == std::numeric_limits<int>::max() ?
            boost::get<node *>(v.back()) : boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_from_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    v2 = append(v2, query_result(&(ctx.gdb_->node_by_id(r.dest_node))));
    consume_(ctx, v2);
    num++;
  });
  ctx.foreach_to_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    v2 = append(v2, query_result(&(ctx.gdb_->node_by_id(r.src_node))));
    consume_(ctx, v2);
    num++;
  });
  PROF_POST(num);
}

void foreach_all_relationship::dump(std::ostream &os) const {
  os << "foreach_all_relationship([" << label << "]) - " << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */

void foreach_variable_all_relationship::process(query_ctx &ctx,
                                                 const qr_tuple &v) {
  PROF_PRE;
  node *n = npos == std::numeric_limits<int>::max() ?
            boost::get<node *>(v.back()) : boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_variable_from_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        v2 = append(v2, query_result(&(ctx.gdb_->node_by_id(r.dest_node))));
        consume_(ctx, v2);
        num++;
      });
  ctx.foreach_variable_to_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        v2 = append(v2, query_result(&(ctx.gdb_->node_by_id(r.src_node))));
        consume_(ctx, v2);
        num++;
      });
  PROF_POST(num);
}

void foreach_variable_all_relationship::dump(std::ostream &os) const {
  os << "foreach_variable_all_relationship([" << label << ", (" << min_range
     << "," << max_range << ")]) - " << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */

void foreach_to_relationship::process(query_ctx &ctx, const qr_tuple &v) {
  PROF_PRE;
  node *n = nullptr;
  if (npos == std::numeric_limits<int>::max())
    n = boost::get<node *>(v.back());
  else
    n = boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_to_relationship_of_node(*n, lcode, [&](relationship &r) {
    auto v2 = append(v, query_result(&r));
    consume_(ctx, v2);
    num++;
  });
  PROF_POST(num);
}

void foreach_to_relationship::dump(std::ostream &os) const {
  os << "foreach_to_relationship([" << label << "]) - " << PROF_DUMP;
}

/* ------------------------------------------------------------------------ */
void foreach_variable_to_relationship::process(query_ctx &ctx,
                                               const qr_tuple &v) {
  PROF_PRE;
  node *n = nullptr;
  if (npos == std::numeric_limits<int>::max())
    n = boost::get<node *>(v.back());
  else
    n = boost::get<node *>(v[npos]);

  if (lcode == 0)
    lcode = ctx.gdb_->get_code(label);

  uint64_t num = 0;
  ctx.foreach_variable_to_relationship_of_node(
      *n, lcode, min_range, max_range, [&](relationship &r) {
        auto v2 = append(v, query_result(&r));
        consume_(ctx, v2);
        num++;
      });
  PROF_POST(num);
}

void foreach_variable_to_relationship::dump(std::ostream &os) const {
  os << "foreach_variable_to_relationship([" << label << ", (" << min_range
     << "," << max_range << ")]) - " << PROF_DUMP;
}
