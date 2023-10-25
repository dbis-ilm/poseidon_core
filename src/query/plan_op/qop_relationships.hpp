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

#ifndef qop_relationships_hpp_
#define qop_relationships_hpp_

#include "qop.hpp"


struct foreach_relationship : public qop, public std::enable_shared_from_this<foreach_relationship> {
  foreach_relationship() = default;
  
  foreach_relationship(RSHIP_DIR dir, const std::string &l, int pos = std::numeric_limits<int>::max()) 
      : dir_(dir), label(l), lcode(0), npos(pos) { type_ = qop_type::foreach_rship;  }

  foreach_relationship(RSHIP_DIR dir, const std::string &l, std::size_t min,
                                     std::size_t max, int pos = std::numeric_limits<int>::max())
      : dir_(dir), label(l), lcode(0), min_range(min), max_range(max), npos(pos) { type_ = qop_type::foreach_rship;  }

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 1;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);    
  }

  RSHIP_DIR dir_;
  std::string label;
  dcode_t lcode;
  std::size_t min_range, max_range;
  int npos;
};

/**
 * foreach_from_relationship is a query operator that scans all outgoing
 * relationships of a given node. This node is the last elment in the vector v
 * of the consume method.
 */
struct foreach_from_relationship : public foreach_relationship {
  foreach_from_relationship(const std::string &l, int pos = std::numeric_limits<int>::max()) : foreach_relationship(RSHIP_DIR::FROM, l, pos) {}
  ~foreach_from_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }
};

/**
 * foreach_variable_from_relationship is a query operator that scans all
 * outgoing relationships of a given node recursively within the given range.
 * This node is the last elment in the vector v of the consume method.
 */
struct foreach_variable_from_relationship : public foreach_relationship {
  foreach_variable_from_relationship(const std::string &l, std::size_t min,
                                     std::size_t max, int pos = std::numeric_limits<int>::max())
      : foreach_relationship(RSHIP_DIR::FROM, l, min, max, pos) {}
      
  ~foreach_variable_from_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    
  }
};

/**
 * foreach_bi_dir_relationship is a query operator that scans all outgoing
 * and incoming relationships of a given node. This node is the last elment 
 * in the vector v of the consume method or given by the index position pos.
 */
struct foreach_all_relationship : public foreach_relationship {
  foreach_all_relationship(const std::string &l, int pos) : foreach_relationship(RSHIP_DIR::ALL, l, pos) {}
  
  ~foreach_all_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    
  }
};

/**
 * foreach_variable_bi_dir_relationship is a query operator that scans all
 * outgoing and incoming relationships of a given node recursively within the given range.
 * This node is the last elment in the vector v of the consume method or given by the
 * position index pos.
 */
struct foreach_variable_all_relationship : public foreach_relationship {
  foreach_variable_all_relationship(const std::string &l, std::size_t min,
                                     std::size_t max, int pos)
      : foreach_relationship(RSHIP_DIR::ALL, l, min, max, pos) {}
  ~foreach_variable_all_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    
  }
};

/**
 * foreach_to_relationship is a query operator that scans all incoming
 * relationships of a given node. This node is the last elment in the vector v
 * of the consume method.
 */
struct foreach_to_relationship : public foreach_relationship {
  foreach_to_relationship(const std::string &l, int pos = std::numeric_limits<int>::max()) : foreach_relationship(RSHIP_DIR::TO, l, pos) {}
  ~foreach_to_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

    void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

};

/**
 * foreach_variable_to_relationship is a query operator that scans all incoming
 * relationships of a given node recursively within the given range.
 * This node is the last elment in the vector v of the consume method.
 */
struct foreach_variable_to_relationship : public foreach_relationship {
  foreach_variable_to_relationship(const std::string &l, std::size_t min,
                                   std::size_t max, int pos = std::numeric_limits<int>::max())
      : foreach_relationship(RSHIP_DIR::TO, l, min, max, pos) {}
  ~foreach_variable_to_relationship() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

    void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

};

#endif