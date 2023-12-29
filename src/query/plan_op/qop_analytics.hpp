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

#ifndef qop_analytics_hpp_
#define qop_analytics_hpp_

#include "qop.hpp"
#include "shortest_path.hpp"

/**
 * shortest_path_opr implements an operator that finds the
 * unweighted shortest path between two nodes.
 * all_spaths_ specfies if all shortest path of equal 
 * distance are searched.
 */
struct shortest_path_opr : public qop, public std::enable_shared_from_this<shortest_path_opr> {
  shortest_path_opr(std::pair<std::size_t, std::size_t> uv,
    rship_predicate pred, bool bidir, bool all) : bidirectional_(bidir),
          all_spaths_(all), rpred_(pred), start_stop_(uv) {}
  ~shortest_path_opr() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  path_item path_;
  bool bidirectional_;
  bool all_spaths_;
  rship_predicate rpred_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

/**
 * weighted_shortest_path_opr implements an operator that finds the
 * weighted shortest path between two nodes.
 * all_spaths_ specfies if all shortest path of equal 
 * weight are searched.
 */
struct weighted_shortest_path_opr : public qop, public std::enable_shared_from_this<weighted_shortest_path_opr> {
  weighted_shortest_path_opr(std::pair<std::size_t, std::size_t> uv, rship_predicate pred,
    rship_weight weight, bool bidir, bool all) : bidirectional_(bidir), all_spaths_(all),
      rpred_(pred), rweight_(weight), start_stop_(uv) {}
  ~weighted_shortest_path_opr() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  path_item path_;
  bool bidirectional_;
  bool all_spaths_;
  rship_predicate rpred_;
  rship_weight rweight_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

#ifdef USE_GUNROCK
/**
 * gunrock_bfs_opr implements an operator for Breadth-First Search 
 * leveraging Gunrock.
 */
struct gunrock_bfs_opr : public qop {
  gunrock_bfs_opr(std::size_t start,
                  bool bidir) : start_(start), bidirectional_(bidir) {}
  ~gunrock_bfs_opr() = default;

  void dump(std::ostream &os) const override;

  void process(graph_db_ptr &gdb, const qr_tuple &v);

  std::size_t start_;
  bool bidirectional_;
};

/**
 * weighted_sssp_opr implements an operator for Single-Source
 * Shortest Path search leveraging Gunrock.
 */
struct gunrock_sssp_opr : public qop {
  gunrock_sssp_opr(std::size_t start, rship_weight weight, bool bidir) :
                    start_(start), rweight_(weight), bidirectional_(bidir) {}
  ~gunrock_sssp_opr() = default;

  void dump(std::ostream &os) const override;

  void process(graph_db_ptr &gdb, const qr_tuple &v);

  std::size_t start_;
  rship_weight rweight_;
  bool bidirectional_;
};

/**
 * gunrock_pr implements an operator for the PageRank algorithm leveraging Gunrock.
 */
struct gunrock_pr_opr : public qop {
  gunrock_pr_opr(bool bidir) : bidirectional_(bidir) {}
  ~gunrock_pr_opr() = default;

  void dump(std::ostream &os) const override;

  void process(graph_db_ptr &gdb, const qr_tuple &v);

  bool bidirectional_;
};
#endif

/**
 * k_weighted_shortest_path_opr implements an operator that finds the
 * top k weighted shortest path between two nodes.
 */
struct k_weighted_shortest_path_opr : public qop, public std::enable_shared_from_this<k_weighted_shortest_path_opr> {
  k_weighted_shortest_path_opr(std::pair<std::size_t, std::size_t> uv,
    std::size_t k, rship_predicate pred, rship_weight weight, bool b) : 
    k_(k), bidirectional_(b), rpred_(pred), rweight_(weight), start_stop_(uv) {}
  ~k_weighted_shortest_path_opr() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  std::size_t k_;
  path_item path_;
  bool bidirectional_;
  rship_predicate rpred_;
  rship_weight rweight_;
  std::pair<std::size_t, std::size_t> start_stop_;
};

/**
 * csr_data implements an operator to get data for for conversion 
 * to the CSR format.
 * The ids of the neighbours of each node of the graph are obtained. 
 * In addition, the weight of the relationship connecting each 
 * neighbouring node is computed from the weight function. 
 * The bidirectional flag specifies whether only outgoing relationships 
 * are considered (false) or both outgoing and incoming relationships 
 * are considered (true).
 */
struct csr_data : public qop, public std::enable_shared_from_this<csr_data> {
  csr_data(rship_weight func, bool bidir = false,
           std::size_t pos = std::numeric_limits<std::size_t>::max())
           : pos_(pos), bidirectional_(bidir), weight_func_(func) {}
  ~csr_data() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  std::size_t pos_;
  bool bidirectional_;
  std::function<double (relationship &)> weight_func_;
};

#endif