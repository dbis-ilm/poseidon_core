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

#ifndef qop_hpp_
#define qop_hpp_

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <memory>
#include <vector>
#include <set>
#include <iterator>
#include <condition_variable>
#include <atomic>
#include <unordered_map>

#include "defs.hpp"
#include "graph_db.hpp"
#include "nodes.hpp"
#include "relationships.hpp"
#include "properties.hpp"
#include "profiling.hpp"
#include "expression.hpp"
#include "binary_expression.hpp"
#include "qresult_iterator.hpp"
#include "qop_visitor.hpp"
// #include "query_ctx.hpp"

template <typename T> std::vector<T> append(const std::vector<T> &v, T t) {
  std::vector<T> v2;
  v2.reserve(v.size() + 1);
  v2.insert(v2.end(), v.begin(), v.end());
  v2.push_back(t);
  return v2;
}

template <typename T>
std::vector<T> concat(const std::vector<T> &v1, const std::vector<T> &v2) {
  std::vector<T> v;
  v.reserve(v1.size() + v2.size());
  std::copy(v1.begin(), v1.end(), std::back_inserter(v));
  std::copy(v2.begin(), v2.end(), std::back_inserter(v));
  return v;
}

enum class qop_type {
    none,
    any,
    scan,
    project,
    printer,
    is_property,
    filter,
    foreach_rship,
    expand,
    node_has_label,
    union_all,
    cross_join,
    left_join,
    hash_join,
    nest_loop_join,
    sort,
    limit,
    distinct,
    collect,
    aggregate,
    order_by,
    group_by,
    create,
    store,
    end
};

/**
 * Tuple result types
 */
enum class result_type {
    node = 0,
    relationship = 1,
    integer = 2,
    double_t = 3,
    string = 4,
    date = 5,
    time = 6,
    boolean = 7,
    uint64 = 8,
    none = 9,
    qres = 10
};

/**
 * Directions of relationships.
 */
enum class RSHIP_DIR {
    FROM = 0,
    TO = 1,
    ALL = 2
};

/**
 * Directions of the expand operator.
 */
enum class EXPAND {
    IN,
    OUT
};

/**
 * Functions to get a property value (defined by var=pos and property name) from a qr_tuple.
 */
p_item get_property_value(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop);

template <typename T>
T get_property_value(query_ctx &ctx, const qr_tuple& v, std::size_t var, const std::string& prop);


struct qop;
using qop_ptr = std::shared_ptr<qop>;

/**
 * qop represents the abstract base class for all query operators. It implements
 * a push-style query engine where the execution is initiated by calling the
 * start method and results of an operator are forwarded to the subsequent
 * operator by invoking its consume method. This consume method as well as the
 * finish method are invoked via a signal/slot mechanism implemented via the
 * connect method.
 */
struct qop {
  /**
   * function pointer for a function called as subscriber consume function, i.e.
   * a function which  processes the list of graph elements (nodes,
   * relationships) given in vector v and forwards the results to the next
   * operator. These elements are contained in the graph gdb.
   */
  using consume_func = std::function<void(query_ctx &, const qr_tuple &)>;

  /**
   * function pointer for a function called as subscriber finish function, i.e.
   * a function which is called when all results have been processed.
   */
  using finish_func = std::function<void(query_ctx &)>;

  /**
   * Constructor.
   */
  qop() : subscriber_(nullptr), consume_(nullptr), finish_(nullptr) {}

  /**
   * Destructor.
   */
  virtual ~qop() = default;

  /**
   * Starts the processing of the whole query. This method is implemented only
   * in producer operators such as scan_nodes.
   */
  virtual void start(query_ctx &ctx) {}

  /**
   * Prints a description of the operator and recursively calls
   * dump of the subscribers.
   */
  virtual void dump(std::ostream &os) const {}

  void connect(qop_ptr op) {
    subscriber_ = op;
  }

  /**
   * Registers the subscriber by initializing the subscriber_ pointer and
   * registering its consume function. The finish function is set to the default
   * function.
   */
  void connect(qop_ptr op, consume_func cf) {
    subscriber_ = op;
    consume_ = cf;
    finish_ = std::bind(&qop::default_finish, subscriber_.get(),
                        std::placeholders::_1);
  }

  /**
   * Registers the subscriber by initializing the subscriber_ pointer and
   * registering its consume and finish functions.
   */
  void connect(qop_ptr op, consume_func cf, finish_func ff) {
    subscriber_ = op;
    consume_ = cf;
    finish_ = ff;
  }

  /**
   * This method is called after all results have been processed by the producer
   * operator. The default behavior is to propagate this event to all
   * subscribers, but operators such as ORDER_BY can use it to implement their
   * own behavior.
   */
  void default_finish(query_ctx &ctx) {
    if (finish_)
      finish_(ctx);
  }

  /**
   * Returns true if the operator has already a subscriber.
   */
  inline bool has_subscriber() const { return subscriber_.get() != nullptr; }

  /**
   * Return the current subscriber.
   */
  inline qop_ptr subscriber() { return subscriber_; }

  /**
   * Return true if this operator is a binary operator (join, union etc.)
   */
  virtual bool is_binary() const { return false; }

  PROF_ACCESSOR;

  /**
   * Accept method for generic visitor
   */
  virtual void accept(qop_visitor& vis) = 0;

  /**
   * Accept method for code generation
   */
  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) = 0;

  unsigned operator_id_;

  qop_type type_;
protected:

  qop_ptr subscriber_; // pointer to the subsequent operator which receives and
                       // processes the results

  consume_func consume_; // pointer to the subscriber's consume function
  finish_func finish_;   // pointer to the subscriber's finish function

  PROF_DATA;
};

/**
 * is_property is a query operator for filtering nodes/relationships based on
 * their properties. For this purpose, the name of the property and a predicate
 * function for checking the value of this property have to be given.
 */
struct is_property : public qop, public std::enable_shared_from_this<is_property> {
  is_property(const std::string &p, std::function<bool(const p_item &)> pred)
      : property(p), pcode(0), predicate(pred) { type_ = qop_type::is_property; }
  ~is_property() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

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

  std::string property;
  dcode_t pcode;
  std::function<bool(const p_item &)> predicate;
};

/**
 * node_has_label is a query operator representing a filter for nodes which have
 * the given label.
 */
struct node_has_label : public qop, public std::enable_shared_from_this<node_has_label> {
  node_has_label(const std::vector<std::string> &l) : labels(l), lcode(0) {
    type_ = qop_type::node_has_label;
  }
  node_has_label(const std::string &l) : label(l), lcode(0) {
    type_ = qop_type::node_has_label;
  }
  ~node_has_label() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = labels.empty() ? 1 : labels.size();

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  std::vector<std::string> labels;
  std::string label;
  dcode_t lcode;
};

struct expand : public qop, public std::enable_shared_from_this<expand> {
  expand(EXPAND dir) : dir_(dir) { type_ = qop_type::expand;  }

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

  EXPAND dir_;
  std::string label;
};

/**
 * get_from_node is a query operator for retrieving the node at the FROM side of
 * a relationship which is given in the last element of vector v.
 */
struct get_from_node : public expand {
  get_from_node() : expand(EXPAND::IN) {}

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

};

/**
 * get_to_node is a query operator for retrieving the node at the TO side of
 * a relationship which is given in the last element of vector v.
 */
struct get_to_node : public expand {
  get_to_node() : expand(EXPAND::OUT) {}

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

};

/**
 * printer is a query operator to output the query results collected in the
 * vector v to standard output.
 */
struct printer : public qop, public std::enable_shared_from_this<printer> {
  printer() : ntuples_(0), output_width_(0) { type_ = qop_type::printer; }

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
    if(has_subscriber())
      subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  std::size_t ntuples_;
  std::size_t output_width_;
};

/**
 * limit_result is a query operator for producing only the given number of
 * results.
 */
struct limit_result : public qop, std::enable_shared_from_this<limit_result> {
  limit_result(std::size_t n) : num_(n), processed_(0) { type_ = qop_type::limit;  }
  ~limit_result() = default;

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

  std::size_t num_;
  std::atomic_ulong processed_;
};


extern result_set::sort_spec_list sort_spec_;
/**
 * order_by implements an operator for sorting results either by giving a
 * comparison function or a specificaton of sorting criteria.
 */
struct order_by : public qop, public std::enable_shared_from_this<order_by> {
    order_by(const result_set::sort_spec_list &spec)  { type_ = qop_type::order_by; sort_spec_= spec; }
 
  order_by(std::function<bool(const qr_tuple &, const qr_tuple &)> func)
      //: cmp_func_(func) 
  { 
    cmp_func_ = func;
    type_ = qop_type::order_by;  
  }
  ~order_by() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  static void sort(result_set *rs) {
      query_ctx ctx; // TODO!!!!
      if (cmp_func_ != nullptr)
        rs->sort(ctx, cmp_func_);
      else {
        rs->sort(ctx, sort_spec_);
      }
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;
    auto next_offset = 0;

    vis.visit(shared_from_this());
    subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);
  }

  result_set results_;
  static std::function<bool(const qr_tuple &, const qr_tuple &)> cmp_func_;
};

/**
 * distinct_tuples implements an operator for outputing distinct
 * result tuples.
 */
struct distinct_tuples : public qop, public std::enable_shared_from_this<distinct_tuples> {
  distinct_tuples() { type_ = qop_type::distinct; }
  ~distinct_tuples() = default;

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

  std::mutex m_;
  std::set<std::string> keys_;
};

/**
 * filter_tuple implements an operator that filters a tuple
 * based on a predicate function.
 */
struct filter_tuple : public qop, public std::enable_shared_from_this<filter_tuple> {
  filter_tuple(std::function<bool(const qr_tuple &)> func)
      : pred_func1_(func) { type_ = qop_type::filter;  }
  filter_tuple(const expr &ex): ex_(ex) { type_ = qop_type::filter;  }

  ~filter_tuple() = default;

  void dump(std::ostream &os) const override;

  void process(query_ctx &ctx, const qr_tuple &v);

  void finish(query_ctx &ctx);

  expr get_expression() { return ex_; }

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

  std::function<bool(const qr_tuple &)> pred_func1_;
  std::function<bool(const qr_tuple &, expr&)> pred_func2_;
  expr ex_;
};

/**
 * union_all_op implements an operator that unions all the
 * query tuples of the left query pipeline and the right query
 * pipeline(s).
 */
struct union_all_op : public qop, public std::enable_shared_from_this<union_all_op> {
  union_all_op() : init_(true), phases_(0) { type_ = qop_type::union_all; }
  ~union_all_op() = default;

  void dump(std::ostream &os) const override;

  void process_left(query_ctx &ctx, const qr_tuple &v);
  void process_right(query_ctx &ctx, const qr_tuple &v);

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

  bool is_binary() const override { return true; }

  bool init_;
  std::size_t phases_;
  std::list<qr_tuple> res_;
};

/**
 * Operator for printing the content of a result set.
 */
std::ostream &operator<<(std::ostream &os, const result_set &rs);

/**
 * collect_result is a query operator for collecting results of a query: either
 * to check the results or to further process the data. Note, that all result
 * values are represented as strings.
 */
struct collect_result : public qop, public std::enable_shared_from_this<collect_result> {
  collect_result(result_set &res) : results_(res) { type_ = qop_type::collect;  }
  ~collect_result() = default;

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
    auto next_offset = 1;

    vis.visit(shared_from_this());
    if(has_subscriber())
      subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);

  }

  result_set &results_;
private:
  std::mutex collect_mtx;
};

/**
 * end_pipeline is a query operator to end a query pipeline without
 * collecting the query results.
 */
struct end_pipeline : public qop, public std::enable_shared_from_this<end_pipeline> {
  end_pipeline() {
    type_ = qop_type::end;
    other_ = qop_type::none;
  }

  void dump(std::ostream &os) const override;

  void process();

  void accept(qop_visitor& vis) override { 
    vis.visit(shared_from_this()); 
    if (has_subscriber())
      subscriber_->accept(vis);
  }

  virtual void codegen(qop_visitor & vis, unsigned & op_id, bool interpreted = false) override {
    operator_id_ = op_id;

    vis.visit(shared_from_this());
    if(has_subscriber()) {
      //subscriber_->codegen(vis, operator_id_+=next_offset, interpreted);    
    }
  }

  void set_other(qop_type other, std::size_t other_idx = -1) {
    other_ = other;
    other_idx_ = other_idx;
  }

  qop_type other_;
  std::size_t other_idx_;
};

/**
 * Macro to simplify definition of arguments in project etc.
 * Usage: Instead of requiring to define a lambda expression
 *        we simply use PExpr_(my_func(res, ...))
 *        Note that res has to be used to refer to the query_result vector.
 */
#define PExpr_(i, func)                                                        \
  projection::expr {                                                           \
    i, [&](auto ctx, auto res) { return func; }                                \
  }

#define PVar_(i)                                                               \
  projection::expr { i, nullptr }

/**
 * Structure to express projections on tuple elements
 */
struct projection_expr {
    /**
     * Possible projection types
     */
    enum PROJECTION_TYPE {
        PROPERTY_PR,
        FORWARD_PR,
        FUNCTIONAL_VAL,
        CONDITIONAL_VAL,
    };

    typedef int (*int_prj_func_node)(node *n);

    typedef query_result (*udf_projection)(query_ctx*, void*);
    /**
     * The position of the tuple element to project
     */
    std::size_t id;

    /**
     * The projection key / property name
     */
    std::string key;

    /**
     * The property type
     */
    result_type type;

    /**
     * Flag to project only if the key exists
     */
    bool if_exist_;

    /**
     * List to check if a tuple element has the properties
     */
    std::vector<std::string> has_properties;

    /**
     * If/Else construct to express an alternative projection
     */
    std::pair<std::string, std::string> then_else;
    
    /**
     * UDF for projection
     */
    int_prj_func_node int_node_func;
    udf_projection udf_function;

    /**
     * The actual projection type
     */
    PROJECTION_TYPE prt;

    projection_expr(udf_projection udf) : udf_function(udf), prt(PROJECTION_TYPE::FUNCTIONAL_VAL) {}
    projection_expr(std::size_t i) : id(i), type(result_type::none), int_node_func(nullptr), prt(PROJECTION_TYPE::FORWARD_PR)  {}
    projection_expr(std::size_t i, int_prj_func_node func) : id(i), int_node_func(func), prt(PROJECTION_TYPE::FUNCTIONAL_VAL) {}
    projection_expr(std::size_t i, std::string k, result_type t, bool if_exist = false) : id(i), key(k), type(t), if_exist_(if_exist), prt(PROJECTION_TYPE::PROPERTY_PR) {}
    projection_expr(std::size_t i, std::vector<std::string> properties, std::pair<std::string, std::string> then) : 
        id(i), has_properties(properties), then_else(then), prt(PROJECTION_TYPE::CONDITIONAL_VAL) {}
};

/**
 * projection implements a project operator.
 */
struct projection : public qop, public std::enable_shared_from_this<projection> {
  struct expr {
    std::size_t vidx;
    std::function<query_result(query_ctx&, const query_result&)> func;
    expr() = default;
    // expr(const expr& ex) = default;
    expr(std::size_t i, std::function<query_result(query_ctx&, const query_result&)> f) : vidx(i), func(f) {}
  };

  using expr_list = std::vector<expr>;

  projection(const expr_list &exprs);
  projection(const expr_list &exprs, std::vector<projection_expr>& prexpr);

  projection(const std::vector<projection_expr>& prexpr) : prexpr_(prexpr) {
    type_ = qop_type::project;
  }

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

  void init_expr_vars();

  expr_list exprs_;
  std::size_t nvars_, npvars_;
  std::vector<std::size_t> var_map_;
  std::set<std::size_t> accessed_vars_;

  std::vector<projection_expr> prexpr_;
  std::vector<int> new_types;
};

#endif
