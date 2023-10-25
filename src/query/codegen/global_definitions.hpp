/**
 * This file provides external methods and structures, used in the query operator processing.
 * Foreach method exists a equivalent LLVM IR prototype and FunctionCallee object.
 */

#ifndef PJIT_GLOBAL_DEFINITIONS_HPP
#define PJIT_GLOBAL_DEFINITIONS_HPP
#include <dict/dict.hpp>
#include <storage/graph_db.hpp>
#include <storage/nodes.hpp>
#include <defs.hpp>
#include <iostream>
#include "qop.hpp"

#include <boost/thread/barrier.hpp>
#include <boost/hana.hpp>

#include "proc/joiner.hpp"
#include "proc/grouper.hpp"

/**
 * Thread local storage to store the Projection of a tuple. These values are
 * materialized to the result_set.
 * //TODO: remove map
 */
extern thread_local std::map<int, uint64_t> uint_result;
extern thread_local std::map<int, std::string> str_result;
extern thread_local std::map<int, boost::posix_time::ptime> time_result;
extern thread_local std::vector<relationship*> fev_rship_list;
extern thread_local std::vector<relationship*>::iterator fev_list_iter;
extern thread_local std::string grpkey_buffer;
extern std::map<int, std::function<std::string(graph_db_ptr*, int*)>> con_map;

class joiner;
class base_joiner;
class nested_loop_joiner;

using query_time_point = std::chrono::time_point<std::chrono::high_resolution_clock>;
struct query_context {
    query_ctx* ctx;
    std::size_t first_chunk;
    std::size_t last_chunk;
    transaction_ptr tx;
    uint64_t** args;

    std::vector<std::pair<int,size_t>> profiling_time;
    std::map<int, size_t> profiling_count;

    query_context(query_ctx *c, std::size_t first, std::size_t last, transaction_ptr t, uint64_t **qargs) :
        ctx(c), first_chunk(first), last_chunk(last), tx(t), args(qargs) {}

    void add_time(int operator_id,  query_time_point start, query_time_point end) {
        auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        profiling_time.push_back({operator_id, diff.count()});
    }

    void incr_count(int operator_id) {
        profiling_count[operator_id]++;
    }   
};

query_time_point get_now();
void add_time_diff(query_context* qtx, int op_id, query_time_point t1, query_time_point t2);

/**
 * The type for the generated query function.
 */ 
//using start_ty = void(*)(graph_db*, int, int, transaction_ptr, int, std::vector<int>*, result_set*, int**, finish_fct_type, uint64_t, uint64_t**);
using start_ty = void(*)(query_context*, uint64_t**);
using finish_fct_type = void(*)(query_context*, uint64_t**);

template <typename T>
using item_vec = buffered_vec<T>;

/**
 * Function to obtain the iterator of a node vector
 */
node_list<item_vec>::range_iterator *get_vec_begin(node_list<item_vec> *vec, size_t first, size_t last);

/**
 * Function to obtain the next iterator of a node vector
 */
node_list<item_vec>::range_iterator *get_vec_next(node_list<item_vec>::range_iterator *it);

/**
 * Boolean function to check if the end of the node vector is reached
 */
 bool vec_end_reached(node_list<item_vec> &vec, node_list<item_vec>::range_iterator *it);

/**
 * Function to obtain the iterator of a relationship vector
 */
relationship_list<item_vec>::iterator get_vec_begin_r(relationship_list<item_vec> &vec);

/**
 * Function to obtain the next iterator of a relationship vector
 */
relationship_list<item_vec>::iterator *get_vec_next_r(relationship_list<item_vec>::iterator *it);

/**
 * Boolean function to check if the end of the relationship vector is reached
 */
bool vec_end_reached_r(relationship_list<item_vec> &vec, relationship_list<item_vec>::iterator it);

/**
 * Function to lookup a label
 */
 dcode_t dict_lookup_label(query_ctx *ctx, char *label);

/**
 * Obtains the pointer to a node from a node vector iterator
 */
 node *get_node_from_it(node_list<item_vec>::range_iterator *it);

/**
 * Obtains the pointer to a relationship from a relationship vector iterator
 */
 relationship *get_rship_from_it(relationship_list<item_vec>::iterator *it);

/**
 * Returns a pointer to the node chunked vector of a given graph
 */
node_list<item_vec>::vec *gdb_get_nodes(query_ctx *ctx);

/**
 * Returns a pointer to the relationship chunked vector of a given graph
 */
relationship_list<item_vec>::vec *gdb_get_rships(query_ctx *ctx);

 void test_ints(uint64_t a, uint64_t b);

/**
 * Return the relationship pointer with the given id
 */
 relationship *rship_by_id(query_ctx *ctx, offset_t id);

/**
 * Return the node pointer with the given id
 */
 node *node_by_id(query_ctx *ctx, offset_t id);

/**
 * Returns the dictionary code of a given string
 */
 dcode_t gdb_get_dcode(query_ctx *ctx, char *property);

/**
 * Returns the property item at the given position in the property set
 */
 const property_set *pset_get_item_at(query_ctx *ctx, offset_t id);

/**
 * Init of the transaction processing
 */
 xid_t get_tx(transaction_ptr);

/**
 * Checks if the current node is valid for transactional processing
 */
 node * get_valid_node(query_ctx *ctx, node * n, transaction_ptr tx);

/**
 * Applies a given Projection on a node result and writes the result at a memory address, given by the caller.
 */
 void apply_pexpr_node(query_ctx *ctx, const char *key, result_type val_type, int *qr, int *ret);

/**
 * Applies a given Projection on a relationship result and writes the result at a memory address, given by the caller.
 */
 void apply_pexpr_rship(query_ctx *ctx, const char *key, result_type val_type, int *qr, int *ret);

/**
 * External function to count all potential 1-hop relationships, used by the variable ForeachRelationship operator
 */
 int count_potential_o_hop(query_ctx *ctx, offset_t rship_id);

/**
 * Allocates a queue in order to scan all relationships recursively
 */
 std::list<std::pair<relationship::id_t, std::size_t>> retrieve_fev_queue();

/**
 * Insert the relationship id and the hop count to the FEV queue 
 */
 void insert_fev_rship(std::list<std::pair<relationship::id_t, std::size_t>> &queue, relationship::id_t rid, std::size_t hop);

/**
 * Returns true when the queue, used by the FEV operator, is empty
 */
 bool fev_queue_empty(std::list<std::pair<relationship::id_t, std::size_t>> &queue);

/**
 * Method that processes the actual projection on a tuple result
 */
 void apply_pexpr(query_ctx *ctx, const char *key, result_type val_type, int *qr, int idx, std::vector<int> types, int *ret);

/**
 * Function to lookup a given dictionary code
 */
 const char* lookup_dc(query_ctx *ctx, dcode_t dc);

// void get_nodes(graph_db gdb, consumer_fct_type consumer);

/**
 * Function for the creation of a node with given properties
 */
extern "C" node* create_node_func(query_ctx *ctx, char *label, properties_t *props);

/**
 * Function for the creation of a relationship with given properties
 */
extern "C" relationship* create_rship_func(query_ctx *ctx, char *label, node *n1, node *n2, properties_t *props);

/**
 * Function to transform a register value into the appropriate type and materialize to 
 * thread local storage.
 */
 void mat_reg_value(query_ctx *ctx, int *reg, int type);

/**
 * collect_tuple inserts the tuple from thread_local storage into the given result_set.  
 * If print is true, the tuple will be printed to the standard output
 */
 void collect_tuple(query_ctx *ctx, result_set *rs, bool print);

qr_tuple &get_qr_tuple();

/**
 * obtain_mat_tuple returns a thread local tuple storage used to materialize the
 * rhs side of a join.
 */
 qr_tuple *obtain_mat_tuple();

/**
 * reg_to_qres converts a register result into a thread local query_result 
 */
 query_result *reg_to_qres(int *reg);

 void *node_to_description(query_ctx *ctx);

/**
 * mat_node materialize a node to a thread local tuple storage
 */
 void mat_node(node *n, qr_tuple *qr);

/**
 * mat_rship materialize a rship to a thread local tuple storage
 */
 void mat_rship(relationship *r, qr_tuple *qr);

/**
 * collect_tuple_join inserts the thread local tuple storage to a list of
 * the appropriate join operation with the id jid
 */
 void collect_tuple_join(base_joiner *j, int jid, qr_tuple *qr);

/**
 * get_join_tp_at returns a tuple from the join list at the given position
 */
 qr_tuple *get_join_tp_at(base_joiner *j, int jid, int pos);

/**
 * get_node_res_at returns a ptr to the node from the tuple at the given postion
 */
 node *get_node_res_at(qr_tuple *tuple, int pos);

/**
 * get_rship_res_at returns a ptr to the rship from the tuple at the given postion
 */
 relationship *get_rship_res_at(qr_tuple *tuple, int pos);

/**
 * get_mat_res_size returns the size of the materialized rhs list of a join with the id = jid
 */
 int get_mat_res_size(base_joiner *j, int jid);

/**
 * index_get_node is a helper method in order to process a index scan for a specific node
 */
 node *index_get_node(query_ctx *ctx, char *label, char *prop, uint64_t value);

 void foreach_from_variable_rship(query_ctx *ctx, dcode_t lcode, node *n, std::size_t min, std::size_t max);

 relationship *get_next_rship_fev();

 bool fev_list_end();

/**
 * Methods to process the group_by operation
 */
 void get_node_grpkey(node* n, unsigned pos);
 void get_rship_grpkey(relationship* r, unsigned pos);
 void get_int_grpkey(int i, unsigned pos);
 void get_double_grpkey(int* d_ptr, unsigned pos);
 void get_string_grpkey(int* str_ptr, unsigned pos);
 void get_time_grpkey(int* time_ptr, unsigned pos);
 void add_to_group(grouper *g);
 void finish_group_by(grouper *g, result_set* rs);
 void clear_mat_tuple();
 qr_tuple* grp_demat_at(grouper *g, int index);
 int get_grp_rs_count(grouper *g);

/**
 * Methods for the dematerialization of the tuple to IR registers
 */
 int int_to_reg(qr_tuple* qr, int pos);
 int double_to_reg(qr_tuple* qr, int pos);
 int str_to_reg(qr_tuple* qr, int pos);
 node* node_to_reg(qr_tuple* qr, int pos);
 relationship* rship_to_reg(qr_tuple* qr, int pos);
 int time_to_reg(qr_tuple* qr, int pos);

/**
 * Methods for the aggregation processing
 */
 void init_grp_aggr(grouper *g);
 int get_group_count(grouper *g);
 int get_total_group_count(grouper *g);
 int get_group_sum_int(grouper *g, int pos);
 double get_group_sum_double(grouper *g, int pos);
 uint64_t get_group_sum_uint(grouper *g, int pos);

 void append_to_tuple(query_result qr);

void insert_join_id_input(nested_loop_joiner *j, int jid, offset_t id);
offset_t get_join_id_at(nested_loop_joiner *j, int jid, int pos);

void collect_tuple_hash_join(joiner *j, int jid, int remainder, qr_tuple *qr);
void insert_join_bucket_input(joiner *j, int jid, int remainder, int id);

int get_hj_input_size(joiner *j, int jid, int bucket);
int get_hj_input_id(joiner *j, int jid, int bucket, int idx);
qr_tuple * get_query_result(joiner *j, int jid, int bucket, int idx);

int node_has_property(query_ctx *ctx, node *n, char *property);
int rship_has_property(query_ctx *ctx, relationship *r, char *property);
void apply_has_property(int has_properties_cnt, char *then_res, char *else_res, int *result);
void apply_if_property_exist(int has_property, char *property, int *result);

void end_notify(result_set *rs);

void print_int(int i);

#endif //PJIT_GLOBAL_DEFINITIONS_HPP