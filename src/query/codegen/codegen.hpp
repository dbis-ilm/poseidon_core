#ifndef POSEIDON_CORE_CODEGEN_INLINE_HPP
#define POSEIDON_CORE_CODEGEN_INLINE_HPP

#include "qop.hpp"
#include "qop_scans.hpp"
#include "qop_relationships.hpp"
#include "qop_joins.hpp"
#include "qop_aggregates.hpp"
#include "qop_visitor.hpp"
#include "jit/p_context.hpp"

void get_rhs_type(qop_ptr  &qop, std::vector<int> &typv);

/**
 * Visitor class for inlined query code generation.
 * Each operator code will generated into the same LLVM IR function.
 * For this purpose, only a single access exists: Scan or Create.
 * The main_function object is only instantiated once by the left-most child
 * of the query plan.
 */

class codegen_inline_visitor : public qop_visitor {
    PContext & ctx;
    unsigned cur_size;
    std::string qid_;
public:
    codegen_inline_visitor(PContext &ctx, std::string & qid) : ctx(ctx), qid_(qid) {cur_size = 0; }

    /*
    * The code generation methods for the actual algebra operators.
    */
    void visit(std::shared_ptr<scan_nodes> op);

    void visit(std::shared_ptr<index_scan> op);

    void visit(std::shared_ptr<foreach_relationship> op);

    void visit(std::shared_ptr<expand> op);

    void visit(std::shared_ptr<filter_tuple> op);

    void visit(std::shared_ptr<limit_result> op);

    void visit(std::shared_ptr<collect_result> op);

    void visit(std::shared_ptr<group_by> op);

    void visit(std::shared_ptr<order_by> op);

    void visit(std::shared_ptr<end_pipeline> op);

    void visit(std::shared_ptr<projection> op);

    void visit(std::shared_ptr<node_has_label> op);
    
    void visit(std::shared_ptr<cross_join_op> op);

    void visit(std::shared_ptr<nested_loop_join_op> op); 

    void visit(std::shared_ptr<hash_join_op> op);

    void visit(std::shared_ptr<left_outer_join_op> op); 

    void visit(std::shared_ptr<is_property> op) { } 

    void visit(std::shared_ptr<printer> op) { } 

    void visit(std::shared_ptr<distinct_tuples> op) { } 

    void visit(std::shared_ptr<union_all_op> op) { } 

     void visit(std::shared_ptr<shortest_path_opr> op) { } 

    void visit(std::shared_ptr<weighted_shortest_path_opr> op) { } 

    void visit(std::shared_ptr<k_weighted_shortest_path_opr> op) { } 

    void visit(std::shared_ptr<csr_data> op) { } 

    void visit(std::shared_ptr<create_node> op) { } 

    void visit(std::shared_ptr<create_relationship> op) { } 

    void visit(std::shared_ptr<update_node> op) { } 

    void visit(std::shared_ptr<detach_node> op) { } 

    void visit(std::shared_ptr<remove_node> op) { } 

    void visit(std::shared_ptr<remove_relationship> op) { } 

    /*
     * Initializer for the main function
     */
    void init_function(BasicBlock *entry);

    /*
     * Initializer for the finish function
     */
    void init_finish(BasicBlock *entry);

    /*
     * The actual main function which emits the
     * JITed start function
     */
    Function *main_function = nullptr;

    /*
     * The query finish function
    */
    Function *main_finish = nullptr;

    /*
     * Return basic block for returning to the previous
     * block. Only temporary
     */
    BasicBlock *main_return;

    /**
     * Entry point for other operators to manipulate the
     * query result before the actual materialization
     */
    BasicBlock *pre_tuple_mat;

    /*
     * BasicBlock for consume handling. Each operator, which is not the
     * access path inserts its branch to its entry block into this BasicBlock.
     * Afterwards it is instantiated with its own consume block.
     */
    BasicBlock *prev_bb;

    int query_id_inline = 0;

    /*
     * Global end block
     */
    BasicBlock *global_end;

    BasicBlock *entry;

    BasicBlock *inits;

    /*
     * The actual graph object
     */
    Value *gdb;

    /*
     * The result set where the tuples are stored
     */
    Value *rs;

    /*
     * Current size of the query results
     */
    Value *qr_size;

    /*
     * Generated type vector for the Projection
     */
    Value *ty;

    /*
     * Allocated array for tuple results
     */
    Value *resAlloc;

    /*
     * Finish function called by the access path
     */
    Value *finish;

    /*
     * Offset for shifting of tuples
     */
    Value *offset;

    /*
     * Query argument array of size 64
     */
    Value *queryArgs;

    /*
     * Alloca for UNKNOWN ID
     */
    Value *rhs_alloca;

    /*
     * Alloca for temporary string, used in Projections
     */
    Value *strAlloc;

    /*
     * Iteratir alloca, e.g., current relationship id
     */
    Value *lhs_alloca;

    /*
     * Current property item array, used in Filter
     */
    Value *cur_item_arr;

    /*
     * Current property item, used in Filter
     */
    Value *cur_item;

    /*
     * Current Property set, used in Filter
     */
    Value *cur_pset;

    /*
     * Array of the new Projection type
     */
    ArrayType *new_res_arr_type;

    /*
     * Alloca for new Projection results
     */
    Value * newResAlloc;

    /*
     * Temporary object for handling of Projection variables in memory
     */
    Value *pv[12]; // projection alloca variable //TODO: better without allocas

    /*
     * Projection exression strings used for allocation
     */
    std::vector<std::string> project_string;

    /*
     * Projection expression keys
     */
    std::vector<Value*> project_keys;

    /*
     * New type vector out of Projection expression list
     */
    std::vector<int> new_type_vec;

    /*
     * Max length of materialized query results, rhs JOIN
     */
    Value *max_idx;

    /*
     * Iterator for materialized query results, lhs JOIN
     */
    Value *cur_idx;

    /*
     * The identifiers of the join in a query, used to combine the intermediate results in the correct order
     */
    std::vector<Value *> cross_join_idx;

    /*
     * Flag to indacte of tuple is dangling
     */
    Value *dangling;

    /*
     * Result counter used for the limit operator
     */
    Value *res_counter;

    /*
    * Temporary loop variables, i.e., counter and current ids
    */
    Value* plist_id;
    Value* loop_cnt;
    Value* max_cnt;
    Value* pitem;

    /*
     * Final block of scan, end of access path -> e.g. call Sort
     */
    BasicBlock *scan_nodes_end;

    std::vector<Value*> qr;
    std::vector<Value*> pqr;
    std::vector<Value*> sizes;

    /*
     * Query Register Value
     * Each producing operator inserts its result and the appropiate
     * type into a global list
     */
    struct QR_VALUE {
        // the actual register value
        Value *reg_val;

        // the actual type of the register value: node, rship, int, double, string, time, boolean
        int type;
    };

    /*
     * Register result values, each producing operator inserts its register
     */
    std::vector<QR_VALUE> reg_query_results;

    /*
     * List for Join ordering, e.g. left sub Join 1, right position 2
     */
    std::list<int> jids;

    /*
     * Extracting the Projection keys from the Projection Expression list
     * Used for string allocation
     */
    void get_projection_keys(qop_ptr prj_op) {
        auto prj = std::dynamic_pointer_cast<projection>(prj_op);
        for(auto & e : prj->prexpr_) {
            //projection->name_ = projection->name_ + e.key;
            project_string.push_back(e.key);
            switch(e.type) {
                case result_type::none:
                    prj->new_types.push_back(0);
                    break;
                case result_type::integer:
                    prj->new_types.push_back(2);
                    break;
                case result_type::double_t:
                    prj->new_types.push_back(3);
                    break;
                case result_type::string:
                    prj->new_types.push_back(4);
                    break;
                case result_type::time:
                    prj->new_types.push_back(5);
                    break;
                case result_type::date:
                    prj->new_types.push_back(6);
                    break;
                case result_type::boolean:
                case result_type::uint64:
                    continue; //TODO
            }
        }

        new_type_vec = prj->new_types;
    }

    /*
     * Calculating the maximal result of the given query expression
     */
    int query_length(qop_ptr op) {
        int count = 0;
        auto cur = op;
        while(cur->has_subscriber()) {
            if(cur->type_ != qop_type::filter || cur->type_ != qop_type::project || cur->type_ != qop_type::sort || cur->type_ != qop_type::collect)
                count++;

            if(cur->type_ == qop_type::project) {
                get_projection_keys(cur);
            }
                

            cur = cur->subscriber();
        }
        return count;
    }

    void link_operator(BasicBlock *next) {
        ctx.getBuilder().SetInsertPoint(prev_bb);
        ctx.getBuilder().CreateBr(next);
        ctx.getBuilder().SetInsertPoint(next);
    }

    Value *first;
    Value *last;
    Value *tx_ptr;
    Value *query_context;
    BasicBlock *next_pipeline;
    BasicBlock *df_finish_bb;

    bool profiling = false;
    bool pipelined = false;
    std::vector<std::string> pipelines;
    
    void extract_query_context(Value* context_arg) {
        gdb = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(context_arg, 0));
        first = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(context_arg, 1));
        last = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(context_arg, 2));
        tx_ptr = ctx.getBuilder().CreateLoad(ctx.getBuilder().CreateStructGEP(context_arg, 3));
    }

    AllocaInst *insert_alloca(Type *ty);

    bool finishing = false;
    Function *cur_pipeline;
    bool pipelined_finish = false;
    std::string query_id_str = "";
    
};


#endif //POSEIDON_CORE_CODEGEN_INLINE_HPP
