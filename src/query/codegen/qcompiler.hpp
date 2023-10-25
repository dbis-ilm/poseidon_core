#ifndef POSEIDON_CORE_QUERY_ENGINE_HPP
#define POSEIDON_CORE_QUERY_ENGINE_HPP

#include "proc/joiner.hpp"
#include "proc/grouper.hpp"
#include "qexmode.hpp"
#include "jit/p_jit.hpp"
#include "jit/p_context.hpp"

class base_op;

class qcompiler;

struct compile_task {
    compile_task(qcompiler & qeng, PContext &ctx, p_jit &jit, qop_ptr query);

    void operator()();

    PContext &ctx_;
    p_jit &jit_;
    qop_ptr query_;
    qcompiler &qeng_;
};

/*
    arg_builder is a helper structure for the compiled query code.
    It is used to store arguments of operators, e.g., labels, into easy
    accessible memory regions, according to their position in the query
    pipeline.
*/
struct arg_builder {
    std::vector<uint64_t> int_args;
    std::vector<std::string> string_args;
    std::vector<properties_t> prop_args;
    std::vector<uint64_t*> args;

    arg_builder() : int_args(64), string_args(64), prop_args(64), args(64) {}
    void arg(int op_id, std::string arg) {
        string_args[op_id] = arg;
        args[op_id] = (uint64_t*)(string_args[op_id].c_str());
    }
    void arg(int op_id, uint64_t arg) {
        int_args[op_id] = arg;
        args[op_id] = (uint64_t*)&(int_args[op_id]);
    }
    void arg(int op_id, grouper *g) {
        args[op_id] = (uint64_t*)g;
    }

    void arg(int op_id, joiner *j) {
        args[op_id] = (uint64_t*)j;
    }

    void arg(int op_id, base_joiner *j) {
        args[op_id] = (uint64_t*)j;
    }

    void arg(int op_id, cross_joiner *j) {
        args[op_id] = (uint64_t*)j;
    }

    void arg(int op_id, result_set *rs) {
        args[op_id] = (uint64_t*)rs;
    }


    void arg(int op_id, properties_t & props) {
        prop_args[op_id] = props;
        args[op_id] = (uint64_t*)&(prop_args[op_id]);
    }

};

class qcompiler : public qexmode {
    friend struct compile_task;

public: 
    qcompiler(query_ctx& ctx);
    ~qcompiler();

    /**
     * exec executes compiled queries 
     */
    void execute(query_batch &queries) override;

    /**
     * generate compiles a query into machine code. 
     */
    void generate(qop_ptr query, bool parallel = true);

    /**
     * run executes the compiled query with the given arguments 
     */
    void run();
    void run(arg_builder & args, bool cleanup_query = true);
    void finish(arg_builder & args);
    void run_parallel(arg_builder & args, unsigned thread_num);

    void cleanup();

    static std::unique_ptr<p_jit> initializeJitCompiler();
private:
    void extract_arg(qop_ptr op);

    PContext ctx_;
    std::unique_ptr<p_jit> jit_;
    std::thread compile_th;

    transaction_ptr cur_tx_;
    qop_ptr cur_query_;
    arg_builder query_args;
    unsigned arg_counter;

    query_ctx query_ctx_;

    bool parallel_;

    std::map<int, start_ty> start_;
    static std::map<int, finish_fct_type> finish_;
    std::map<int, std::vector<finish_fct_type>> qpipelines_;

    std::function<void(transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, query_ctx::node_consumer_func consumer)> task_callee_;

};

#endif //POSEIDON_CORE_QUERY_ENGINE_HPP
