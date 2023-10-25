#include <llvm/Support/Error.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/InitLLVM.h>
#include <memory>
#include "qcompiler.hpp"
#include "qid_generator.hpp"
#include "codegen.hpp"
#include "thread_pool.hpp"

#include "proc/grouper.hpp"

#include "query_args.hpp"

using namespace std::placeholders;

std::unique_ptr<p_jit> qcompiler::initializeJitCompiler() {
    InitLLVM LLVM();
    InitializeNativeTarget();
	InitializeNativeTargetAsmPrinter();
	InitializeNativeTargetAsmParser();
    ExitOnError exitOnError;
    return std::make_unique<p_jit>(exitOnError);
}

std::mutex tp_mut;


qcompiler::qcompiler(query_ctx& ctx) 
    :
    jit_(initializeJitCompiler()),
    ctx_(PContext(ctx.gdb_)),  
    arg_counter(1u),
    query_ctx_(ctx) 
    {
        
    ctx_.getModule().setDataLayout(jit_->getDataLayout());

    auto insert_nd = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        auto n = (node*)ptr;
        return (*gdb)->get_node_description(n->id()).to_string();
    };

    auto insert_rd = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        auto r = (relationship*)ptr;
        return (*gdb)->get_rship_description(r->id()).to_string();
    };

    auto insert_int = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        return std::to_string(*ptr);
    };

    auto insert_double = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        return std::to_string(*reinterpret_cast<double*>(ptr));
    };

    auto insert_str = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        return str_result[*ptr];
    };

    auto insert_uint = [&] (graph_db_ptr* gdb, int *ptr) -> std::string {
        return std::to_string(uint_result[*ptr]);
    }; 

    con_map[0] = insert_nd;
    con_map[1] = insert_rd;
    con_map[2] = insert_int;
    con_map[3] = insert_double;
    con_map[4] = insert_str;
    con_map[8] = insert_uint;
}

qcompiler::~qcompiler() {
    if(compile_th.joinable())
        compile_th.join();
}

void qcompiler::generate(qop_ptr query, bool parallel) {
    parallel_ = parallel;
    cur_query_ = query;
    compile_th = std::thread(compile_task(*this, ctx_, *jit_.get(), query));
    if(!parallel) {
        compile_th.join();
    }
    /*codegen_visitor cv(ctx_);
    query->codegen(cv);
    jit.addModule(ctx_.moveModule());*/
}

void qcompiler::cleanup() {
    start_.clear();
    finish_.clear();
    qpipelines_.clear();
}

joiner * last_joiner;
void qcompiler::extract_arg(qop_ptr op) {
    std::cout << "Extract args" << std::endl;
    arg_counter = op->operator_id_;
    switch(op->type_) {
        case qop_type::scan: {
            auto s_op = std::dynamic_pointer_cast<scan_nodes>(op);
            std::cout << "Scan: " << op->operator_id_ << std::endl; 
            query_args.arg(arg_counter, s_op->label);
            break;
        }
        case qop_type::foreach_rship: {
            auto fe_op = std::dynamic_pointer_cast<foreach_relationship>(op);
            std::cout << "FE: " << arg_counter << std::endl; 
            query_args.arg(arg_counter, fe_op->label);
            break;
        }
        case qop_type::expand: {
            auto exp_op = std::dynamic_pointer_cast<expand>(op);
            if(!exp_op->label.empty())
                query_args.arg(arg_counter, exp_op->label);
            break;
        }
        case qop_type::node_has_label: {
            auto hl_op = std::dynamic_pointer_cast<node_has_label>(op);
            std::cout << "Label: " << arg_counter << std::endl; 
            query_args.arg(arg_counter, hl_op->label);
            break;
        }
        case qop_type::filter: {
            //TODO: all filter arguments
            break;
        }
        case qop_type::create: {
            //TODO: all create arguments
            break;
        }
        case qop_type::cross_join: {
            last_joiner = new joiner();
            query_args.arg(arg_counter, last_joiner);
            break;
        }
        case qop_type::hash_join:
        case qop_type::left_join:
        case qop_type::nest_loop_join: {
            last_joiner = new joiner();
            query_args.arg(arg_counter, last_joiner);
            break;
        }
        case qop_type::end: {
            query_args.arg(arg_counter, last_joiner);
            break;
        }
        case qop_type::group_by: {
            query_args.arg(arg_counter, new grouper()); //TODO: allocation
            query_args.arg(arg_counter, new grouper());
            break;
        }
        case qop_type::collect: {
            std::cout << "Collect: " << op->operator_id_ << std::endl; 
            auto cop = std::dynamic_pointer_cast<collect_result>(op);
            query_args.arg(arg_counter, &cop->results_);
            break;
        }
        case qop_type::limit:
        // inline argument, nothing to do here
        case qop_type::project:
        case qop_type::aggregate:
        case qop_type::sort:
        case qop_type::any:
        case qop_type::none:
        default:
            return;
    }
}


void qcompiler::run(arg_builder & args, bool cleanup_query) {

    query_ctx_.gdb_->begin_transaction();
    current_transaction_ = current_transaction();
    if(parallel_) {
        unsigned int op_start = 1;
        arg_builder args;
        //interprete_visitor iv(graph_, args, rs);
        //cur_query_->codegen(iv, op_start, true);
        //iv.start();
        compile_th.join();
    }
    
    int i = 0;


    auto start_idx = start_.size()-1;
    auto last = query_ctx_.gdb_->get_nodes()->num_chunks();
    query_context qtx = {&query_ctx_, 0, last, current_transaction_, args.args.data()};
    qtx.ctx = &query_ctx_;
    *qtx.args = *args.args.data();
    qtx.tx = current_transaction_;

    for(i = start_idx; i >= 0; i--) {
        start_[i](&qtx, args.args.data());
        //finish_[i](&qtx, args.args.data(), rs);
       for(auto finish : qpipelines_[i]) {
           finish(&qtx, args.args.data());
       }
    }

    query_ctx_.gdb_->commit_transaction();

    auto bench = qtx.profiling_time;

    std::map<int, int> operator_exec_times;
    for(auto & b : bench) {
        if(operator_exec_times.find(b.first) == operator_exec_times.end())
            operator_exec_times[b.first] = b.second;    
        else
            operator_exec_times[b.first] += b.second;
    }

    for(auto & t : operator_exec_times) {
        std::cout << "Operator: " << t.first << " : " << t.second << " ns" << std::endl;
    }

    if(cleanup_query)
        cleanup();
}

void qcompiler::run() {
    arg_extractor ax;
    unsigned i = 1;
    cur_query_->codegen(ax, i);

    run(ax.get_args());
}

std::map<int, finish_fct_type> qcompiler::finish_ = {};

void consumer_dummy(node &n) {

}
result_set rs2;
void qcompiler::run_parallel(arg_builder & args, unsigned thread_num) {
    bool interprete_started = false;
    scan_task::callee_ = scan_task::scan;
    //interprete_visitor iv(graph_, args, &rs2);
    auto start = 1u;
    //cur_query_->codegen(iv, start, false);

    auto pipeline_num = 1;

    for(auto pipeline = pipeline_num - 1; pipeline >= 0; pipeline--) {
        auto ctx = current_transaction_;
        std::thread interpreter_thread([&]{
            current_transaction_ = ctx;
            //iv.start();
        });
        std::thread t1([&] {
            if(compile_th.joinable())
                    compile_th.join();
           
            task_callee_ = [&](transaction_ptr tx, graph_db_ptr gdb, std::size_t first, std::size_t last, query_ctx::node_consumer_func consumer) {
                current_transaction_ = tx;
                query_context qtx = {&query_ctx_, first, last, tx, args.args.data()};
                start_[pipeline](&qtx, args.args.data());
            };

            scan_task::callee_ = task_callee_;
        });

        if(t1.joinable())
            t1.join();

        if(interpreter_thread.joinable())
           interpreter_thread.join();

        //if(has_join(cur_query_) && (pipeline != pipeline_num-1)) {
        //    graph_->parallel_nodes(consumer_dummy);
        //}
    }
}

void qcompiler::finish(arg_builder & args) {

    query_context qtx = {&query_ctx_, 0,  query_ctx_.gdb_->get_nodes()->num_chunks(), current_transaction_, args.args.data()};
    qtx.ctx = &query_ctx_;
    *qtx.args = *args.args.data();
    
    for(int i = start_.size()-1; i >= 0; i--) {
        finish_[i](&qtx, args.args.data());
    }
}

compile_task::compile_task(qcompiler & qeng, PContext &ctx, p_jit &jit, qop_ptr query) 
    : ctx_(ctx), jit_(jit), query_(query), qeng_(qeng) {}

int query_cnt = 0;

void compile_task::operator()() {
    //0. obtain the query identifier for the LLVM module
    qid_generator qd;
    unsigned int op_start = 1;
    unsigned int cg_start = 1;
    query_->codegen(qd, op_start, true);
    query_->dump(std::cout);

    auto internal_qid = qd.qid;//+std::to_string(query_cnt);
    query_cnt++;
    
    //1. generate LLVM IR code & compile into machine code
    bool inlined = true;
    codegen_inline_visitor cv(ctx_, internal_qid);
    query_->codegen(cv, cg_start, inlined);

    //2. add LLVM module with IR to jit for compilation
    ctx_.getModule().setModuleIdentifier(internal_qid);
    jit_.get_exit()(jit_.addModule(ctx_.moveModule()));

    ctx_.createNewModule();
    ctx_.getModule().setDataLayout(jit_.getDataLayout());
    
    //3. extract all operator names from query 
    //first function name == scan => start function
    auto query_id = 0;
    auto cur_op = query_;
    std::string finish_name = "default_finish_"+internal_qid;

    qeng_.qpipelines_.clear();
    qeng_.start_.clear();
    qeng_.finish_.clear();
    
    // obtain the function names for each pipeline and assign the ptrs to the appropriate map
    int i = 0;
    int pipe_id = -1;
    for(auto & s : cv.pipelines) {
        if(s.find("finish_") == 0) {
            auto ffct = jit_.getFunctionRaw<finish_fct_type>(s);
            if(ffct) {
                qeng_.finish_[i] = *ffct;
                qeng_.qpipelines_[pipe_id].push_back(*ffct);
                i++;
            }
        } else {
            auto start_fc = jit_.getFunctionRaw<start_ty>(s);
            if(start_fc) {
                qeng_.start_[i] = *start_fc;
                pipe_id++;
            }
        }       
    }
}

void qcompiler::execute(query_batch &queries) {
    for(int i = 0; i < queries.size(); i++) {
        auto p = queries.at(i).plan_head();
        generate(p, false);
        run();
    }
}