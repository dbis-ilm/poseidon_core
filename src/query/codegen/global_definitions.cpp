#include "global_definitions.hpp"

thread_local std::map<int, std::string> str_result;
thread_local std::map<int, uint64_t> uint_result;
thread_local std::map<int, boost::posix_time::ptime> time_result;
thread_local int str_res_ctr = 0;

thread_local std::map<int, node_description> descs;
thread_local std::map<int, rship_description> rdescs;
thread_local qr_tuple mat_tuple;

thread_local qr_tuple rec;
thread_local std::vector<relationship*> fev_rship_list;
thread_local std::vector<relationship*>::iterator fev_list_iter;
thread_local std::string grpkey_buffer;

query_time_point get_now() {
    return std::chrono::high_resolution_clock::now();
}

void add_time_diff(query_context* qtx, int op_id, query_time_point t1, query_time_point t2) {
    qtx->add_time(op_id, t1, t2);
}

node_list<item_vec>::range_iterator *get_vec_begin(node_list<item_vec> *vec, size_t first, size_t last) {
    return vec->range_ptr(first, last);
}

node_list<item_vec>::range_iterator *get_vec_next(node_list<item_vec>::range_iterator *it) {
    return &it->operator++();
}

int ixc = 0;
 bool vec_end_reached(node_list<item_vec> &vec, node_list<item_vec>::range_iterator *it) {
    return !it->operator bool();
}

relationship_list<item_vec>::iterator get_vec_begin_r(relationship_list<item_vec> &vec) {
    return vec.as_vec().begin();
}

relationship_list<item_vec>::iterator *get_vec_next_r(relationship_list<item_vec>::iterator *it) {
    return &it->operator++();
}

bool vec_end_reached_r(relationship_list<item_vec> &vec, relationship_list<item_vec>::iterator it) {
    return !(it != vec.as_vec().end());
}

 dcode_t dict_lookup_label(query_ctx *ctx, char *label) {
    return ctx->gdb_->get_code(label);
}

node *get_node_from_it(node_list<item_vec>::range_iterator *it) {
    return &it->operator*();
}

relationship *get_rship_from_it(relationship_list<item_vec>::iterator *it) {
    return &it->operator*();
}

node_list<item_vec>::vec *gdb_get_nodes(query_ctx *ctx) {
    return &ctx->gdb_->get_nodes()->as_vec();
}

relationship_list<item_vec>::vec *gdb_get_rships(query_ctx *ctx) {
    return &ctx->gdb_->get_relationships()->as_vec();
}

 void test_ints(uint64_t a, uint64_t b) {
    std::cout << "A: " << a << " B: " << b << "EQ: " << (a == b) << std::endl;
}

 relationship *rship_by_id(query_ctx *ctx, offset_t id) {
    return &ctx->gdb_->rship_by_id(id);
}

 node *node_by_id(query_ctx *ctx, offset_t id) {
    return &ctx->gdb_->node_by_id(id);
}

 dcode_t gdb_get_dcode(query_ctx *ctx, char *property) {
    return ctx->gdb_->get_code(property);
}

 const property_set *pset_get_item_at(query_ctx *ctx, offset_t id) {
    return &ctx->gdb_->get_node_properties()->get(id);
}

std::map<int, std::function<std::string(graph_db_ptr*, int*)>> con_map;

 xid_t get_tx(transaction_ptr tx) {
    return tx->xid();
}

 node * get_valid_node(query_ctx *ctx, node * n, transaction_ptr tx) {
    return &ctx->gdb_->get_valid_node_version(*n, current_transaction_->xid());
}

 char* get_str_property(const properties_t &p, const std::string &key) {
    auto it = p.find(key);
    if (it == p.end()) {
        spdlog::info("unknown property: {}", key);
        throw unknown_property();
    }
    auto prop_heap = new std::string;
    *prop_heap = std::any_cast<std::string>(it->second);
    auto ret = const_cast<char*>(reinterpret_cast<const char*>(prop_heap->c_str()));
    return ret;
}

void apply_pexpr_node(query_ctx *ctx, const char *key, result_type val_type, int *qr, int *ret) {
    // cast the query result to a node
    auto n = (node*)qr;
    
    // try to find the description in the thread local result memory
    // if it is not present, generate the description and write it to the thread local memory
    if(descs.find(n->id()) == descs.end())
        descs[n->id()] = ctx->gdb_->get_node_description(n->id());
    auto nd = descs[n->id()];

    switch(val_type) {
        case result_type::integer: { // an integer type can be directly written to the memory
            *ret = get_property<int>(nd.properties, key).value();
            break;
        }
        case result_type::uint64: { // store the uint64 result in thread local memory and return the result id to the caller
            uint_result[str_res_ctr] = get_property<uint64_t>(nd.properties, key).value(); 
            *ret = str_res_ctr++;
            break;
        }
        case result_type::string: { // store the string in thread local memory and return the result id to the caller
            str_result[str_res_ctr] = std::any_cast<std::string>(nd.properties[std::string(key)]);
            *ret = str_res_ctr++;
            break;
        }
        case result_type::time: // TODO: wip
        case result_type::date: { // store the time object in thread local memory and return the result id to the caller
            //
            time_result[str_res_ctr] = get_property<boost::posix_time::ptime>(nd.properties, key).value();
            *ret = str_res_ctr++;
            break;
        }
        case result_type::double_t:
        case result_type::boolean:
        case result_type::node:
        case result_type::relationship:
        default:
            break;   
    }
}

void apply_pexpr_rship(query_ctx *ctx, const char *key, result_type val_type, int *qr, int *ret) {
    auto r = (relationship*)qr;
    if(rdescs.find(r->id()) == rdescs.end())
        rdescs[r->id()] = ctx->gdb_->get_rship_description(r->id());
    auto rd = rdescs[r->id()];

    switch(val_type) {
        case result_type::integer: { // an integer type can be directly written to the memory
            *ret = get_property<int>(rd.properties, key).value();
            break;
        }
        case result_type::uint64: { // store the uint64 result in thread local memory and return the result id to the caller
            break;
        }
        case result_type::string: { // store the string in thread local memory and return the result id to the caller
            str_result[str_res_ctr] = std::any_cast<std::string>(rd.properties[std::string(key)]);;
            *ret = str_res_ctr++;
            break;
        }
        case result_type::time: // TODO: wip
        case result_type::date: { // store the time object in thread local memory and return the result id to the caller
            //time_result[str_res_ctr] = to_iso_extended_string(get_property<boost::posix_time::ptime>(rd.properties, key).value());
            time_result[str_res_ctr] = get_property<boost::posix_time::ptime>(rd.properties, key).value();
            *ret = str_res_ctr++;
            break;
        }
        case result_type::double_t:
        case result_type::boolean:
        case result_type::node:
        case result_type::relationship:
        default:
            break;   
    }
}

std::mutex prj_mutex;

 void apply_pexpr(query_ctx *ctx, const char *key, result_type val_type, int *qr, int idx, std::vector<int> types, int *ret) {
    std::lock_guard<std::mutex> lock(prj_mutex);
    if(types.at(idx) == 0) { // is node
        apply_pexpr_node(ctx, key, val_type, qr, ret);

    } else if(types.at(idx) == 1) { // is rship
        apply_pexpr_rship(ctx, key, val_type, qr, ret);
    } else if(types.at(idx) == 2) { // is rship
        ret = qr;
    }
}

 const char* lookup_dc(query_ctx *ctx, dcode_t dc) {
    return ctx->gdb_->get_string(dc);
}


extern "C" node* create_node_func(query_ctx *ctx, char *label, properties_t *props) {
    auto node_id = ctx->gdb_->add_node(std::string(label), *props, true);
    return &ctx->gdb_->node_by_id(node_id);
}

extern "C" relationship* create_rship_func(query_ctx *ctx, char *label, node *n1, node *n2, properties_t *props) {
    auto rid = ctx->gdb_->add_relationship(n1->id(), n2->id(), label, *props, true);
    return &ctx->gdb_->rship_by_id(rid);
}


std::map<std::thread::id, qr_tuple> tp_m;
int tcnt = 0;
std::mutex mat_reg_mut;
 void mat_reg_value(query_ctx *ctx, int *reg, int type) {
    std::lock_guard<std::mutex> lck(mat_reg_mut);
    
    auto & tp = tp_m[std::this_thread::get_id()];
    {
        if(type == 2) {
            int res = std::stoi(con_map[type](&ctx->gdb_, reg));
            tp.push_back(res);
        } else if(type == 3) {
            tp.push_back(*(double*)reg);
        } else if(type == 5 || type == 6) {
            tp.push_back(time_result[*reg]);
        } else if(type == 8) {
            tp.push_back(uint64_t(std::stoull(con_map[type](&ctx->gdb_, reg))));
        } else if(type == 10) {
            //auto x = *(query_result*)reg;
            //auto str = boost::get<std::string>(x);
            std::cout << "collect projection: " << 2 << std::endl;
            tp.push_back("testc");
        } else if(type == 90) { // special handling for direct node access when grouping
            tp.push_back((node*)reg);
        } else if(type == 91) { // special handling for direct rship access when grouping
            tp.push_back((relationship*)reg);
        } else if(type == 92) {
            tp.push_back(*reg);
        } else if(type == 93) {
            tp.push_back(*(double*)reg);
        } else {
            tp.push_back(con_map[type](&ctx->gdb_, reg));
        }
    }   
}

query_result tl_qres;
void append_to_tuple(query_result qr) {
    auto & tp = tp_m[std::this_thread::get_id()];
    tp.push_back(tl_qres);
}

qr_tuple &get_qr_tuple() {
    auto & tp = tp_m[std::this_thread::get_id()];
    return tp;
}

std::mutex ct_mut;
void collect_tuple(query_ctx *ctx, result_set *rs, bool print) {
std::lock_guard<std::mutex> lck(mat_reg_mut);
auto & tp = tp_m[std::this_thread::get_id()];
{
    rs->append(tp);
    /*if(print) {
        std::cout << "{";
        auto my_visitor = boost::hana::overload(
            [&](node *n) { os << ctx->gdb_->get_node_description(*n);  },
            [&](relationship *r) {  os << ctx->gdb_->get_relationship_label(*r);  },
            [&](int i) { std::cout << i; }, [&](double d) { std::cout << d; },
            [&](const std::string &s) { std::cout << s; }, [&](uint64_t ll) { std::cout << ll; },
            [&](null_t n) { std::cout << "NULL"; },
            [&](boost::posix_time::ptime dt) { std::cout << dt; }); 

        auto i = 0u;
        for (const auto &qr : tp) {
            boost::apply_visitor(my_visitor, qr);
            if (++i < tp.size())
                std::cout << ", ";
        }
        std::cout << "}" << std::endl;
    } */
    tp.clear();
}
}

qr_tuple *obtain_mat_tuple() {
    //auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    //return &joiner::mat_tuple_[tid];
    auto & tp = tp_m[std::this_thread::get_id()];
    return &tp;
}

query_result *reg_to_qres(int *reg) {
    tl_qres = query_result((node*)reg);
    //tl_qres = (node*)reg;
    return &tl_qres;
}

void * node_to_description(query_ctx *ctx) {
    auto n = boost::get<node*>(tl_qres);
    tl_qres = ctx->gdb_->get_node_description(n->id());
    return (void*)&tl_qres;
}


 void mat_node(node *n, qr_tuple *qr) {
    qr->push_back(n);
}

 void mat_rship(relationship *r, qr_tuple *qr) {
    qr->push_back(r);
}

 void collect_tuple_join(base_joiner *j, int jid, qr_tuple *qr) {
    j->insert_tuple(qr);
}

 qr_tuple *get_join_tp_at(base_joiner *j, int jid, int pos) {
    return &j->tuples_.at(pos);
}

 node *get_node_res_at(qr_tuple *tuple, int pos) {
    return boost::get<node*>(tuple->at(pos));
}

 relationship *get_rship_res_at(qr_tuple *tuple, int pos) {
    auto x = boost::get<relationship*>(tuple->at(pos));
    return x;
}

 int get_mat_res_size(base_joiner *j, int jid) {
    return j->tuples_.size();
}

 node *index_get_node(query_ctx *ctx, char *label, char *prop, uint64_t value) {
    auto idx = ctx->gdb_->get_index(std::string(label), std::string(prop));

    node *n_ptr;
    auto found = false;
    ctx->gdb_->index_lookup(idx, value, [&](auto& n) {
        found = true;
        n_ptr = &n;
    });
    return n_ptr;
}

 int count_potential_o_hop(query_ctx *ctx, offset_t rship_id) {
    auto cnt = 0;
    while(rship_id != UNKNOWN) {
        cnt++;
        rship_id = ctx->gdb_->rship_by_id(rship_id).next_src_rship;
    }
    return cnt;
}


 std::list<std::pair<relationship::id_t, std::size_t>> retrieve_fev_queue() {
    return std::list<std::pair<relationship::id_t, std::size_t>>();
}

 void insert_fev_rship(std::list<std::pair<relationship::id_t, std::size_t>> &queue, relationship::id_t rid, std::size_t hop) {
    queue.push_back(std::make_pair(rid, hop));
}

 bool fev_queue_empty(std::list<std::pair<relationship::id_t, std::size_t>> &queue) {
    return queue.empty();
}


 void foreach_from_variable_rship(query_ctx *ctx, dcode_t lcode, node *n, std::size_t min, std::size_t max) {
    ctx->foreach_variable_from_relationship_of_node(*n, lcode, min, max, [&](relationship &r) {
        fev_rship_list.push_back(&r);
    });
    fev_list_iter = fev_rship_list.begin();
}

  relationship *get_next_rship_fev() {
    auto rship = *fev_list_iter;
    fev_list_iter++;
    return rship;
}

 bool fev_list_end() {
    auto is_end = fev_list_iter == fev_rship_list.end();
    if(is_end)
        fev_rship_list.clear();
    return is_end;
}
std::set<unsigned> pos_set;
 void get_node_grpkey(node* n, unsigned pos) {
    pos_set.insert(pos);
    grpkey_buffer += std::to_string(n->id());
}

 void get_rship_grpkey(relationship* r, unsigned pos) {
    pos_set.insert(pos);
    grpkey_buffer += std::to_string(r->id());
}

 void get_int_grpkey(int i, unsigned pos) {
    pos_set.insert(pos);
    grpkey_buffer += std::to_string(i);
}

 void get_double_grpkey(int* d_ptr, unsigned pos) {
    //TODO: implement double handling
}

 void get_string_grpkey(int* str_ptr, unsigned pos) {
    pos_set.insert(pos);
    grpkey_buffer += str_result[*str_ptr];
}

 void get_time_grpkey(int* time_ptr, unsigned pos) {
    pos_set.insert(pos);
    //std::string dt = time_result[*time_ptr];
    //grpkey_buffer += dt.substr(0, dt.find("-"));
}

 void add_to_group(grouper *g) {
    auto & tp = tp_m[std::this_thread::get_id()];
    g->add_to_group(grpkey_buffer, tp, pos_set);
    grpkey_buffer = "";
    pos_set.clear();
    tp.clear();
}

 void finish_group_by(grouper *g, result_set* rs) {
    g->finish(rs);
}

 void clear_mat_tuple() {
    auto & tp = tp_m[std::this_thread::get_id()];
    tp.clear();
}

 qr_tuple* grp_demat_at(grouper *g, int index) {
    return g->demat_tuple(index);
}

 int int_to_reg(qr_tuple* qr, int pos) {
    return boost::get<int>(qr->at(pos));
}

 int str_to_reg(qr_tuple* qr, int pos) {
    str_result[str_res_ctr] = boost::get<std::string>(qr->at(pos));
    return str_res_ctr++;
}

int double_to_reg(qr_tuple* qr, int pos) {
    return boost::get<double>(qr->at(pos));
}

 node* node_to_reg(qr_tuple* qr, int pos) {
    return boost::get<node*>(qr->at(pos));
}

 relationship* rship_to_reg(qr_tuple* qr, int pos) {
    return boost::get<relationship*>(qr->at(pos));
}

 int time_to_reg(qr_tuple* qr, int pos) { 
    time_result[str_res_ctr] = boost::get<boost::posix_time::ptime>(qr->at(pos));
    return str_res_ctr++;
}

 int get_grp_rs_count(grouper *g) {
    return g->get_rs_count();
}

 void init_grp_aggr(grouper *g) {
    g->init_grp_aggr();
}

 int get_group_count(grouper *g) {
    auto gcnt = g->get_group_cnt();
    return gcnt;
}

 int get_total_group_count(grouper *g) {
    return g->get_total_group_cnt();
}   

 int get_group_sum_int(grouper *g, int pos) {
    return g->get_group_sum_int(pos);
}

 double get_group_sum_double(grouper *g, int pos) {
    return g->get_group_sum_double(pos);
}

 uint64_t get_group_sum_uint(grouper *g, int pos) {
    return g->get_group_sum_uint(pos);
}

void insert_join_id_input(nested_loop_joiner *j, int jid, offset_t id) {
    //j->insert_tuple(jid, id);
}

offset_t get_join_id_at(nested_loop_joiner *j, int jid, int pos) {
    return j->get_input_id(pos);
}

void collect_tuple_hash_join(joiner *j, int jid, int remainder, qr_tuple *qr) {
    j->materialize_rhs_hash_join(jid, remainder, qr);
}

void insert_join_bucket_input(joiner *j, int jid, int remainder, int id) {
    j->materialize_rhs_id_hash_join(jid, remainder, id);
}

int get_hj_input_size(joiner *j, int jid, int bucket) {
    return j->get_input_size(jid, bucket);
}
int get_hj_input_id(joiner *j, int jid, int bucket, int idx) {
    return j->get_input_id(jid, bucket, idx);
}

qr_tuple * get_query_result(joiner *j, int jid, int bucket, int idx) {
    return j->get_query_result(jid, bucket, idx);
}

int node_has_property(query_ctx *ctx, node *n, char *property) {
    auto nd = ctx->gdb_->get_node_description(n->id());
    return nd.has_property(std::string(property));
}

int rship_has_property(query_ctx *ctx, relationship *r, char *property) {
    auto rd = ctx->gdb_->get_rship_description(r->id());
    return rd.has_property(std::string(property));
}

void apply_has_property(int has_properties_cnt, char *then_res, char *else_res, int *result) {
    if(has_properties_cnt > 0) {
        *result = str_res_ctr;
        str_result[str_res_ctr++] = std::string(then_res);
    } else {
        *result = str_res_ctr;
        str_result[str_res_ctr++] = std::string(else_res);
    }
}

void end_notify(result_set *rs) {
    rs->notify();
}

int pl = 0;
void print_int(int i) { std::cout << i << std::endl; }