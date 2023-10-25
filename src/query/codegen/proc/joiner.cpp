#include "joiner.hpp"

void joiner::materialize_rhs(int jid, qr_tuple *qr) {
    std::lock_guard<std::mutex> lck(materialize_mutex);
    rhs_input_[jid].push_back(*qr);
    qr->clear();;
}

void joiner::materialize_rhs_id(int jid, offset_t id) {
    std::lock_guard<std::mutex> lck(materialize_mutex);
    id_input_[jid].push_back(id);
}

void joiner::materialize_rhs_hash_join(int jid, int bucket, qr_tuple *qr) {
    std::lock_guard<std::mutex> lck(materialize_mutex);
    rhs_hash_input_[jid][bucket].push_back(*qr);
}

void joiner::materialize_rhs_id_hash_join(int jid, int bucket, int id) {
    std::lock_guard<std::mutex> lck(materialize_mutex);
    id_hash_input_[jid][bucket].push_back(id);
}

int joiner::get_input_size(int jid, int bucket) {
    return id_hash_input_[jid][bucket].size();
}

int joiner::get_input_id(int jid, int bucket, int idx) {
    return id_hash_input_[jid][bucket][idx];
}

qr_tuple * joiner::get_query_result(int jid, int bucket, int idx) {
    return &rhs_hash_input_[jid][bucket][idx];
}