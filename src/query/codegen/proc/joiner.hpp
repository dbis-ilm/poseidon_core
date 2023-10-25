#ifndef POSEIDON_CORE_JOINER_HPP
#define POSEIDON_CORE_JOINER_HPP

#include <vector>
#include "global_definitions.hpp"

using input = std::vector<qr_tuple>;
using id_input = std::vector<offset_t>;


class base_joiner {

public:
    base_joiner() = default;

    virtual void insert_tuple(qr_tuple *qr) = 0;
    
    virtual qr_tuple * get_tuple_at(int id) {
        return &tuples_.at(id);
    }

    virtual int get_input_size() {
        return tuples_.size();
    }

    input tuples_;
};

class cross_joiner : public base_joiner {
    std::mutex materialize_mutex;
public:
    virtual void insert_tuple(qr_tuple *qr) override {
        std::lock_guard<std::mutex> lck(materialize_mutex);
        tuples_.push_back(*qr);
        qr->clear();
    }
};

class nested_loop_joiner : public base_joiner {
    id_input id_inputs_;
    std::size_t id_pos_;
    std::mutex materialize_mutex;
public:
    nested_loop_joiner(std::size_t id) : id_pos_(id) {}

    virtual void insert_tuple(qr_tuple *qr) override {
        std::lock_guard<std::mutex> lck(materialize_mutex);
        auto n = boost::get<node*>(qr->at(id_pos_));
        id_inputs_.push_back(n->id());
        tuples_.push_back(*qr);
        qr->clear();
    }

    offset_t get_input_id(std::size_t pos) {
        return id_inputs_[pos];
    }
};


class joiner {
public:
    joiner() = default;
    
    std::map<int, qr_tuple> mat_tuple_;
    std::map<int, input> rhs_input_;
    std::map<int, id_input> id_input_;

    std::map<int, input> rhs_hash_input_[10];
    std::map<int, id_input> id_hash_input_[10];

    std::mutex materialize_mutex;
    void materialize_rhs(int jid, qr_tuple *qr);

    void materialize_rhs_id(int jid, offset_t id);

    void materialize_rhs_hash_join(int jid, int bucket, qr_tuple *qr);
    void materialize_rhs_id_hash_join(int jid, int bucket, int id);

    int get_input_size(int jid, int bucket);
    int get_input_id(int jid, int bucket, int idx);
    qr_tuple * get_query_result(int jid, int bucket, int idx);
};


#endif //POSEIDON_CORE_JOINER_HPP
