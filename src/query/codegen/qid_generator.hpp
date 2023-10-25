#ifndef POSEIDON_CORE_QID_GENERATOR_HPP
#define POSEIDON_CORE_QID_GENERATOR_HPP
#include "qop_visitor.hpp"

class qid_generator : public qop_visitor {
public:
    std::string qid;

    virtual void visit(std::shared_ptr<scan_nodes> op) override {
        qid += "S";
    }

    virtual void visit(std::shared_ptr<foreach_relationship> op) override {
        qid += "4";
    }

    virtual void visit(std::shared_ptr<projection> op) override {
        qid += "P";
    }

    virtual void visit(std::shared_ptr<expand> op) override {
        qid += "E";
    }

    virtual void visit(std::shared_ptr<filter_tuple> op) override {
        qid += "F";
    }

    virtual void visit(std::shared_ptr<collect_result> op) override {
        qid += "C";
    }

    virtual void visit(std::shared_ptr<limit_result> op) override {
        qid += "L";
    }

    virtual void visit(std::shared_ptr<end_pipeline> op) override {
        qid += "N";
    }

    virtual void visit(std::shared_ptr<create_node> op) override {
        qid += "A";
    }

    virtual void visit(std::shared_ptr<group_by> op) override {
        qid += "G";
    }

    /*virtual void visit(std::shared_ptr<store_op> op) {
        qid += "$";
    }*/
};
#endif //POSEIDON_CORE_QID_GENERATOR_HPP
