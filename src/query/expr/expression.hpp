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
#ifndef expression_hpp_
#define expression_hpp_

#include <string>
#include <memory>
#include <iostream>
#include "filter_visitor.hpp"

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

enum class FOP {
    EQ = 0,
    NEQ = 1,
    LE = 2,
    LT = 3,
    GE = 4,
    GT = 5,
    AND = 6,
    OR = 7,
    NOT = 8,
    PLUS = 9,
    MINUS = 10,
    MULT = 11,
    DIV = 12,
    MOD = 13
};

enum class FOP_TYPE {
    INT = 0,
    DOUBLE = 1,
    STRING = 2,
    DATE = 3,
    TIME = 4,
    OP = 5,
    BOOL_OP = 6,
    KEY = 7,
    UINT64 = 8
};

struct expression;
struct binary_expression;
struct binary_predicate;
struct fep_visitor;
struct number_token;
struct key_token;
struct str_token;
struct qparam_token;
struct time_token;
struct fct_call;
struct func_call;
struct eq_predicate;
struct le_predicate;
struct lt_predicate;
struct ge_predicate;
struct gt_predicate;
struct and_predicate;
struct or_predicate;
struct call_predicate;
using expr = std::shared_ptr<expression>;
using bin_expr = std::shared_ptr<binary_expression>;

struct expression_visitor {
protected:
    expression_visitor() = default;

public:
    virtual ~expression_visitor() = default;

    virtual void visit(int rank, std::shared_ptr<number_token> op) {}
    virtual void visit(int rank, std::shared_ptr<key_token> op) {}
    virtual void visit(int rank, std::shared_ptr<str_token> op) {}
    virtual void visit(int rank, std::shared_ptr<time_token> op) {}
    virtual void visit(int rank, std::shared_ptr<fct_call> op) {}
    virtual void visit(int rank, std::shared_ptr<func_call> op) {}
    virtual void visit(int rank, std::shared_ptr<eq_predicate> op) {}   
    virtual void visit(int rank, std::shared_ptr<le_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<lt_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<ge_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<gt_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<and_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<or_predicate> op) {}
    virtual void visit(int rank, std::shared_ptr<call_predicate> op) {}
};

struct expression {
    int opd_num;
    std::string name_;
    FOP_TYPE ftype_;
    FOP_TYPE rtype_; // result type - deduced from the operands and the operator

    virtual ~expression() {};

    virtual std::string dump() const = 0;

    virtual void accept(int rank, expression_visitor &vis) = 0;

    std::string fop_str(FOP fop) const;

    FOP_TYPE result_type() const { return rtype_; }
};

struct number_token : public expression, public std::enable_shared_from_this<number_token> {
    int ivalue_;
    uint64_t lvalue_;
    double dvalue_;

    number_token(int value = 0);
    number_token(uint64_t value);
    number_token(double value);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr Int(int value = 0) { return std::make_shared<number_token>(value); }
inline expr UInt64(uint64_t value) { return std::make_shared<number_token>(value); }
inline expr Float(double value) { return std::make_shared<number_token>(value); }

struct key_token : public expression, std::enable_shared_from_this<key_token> {
    std::string key_;
    unsigned qr_id_; 

    key_token(unsigned qr_id, std::string key);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr Key(unsigned qr_id, std::string value = "") { return std::make_shared<key_token>(qr_id, value); }

struct str_token : public expression, std::enable_shared_from_this<str_token> {
    std::string str_;

    str_token(std::string str);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr Str(std::string value = 0) { return std::make_shared<str_token>(value); }

struct qparam_token : public expression, std::enable_shared_from_this<qparam_token> {
    std::string str_;

    qparam_token(std::string str);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr QParam(std::string value = 0) { return std::make_shared<qparam_token>(value); }

struct time_token : public expression, std::enable_shared_from_this<time_token> {
    boost::posix_time::ptime time_;

    time_token(boost::posix_time::ptime time);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};

inline expr Time(boost::posix_time::ptime time) { return std::make_shared<time_token>(time); }

struct fct_call : public expression, std::enable_shared_from_this<fct_call> {
    using fct_int_t = bool (*)(int*);
    using fct_uint_t = bool (*)(uint64_t*);    
    using fct_str_t = bool (*)(char*);  
    fct_int_t fct_int_;
    fct_uint_t fct_uint_;
    fct_str_t fct_str_;
    FOP_TYPE fct_type_;

    fct_call(fct_int_t fct);
    fct_call(fct_uint_t fct);
    fct_call(fct_str_t fct);

    std::string dump() const override;

    void accept(int rank, expression_visitor &fep) override;
};


inline expr Fct(fct_call::fct_int_t fct) { return std::make_shared<fct_call>(fct); }

#endif 