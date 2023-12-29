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

#include "qop_projection.hpp"
#include "qop_builtins.hpp"

projection::projection(const expr_list &exprs) : exprs_(exprs) {
  type_ = qop_type::project;
  init_expressions();
}

projection::projection(const expr_list &exprs, udf_list& ul) : exprs_(exprs), udfs_(ul) {
  type_ = qop_type::project;
  init_expressions();
}

void projection::init_expressions() {
    if (exprs_.empty())
        return;

    pfuncs_.resize(exprs_.size());

    auto udf_idx = 0u;
    for (auto i = 0u; i < exprs_.size(); i++) {
        auto& ex = exprs_[i];
        accessed_.insert(ex.idx);
        // TODO: we should work with codes only
        // dcode_t prop_code = ex.pname.empty() ? UNKNOWN_CODE : dct->lookup_string(ex.pname);
        auto pname = ex.pname;
        //spdlog::info("lookup code for '{}':{}", ex.pname, prop_code);
        //if (ex.pfunc != prj::forward  && prop_code == UNKNOWN_CODE)
        //    spdlog::info("unknown string '{}'", ex.pname);

        switch(ex.pfunc) {
            case prj::forward:
                pfuncs_[i] = prj_func { ex.idx, [](query_ctx&, const query_result& qr) { return qr; }};
                break;
            case prj::int_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::int_property(qr, pname); }};
                break;
            case prj::int_property_as_datestring:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::int_to_datestring(builtin::int_property(qr, pname)); }};
                break;
            case prj::int_property_as_datetimestring:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::int_to_dtimestring(builtin::int_property(qr, pname)); }};
                break;            
            case prj::uint64_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::uint64_property(qr, pname); }};
                break;
            case prj::double_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::double_property(qr, pname); }};
                break;
            case prj::string_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::string_property(qr, pname); }};
                break;
            case prj::ptime_property:
            case prj::date_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::ptime_property(qr, pname); }};
                break;
            /*
            case prj::date_property:
                pfuncs_[i] = prj_func { ex.idx, [pname](query_ctx&, const query_result& qr) { 
                    return builtin::pr_date(qr, pname); }};
                break;
            */
            case prj::label:
                pfuncs_[i] = prj_func { ex.idx, [](query_ctx&, const query_result& qr) { 
                    return builtin::get_label(qr); }};
                break;
            case prj::function:
                {
                    auto fv = udfs_[udf_idx];
                    if (fv.which() == 0) {
                        auto func = boost::get<user_defined_func1>(fv);
                       pfuncs_[i] = prj_func { ex.idx, [=](query_ctx& ctx, const query_result& qr) { return func(ctx, qr); }};
                    }
                    else {
                        // TODO: handle UDFs with more params
                    }
                    udf_idx++;
                    break;
                }
            default:
                // TODO
                break;
        } 
    }
}

void projection::dump(std::ostream &os) const {
  os << "project([";
  for (auto &ex : exprs_) {
    os << " $" << ex.idx;
    if (!ex.pname.empty())
      os << "." << ex.pname;
    switch(ex.pfunc) {
        case prj::int_property:
            os << ":int";
            break;
        case prj::uint64_property:
            os << ":uint64";
            break;
        case prj::string_property:
            os << ":string";
            break;
        case prj::date_property:
            os << ":datetime";
            break;
        case prj::int_property_as_datestring:
            os << ":int->date";
            break;
        case prj::int_property_as_datetimestring:
            os << ":int->datetime";
            break;
        default:
            break;
    }
  }
  os << " ]) - " << PROF_DUMP;
}

void projection::process(query_ctx &ctx, const qr_tuple &v) {
    PROF_PRE;
    
    // First, we build a list of all node_/rship_description objects which appear
    // in the query result. This list is used as a cache for property functions.
    std::vector<query_result> v_cached(v.size());
    for (auto i = 0u; i < v.size(); i++) 
        v_cached[i] = v[i];

    for (auto av : accessed_) {
        if (v[av].which() == node_ptr_type) {
            auto n = qv_get_node(v[av]);
            v_cached[av] = ctx.gdb_->get_node_description(n->id());
        } else if (v[av].which() == rship_ptr_type) {
            auto r = qv_get_relationship(v[av]);
            v_cached[av] = ctx.gdb_->get_rship_description(r->id());
        }
    }

    // Then, we process all projection functions...
    qr_tuple res(pfuncs_.size());
    for (auto i = 0u; i < pfuncs_.size(); i++) {
        try {
            auto& pf = pfuncs_[i];
            auto& qv = v_cached[pf.idx];
            res[i] = pf.func(ctx, qv);
        } catch (unknown_property& exc) { }
    }
    consume_(ctx, res);
    PROF_POST(1);
 }