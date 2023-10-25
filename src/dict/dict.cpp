/*
 * Copyright (C) 2019-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include <iostream>
#include "spdlog/spdlog.h"
#include "dict.hpp"

dict::dict(bufferpool& bpool, const std::string& prefix, uint32_t init_pool_size) : bpool_(bpool) {
    dict_file_ = std::make_shared<paged_file>();
    dict_file_->open(prefix == "" ? "dict.db" : prefix + "/dict.db", DICT_FILE_ID);
    bpool_.register_file(DICT_FILE_ID, dict_file_);
    pool_ = std::make_shared<paged_string_pool>(bpool_, DICT_FILE_ID);
    initialize();
    spdlog::info("dictionary initialized: {} strings", table_->size());
}

dict::~dict() {
    bpool_.flush_all();
    dict_file_->close();

  delete table_;
}

void dict::initialize() {
    spdlog::info("initialize string dictionary...");
    std::unique_lock lock(m_);
    table_ = new code_table(pool_/*, 2920000*/);
    table_->rebuild();
}

std::size_t dict::size() const {
    std::shared_lock lock(m_);
    return table_->size();    
}

dcode_t dict::insert(const std::string& s) {
    std::unique_lock loc(m_);
    auto pos = table_->find(s);
    if (pos != UNKNOWN_CODE) {
        return table_->get(pos);
    }
    auto id = pool_->add(s);
    table_->insert(s, id);
    return id;
}

dcode_t dict::lookup_string(const std::string& s) const {
    std::shared_lock lock(m_);
    auto k = table_->find(s);
    return k != UNKNOWN_CODE ? table_->get(k) : 0;
}

const char* dict::lookup_code(dcode_t code) const {
    std::shared_lock lock(m_);
    return  pool_->extract(code);
}

void dict::print_pool() const {
    pool_->print();
}

void dict::print_table() const {
    table_->print();
}

void dict::resize() {
    std::unique_lock lock(m_);
    table_->resize();
}

std::size_t dict::count_string_pool_size() {
    spdlog::info("dict::count_string_pool_size");
    std::size_t num = 0;
    pool_->scan([&num](const char *s, dcode_t c) {
        num++;
    }); 
    return num;   
}
 