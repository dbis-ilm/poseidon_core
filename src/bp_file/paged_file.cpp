/*
 * Copyright (C) 2019-2022 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include <filesystem>
#include "defs.hpp"
#include "exceptions.hpp"
#include "paged_file.hpp"

#include "spdlog/spdlog.h"

bool paged_file::open(const std::string& path, int file_type) {
    file_name_ = path;
    std::filesystem::path path_obj(path);
    // check if path exists and is of a regular file
    if (! std::filesystem::exists(path_obj)) {
        file_.open(path, std::fstream::in | std::fstream::out | std::fstream::trunc | std::fstream::binary);
        header_.ftype_ = file_type;
        header_.slots_.reset();
        memset(header_.payload_, 0, FHEADER_PAYLOAD_SIZE);
        file_.write((const char *)&header_, sizeof(header_));
        spdlog::debug("create new paged_file: {}", path);
    }
    else {
        file_.open(path, std::fstream::in | std::fstream::out | std::fstream::binary);
        spdlog::debug("open existing paged_file: {}, header size: {}", path, sizeof(header_));

        // read & check header
        file_.read((char *) &header_, sizeof(header_));
        if (memcmp(header_.fid_, "PSDN", 4) || header_.ftype_ != file_type) {
            spdlog::info("invalid file: {}, type={}({})", path, header_.ftype_, file_type);
            file_.close();
            return false;
        }

    }
    if (header_callback_ != nullptr)
        header_callback_(header_read, header_.payload_);
    file_.seekp(0, file_.end);
    npages_ = ((unsigned long)file_.tellp() - sizeof(file_header)) / PAGE_SIZE;
    spdlog::debug("file '{}' opened with {} pages", path, npages_);
    return is_open();
}

paged_file::~paged_file() {
    close();
}

void paged_file::set_callback(header_cb cb) { 
    header_callback_ = cb; 
    if (cb != nullptr)
        header_callback_(header_read, header_.payload_);
}

void paged_file::close() {
    if (!is_open()) return;

    if (header_callback_ != nullptr)
        header_callback_(header_write, header_.payload_);
    // sync header
    file_.seekp(0, file_.beg);
    file_.write((char *) &header_, sizeof(header_));
    file_.flush(); 
    // spdlog::debug("file {} closed with {} pages", file_name_, npages_);
    file_.close();
}

paged_file::page_id paged_file::allocate_page() {
    uint8_t *buf = new uint8_t[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);

    // find first 0 bit in slots_
    paged_file::page_id pid = find_first_slot();

    if (pid != UNKNOWN) {
        // reuse a freed page
        // file_.seekp(0, file_.end);
        file_.seekp(pid * PAGE_SIZE + sizeof(file_header));
        file_.write((const char *) buf, PAGE_SIZE);
        pid += 1;
    }
    else {
        // append a page to the file
        file_.seekp(0, file_.end);
        file_.write((const char *) buf, PAGE_SIZE);
        npages_++;
        pid = ((unsigned long)file_.tellp() - sizeof(file_header)) / PAGE_SIZE;
    }
    // mark slot
    // std::cout << "allocate --> " << pid << " : " << npages_ << std::endl;
    assert(pid >= 0 && pid < header_.slots_.size());
    header_.slots_.set(pid-1, true);
    delete [] buf;
    return pid;
}

paged_file::page_id paged_file::last_valid_page() {
    if (npages_ == 0) return allocate_page();
    auto i = npages_ - 1;
    while (i >= 0) {
        if (header_.slots_.test(i)) {
            return i+1;
        }
        if (i == 0) break;
        i--;
    }
    return allocate_page();
}

bool paged_file::free_page(paged_file::page_id pid) {
    if (pid == 0 || (pid-1) > npages_)
        return false;
    if (!header_.slots_.test(pid-1))
        return false;
    header_.slots_.set(pid-1, false);
    return true;
}

bool paged_file::read_page(paged_file::page_id pid, page& pg) {
    // check slot & npages_
    if (pid == 0 || (pid-1) > npages_ || !header_.slots_.test(pid-1)) {
        spdlog::info("ERROR in read_page in {}: {}, pages={} -> slot={}", file_name_, pid, npages_, header_.slots_.test(pid-1));
        throw index_out_of_range();
    }
    // std::cout << "read from pos " << (pid-1) * PAGE_SIZE + sizeof(file_header) << std::endl;

    spdlog::debug("read page in {}: {}", file_name_, pid);
    file_.seekg((pid-1) * PAGE_SIZE + sizeof(file_header));
    file_.read((char *) pg.payload, PAGE_SIZE);     

    return file_.good();
}

bool paged_file::write_page(paged_file::page_id pid, page& pg) {
    // check slot & npages_
    if (pid == 0 || (pid-1) > npages_ || !header_.slots_.test(pid-1)) {
        spdlog::info("ERROR in write_page: {}, {}", pid, npages_);
        throw index_out_of_range();
    }
    spdlog::debug("write page in {}: {}", file_name_, pid);
    file_.seekp((pid-1) * PAGE_SIZE + sizeof(file_header));
    file_.write((char *) pg.payload, PAGE_SIZE); 

    return file_.good();
}

paged_file::page_id paged_file::find_first_slot() {
    for (auto i = 0u; i < npages_; i++) {
        if (!header_.slots_.test(i))
            return i;
    }
    return UNKNOWN;
}

void paged_file::scan_pages(page& pg, std::function<void(page&, paged_file::page_id)> cb) {
    paged_file::page_id pid = 1;
    file_.seekg(sizeof(file_header));
    while (!file_.eof()) {
        file_.read((char *) pg.payload, PAGE_SIZE);   
        if (header_.slots_.test(pid-1))  
            cb(pg, pid);
        pid++;
    }
    // reset the eof flag, otherwise tellp will fail
    file_.clear();
}

void paged_file::truncate() {
     header_.slots_.reset();
    // TODO: truncate file    
}
