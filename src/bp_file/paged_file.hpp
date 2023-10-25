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
#ifndef paged_file_hpp_
#define paged_file_hpp_

#include <fstream>
#include <string>
#include <memory>
#include <bitset>
#include <functional>

#define PAGE_SIZE           1048576 // 1024 * 1024
#define FHEADER_PAYLOAD_SIZE 9216
/**
 * The header of a file.
 */
struct file_header {
    char fid_[4] = { 'P', 'S', 'D', 'N' };  // file identifier
    uint8_t ftype_;                         // items stored in the file (nodes, rships, properties)
    std::bitset<65536> slots_;              // slots representing which pages are not used (0) or in use (1)
    uint8_t payload_[FHEADER_PAYLOAD_SIZE]; // space usable by the application
};

/**
 * A page as the object read from and written to the file.
 */
struct page {
    uint8_t payload[PAGE_SIZE];
};

/**
 * A paged file is a disk-based file to store data which is organized in pages of a fixed size. Pages can be
 * read from the file or written back. A page is identified by a page_id which represents the offset in the file.
 * Each paged file maintains a freelist by a bitset stored in the header of the file.
 */
class paged_file {
public:
    using page_id = uint64_t;                                  /// page identifier
    enum cb_mode { header_read, header_write };                /// mode for calling the callback
    using header_cb = std::function<void(cb_mode, uint8_t *)>; /// function called after reading/before writing the header

    /**
     * Constructor: does not open or create the file.
     */
    paged_file() = default;

    /**
     * Destructor for closing the file.
     */
    ~paged_file();

    /**
     * Register the callback that is called after reading and before writing the header.
     */
    void set_callback(header_cb cb);

    uint8_t *get_header_payload() { return header_.payload_; }

    /**
     * Open or create the file with the given name and file type. If the file doesn't exist
     * a new file is created.
     */
    bool open(const std::string& path, int file_type = 0);

    /**
     * Return true if the file is open.
     */
    bool is_open() { return file_.is_open(); }

    /**
     * Close the file and ensure that the header is also flushed to disk.
     */
    void close();

    /**
     * Return the number of occupied pages.
     */
    uint64_t num_pages() const { return npages_; }

    /**
     * Add a new empty page to the file and return its page_id.
     */
    page_id allocate_page();

    /**
     * Return the page_id of the last valid page of the file.
     * If it doesn't exist (in case of an empty file) then a new
     * page is allocated.
     */
    page_id last_valid_page();

    /**
     * Deallocate the page with the given page_id. The space can 
     * be reused by a subsequent allocate_page() call.
     */
    bool free_page(page_id pid);

    /**
     * Read the page with the given identifier from the file to the given memory location pg.
     */
    bool read_page(page_id pid, page& pg);

    /**
     * Write the content of the given page to the file at the position identified by pid.
     */
    bool write_page(page_id pid, page& pg);

    /**
     * Return true of the page_id refers to a valid page.
     */
    bool is_valid(page_id pid) const { return (pid < 1 || pid > npages_) ? false : header_.slots_.test(pid-1); }

    void scan_pages(page& p, std::function<void(page&, page_id)> cb);

    void truncate();
    
private:
    /**
     * Find the first available slot (page) which can be reused.
     */
    page_id find_first_slot();

    std::string file_name_;
    std::fstream file_;  /// the file stream of reading/writing the file
    uint64_t npages_;    /// the number of pages occupied by the file (used and unused)
    file_header header_; /// the file header
    header_cb header_callback_; /// function called after reading before writing the header
};

using paged_file_ptr = std::shared_ptr<paged_file>;

#endif
