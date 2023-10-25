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
#ifndef bufferpool_hpp_
#define bufferpool_hpp_

#include <list>
#include <array>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <boost/dynamic_bitset.hpp>
#include "paged_file.hpp"

#define DEFAULT_BUFFER_SIZE 5000 // 1000
#define MAX_PFILES          15 // 4 bits

/**
 * bufferpool implements a page cache for accessing paged_files. For a single
 * bufferpool multiple files can be registered. These files can be addressed
 * by their file_id. The corresponding pages are identified by page_id with a 
 * file_id mask in the upper 4 bits.
 */
class bufferpool {
public:
    /**
     * Construct a bufferpool of the given size (in pages).
     */
    bufferpool(std::size_t bsize = DEFAULT_BUFFER_SIZE);

    /**
     * Destroy the bufferpool and deallocate the occupied memory.
     */
    ~bufferpool();

    /**
     * Register the given (open) paged_file by the given id (0-9).
     */
    void register_file(uint8_t file_id, paged_file_ptr pf);

    /**
     * Get the paged_file ptr associated with the given id.
     */ 
    paged_file_ptr get_file(uint8_t file_id);

    /**
     * Fetch the given page either from the bufferpool or - if not cached -
     * from the corresponding paged_file. The file_id is masked in the upper
     * 4 bits of pid.
     */
    page* fetch_page(paged_file::page_id pid);

    /**
     * Allocate a new page in the given file.
     */
    std::pair<page*, paged_file::page_id> allocate_page(uint8_t file_id);

    /**
     * Deallocate the page with the given page_id. The space can 
     * be reused by a subsequent allocate_page() call.
     */
    void free_page(paged_file::page_id pid);

    void scan_file(uint8_t file_id, std::function<void(page *)> cb);

    /**
     * Return the page_id of the last valid page of the given file.
     * If it doesn't exist (in case of an empty file) then a new
     * page is allocated.
     */
    std::pair<page*, paged_file::page_id> last_valid_page(uint8_t file_id);

    /**
     * Mark the page identified by the given id as dirty, i.e.
     * if the page is evicted or flushed then it will be written
     * back to disk.
     */
    void mark_dirty(paged_file::page_id pid);

    /**
     * Write all pages in the bufferpool back to the disk files.
     */
    void flush_all();

    /**
     * Write the given page back to the corresponding paged_file. If
     * evict is set to true, then the page is removed from cache.
     */
    void flush_page(paged_file::page_id pid, bool evict = true);

    /**
     * Removes all pages from the cache without writing them to disk.
     */
    void purge();
    
    /**
     * Calculate and return the bufferpool hit ratio.
     */
    double hit_ratio() const;
    
private:
    void dump();
    
    bool evict_page();
    std::pair<page *, std::size_t> load_page_from_file(paged_file::page_id pid);
    void write_page_to_file(paged_file::page_id pid, page *pg);

    std::size_t bsize_; // the size of the bufferpool in pages
    page *buffer_;      // the memory region for all cached pages

    struct buf_slot {
        page *p_;           // a pointer to a page in buffer_
        bool dirty_;        // a flag indicating that the page is marked as dirty
        std::size_t pos_;   // position of the slot in buffer_
    };

    std::list<uint64_t> lru_list_; // the LRU list
    std::unordered_map<uint64_t, buf_slot> ptable_; // a hashtable to map page_id to pages
    boost::dynamic_bitset<> slots_; // a bitset indicating which slot in buffer_ is occupied

    std::array<paged_file_ptr, MAX_PFILES> files_; // the registered paged_files

    std::size_t p_reads_, l_reads_; // number of physical and logical reads

    mutable std::recursive_mutex mutex_;
};

#endif