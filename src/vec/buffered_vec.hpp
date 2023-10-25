/*
TODO:
- clear
*/

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

#ifndef buffered_vec_hpp_
#define buffered_vec_hpp_

#include <array>
#include <bitset>
#include <cassert>
#include <list>

#include <iostream>

#include <mutex>  // For std::unique_lock
#include <shared_mutex>

#include "defs.hpp"
#include "exceptions.hpp"
#include "bufferpool.hpp"
#include "paged_file.hpp"
#include "spdlog/spdlog.h"

#define DEFAULT_BCHUNK_SIZE PAGE_SIZE

/**
 * bchunk is a contiguous buffer of a fixed size which stores records (byte
 * sequences) of the type given as template parameter. 
 * 
 * @tparam T type to records
 * @tparam num_records number of records to store per chunk
 */
template <typename T, int num_records>
struct bchunk {
  T data_[num_records];             // the array of data
  std::bitset<num_records> slots_;  // bitstring representing empty slots (0), used slots (1)
  uint32_t first_;                  // the index of the first available slot
  /**
   * Create a new chunk, allocate and initialize the memory.
   */
  bchunk() : first_(0) {}

  /**
   * Deallocate the memory.
   */
  ~bchunk() = default;

  /**
   * Returns true if the slot at position i is used by a record.
   */
  inline bool is_used(std::size_t i) const {
    return slots_.test(i);
  }

  /**
   * Sets the slot at position i to used (b = true) or available (b = false);
   */
  inline void set(std::size_t i, bool b) {
    slots_.set(i, b);
    if (!b && i < first_) {
      // the record was deleted - update first_
      first_ = i;   
      return;
    }
    if (b && i == first_) {
      // we have to find the next available slot starting from first_
      for (auto j = first_; j < num_records; j++) {
        if (!slots_.test(j)) {
          first_ = j;
          return;
        }
      }
      if (first_ == i) first_ = num_records;
    }
  }

  /**
   * Returns the first available slot or SIZE_MAX if no slot is available
   * anymore.
   */
  std::size_t first_available() const {
    if (slots_.all())
      return SIZE_MAX;

    for (auto i = first_; i < num_records; i++)
      if (!slots_.test(i))
        return i;
    return SIZE_MAX;
  }

  /**
   * Returns the last used slot or SIZE_MAX if no slot is
   * used.
   */
  std::size_t last_used() const {
    if (slots_.none())
      return SIZE_MAX;

    for (auto i = (num_records - 1); i >= 0; i--)
      if (slots_.test(i))
        return i;
    return SIZE_MAX;
  }

  /**
   * Returns true if all slots of the chunk are used, i.e. no slots are
   * available.
   */
  inline bool is_full() const {
    return slots_.all();
  }

  /**
   * Returns true if none of the slots of the chunk are used, i.e. the chunk is
   * empty.
   */
  inline bool is_empty() const {
    return slots_.none();
  }
};

/**
 * buffered_vec is a class implementing a persistent vector as a sequence of
 * fixed-size elements. The vector consists of a linked list of chunks where
 * each chunk represents a page in a file which is cached via a bufferpool.
 * Elements of the vector are of type T which can be addressed via their index
 * (starting at 0). There are two ways to add element to buffered_vec: either via
 * store_at() to store an element at a given position or via append() where the
 * element is added to the end of the vector. For iterating over buffered_vec
 * also two methods are available: begin()/end() are used for an element-wise
 * iteration while range allows to specifiy a start and end chunk for the
 * iteration.
 */
template <typename T>
class buffered_vec {
  static constexpr auto num_entries = static_cast<int>((DEFAULT_BCHUNK_SIZE - 13) / (sizeof(T) + 1/8.0));

/// The type for pointers to single chunks.
  using bchunk_ptr = bchunk<T, num_entries> *;

 public:
  /**
   * An implementation of an iterator for buffered_vec.
   */
  class iter {
   public:
    iter(buffered_vec& bv, paged_file::page_id pid, paged_file::page_id num, offset_t p = 0) : 
      bvec_(bv), curr_pid_(pid), npages_(num), cptr_(nullptr), pos_(p) {
        if (pid == 0) 
          return; 
        cptr_ = bvec_.load_chunk(pid); 
      // make sure the element at pos_ isn't deleted
      if (cptr_ != nullptr) {
        while (pos_ < num_entries) {
          if (cptr_->is_used(pos_))
            break;
          pos_++;
        }
        if (pos_ == num_entries) {
          cptr_ = nullptr;
          pos_ = 0;
          // TODO: we assume that we don't have empty chunks
        }
      }
    }

    bool operator!=(const iter &other) const {
      return cptr_ != other.cptr_ || pos_ != other.pos_;
    }

    T &operator*() const {
      assert(cptr_ != nullptr && cptr_->is_used(pos_));
      return cptr_->data_[pos_];
    }

    iter &operator++() {
      do {
        if (++pos_ == num_entries) {
          cptr_ = bvec_.load_chunk(++curr_pid_); 
          pos_ = 0;
        }
        // make sure, cptr_[pos_] is valid
      } while (cptr_ != nullptr && !cptr_->is_used(pos_));
      return *this;
    }

  private:
    buffered_vec& bvec_;
    paged_file::page_id curr_pid_;  // page_id of the current chunk
    paged_file::page_id npages_;    // number of chunks
    bchunk_ptr cptr_;                // pointer to the current chunk
    offset_t pos_;                  // position within the current chunk
  };

  struct range_iter {
    range_iter(buffered_vec &v, paged_file::page_id first, paged_file::page_id last, std::size_t pos = 0)
        : bvec_(v), range_(first, last), current_pid_(first),
          cptr_(nullptr), pos_(pos) {
          cptr_ = bvec_.load_chunk(current_pid_); 
        }

    operator bool() const { return current_pid_ <= range_.second && cptr_; }

    T &operator*() const { return cptr_->data_[pos_]; }

    range_iter &operator++() {
      do {
        if (++pos_ == num_entries) {
          cptr_ = bvec_.load_chunk(++current_pid_); 
          pos_ = 0;
        }
        // make sure, cptr_[pos_] is valid
      } while (current_pid_ <= range_.second && cptr_ &&
               !cptr_->is_used(pos_));
      return *this;
    }

    std::size_t get_cur_chunk() { return current_pid_; }
    std::size_t get_cur_pos() { return pos_; }

  private:
    buffered_vec &bvec_; // reference to the actual chunked_vec
    std::pair<paged_file::page_id, paged_file::page_id> range_; // range of chunks to visit
    paged_file::page_id current_pid_; // position of the current chunk (initially
                                // range_.first)
    bchunk_ptr cptr_;            // pointer to the current chunk
    offset_t pos_;              // position within the current chunk
  };

  /**
   * Create a new vector associated with a file in the bufferpool.
   */
  buffered_vec(bufferpool& pool, uint64_t file_id)
      : bpool_(pool), file_id_(file_id), file_mask_(file_id << 60),
        available_slots_(0), elems_per_chunk_(num_entries), capacity_(0) {
      freelist_.reset();
      auto fptr = bpool_.get_file(file_id_);
      capacity_ = fptr->num_pages() * elems_per_chunk_;
      // initialize available_slots_ for an existing file
      fptr->set_callback([this](paged_file::cb_mode m, uint8_t *data) {
        if (m == paged_file::header_read) {
          memcpy(&available_slots_, data, sizeof(offset_t)); 
          memcpy(&freelist_, data + sizeof(offset_t), sizeof(freelist_));
        }
        else {
          // paged_file::header_write
          memcpy(data, &available_slots_, sizeof(offset_t));
          memcpy(data + sizeof(offset_t), &freelist_, sizeof(freelist_));
        }
      });
  }

  /**
   * Destructor.
   */
  ~buffered_vec() {
  }

  /**
   * Delete all chunks of the vector and reset it to an empty vector.
   */
  void clear() {
    bpool_.get_file(file_id_)->truncate();
    capacity_ = 0;
    available_slots_ = 0;
    freelist_.reset();
  }

  /**
   * Return an iterator pointing to the begin of the buffered_vec.
   */
  iter begin() { return iter(*this, 1, num_chunks()); }

  /**
   * Return an iterator pointing to the end of the buffered_vec.
   */
  iter end() { return iter(*this, 0, 0); }

  range_iter range(std::size_t first_chunk, std::size_t last_chunk, std::size_t start_pos = 0) {
    return range_iter(*this, first_chunk + 1, last_chunk + 1, start_pos);
  }

  range_iter* range_ptr(std::size_t first_chunk, std::size_t last_chunk, std::size_t start_pos = 0) {
    return new range_iter(*this, first_chunk + 1, last_chunk + 1, start_pos);
  }

  /**
   * Store the given record at position idx (note: move semantics) and mark this
   * slot as used.
   */
  void store_at(offset_t idx, T &&o) {
    auto ch = find_chunk(idx, true);
    offset_t pos = idx % elems_per_chunk_;
    ch->data_[pos] = std::move(o); // Move the temporary object.
    ch->set(pos, true);

    if (!ch->is_used(pos)) {
      std::lock_guard<std::mutex> lk(hd_mtx_);
      available_slots_--;
    }
    
    if (ch->is_full())
      remove_from_free_list(idx);
  }

  /**
   * Store the record at the end of the vector and return its position and a
   * pointer(!) to the record as a pair.
   */
  std::pair<offset_t, T *> append(T &&o, std::function<void(offset_t)> callback = nullptr) {
    bchunk_ptr tail = nullptr;
    {
      std::lock_guard<std::mutex> lk(rsz_mtx_);

      if (is_full()) {
        resize(1);
      }
      tail = get_last_chunk(true);
      assert(tail != nullptr);

      if (tail->is_full()) {

        resize(1);
        tail = get_last_chunk(true);
      }
    }
    auto pos = tail->first_available();
    assert(pos != SIZE_MAX);
    auto offs = (bpool_.get_file(file_id_)->num_pages() - 1) * elems_per_chunk_;
    if (callback != nullptr) callback(offs + pos);
    {
      std::lock_guard<std::mutex> lk(hd_mtx_);
      available_slots_--;
    }
    tail->set(pos, true);
    tail->data_[pos] = o;
    if (tail->is_full())
      remove_from_free_list(offs);
    return std::make_pair(offs + pos, &tail->data_[pos]);
  }

  /**
   * Store the record at the first available slot and return its position and a
   * pointer(!) to the record as a pair.
   */
  std::pair<offset_t, T *> store(T &&o, std::function<void(offset_t)> callback = nullptr) {
    bchunk_ptr chk = nullptr;
    paged_file::page_id pid = 0;

    {
      std::lock_guard<std::mutex> lk(rsz_mtx_);
      if (is_full()) {
        resize(1);
    
        chk = get_last_chunk(true);
        assert(chk != nullptr);

        if (chk->is_full()) {
          resize(1);
          chk = get_last_chunk(true);
        }
      }
      else {
        pid = find_in_free_list();
        chk = load_chunk(pid);
        assert(chk != nullptr);
      }
    }
    auto pos = chk->first_available();
    assert(pos != SIZE_MAX);
    auto offs = (bpool_.get_file(file_id_)->num_pages() - 1) * elems_per_chunk_;
    if (callback != nullptr) callback(offs + pos);
    {
      std::lock_guard<std::mutex> lk(hd_mtx_);
      available_slots_--;
    }
    chk->set(pos, true);
    chk->data_[pos] = o;
    if (chk->is_full()) 
      remove_from_free_list(offs);
    return std::make_pair(offs + pos, &chk->data_[pos]);
  }

  /**
   * Erase the record at the given position, i.e. mark the slot as available.
   */
  void erase(offset_t idx) {
    auto ch = find_chunk(idx, true);
    offset_t pos = idx % elems_per_chunk_;
    ch->set(pos, false);
    if (ch->is_used(pos)) {
      std::lock_guard<std::mutex> lk(hd_mtx_);
      available_slots_++;
    }
    // TODO: if (ch->empty()) delete ch;
    // if this was the first slot on this chunk which is now empty, 
    // add this chunk to the free list
    if (ch->is_full()) 
      add_to_free_list(idx);
  }

  /**
   * Return the index of the first available slot in any of the
   * chunks. The index is a global offset in the buffered_vec.
   */
  offset_t first_available() const {
    if (available_slots_ == 0)
      return UNKNOWN;

    // use the freelist
    auto pid = find_in_free_list();
    auto ch = load_chunk(pid);
    if (ch != nullptr) {
      auto first = ch->first_available();
      if (first != SIZE_MAX) 
        return (pid-1) * elems_per_chunk_ + first;
    }
    return UNKNOWN;
  }

  /**
   * Return the index of the last used slot in the last
   * chunk. The index is a global offset in the buffered_vec.
   */
  offset_t last_used() const {
    auto pg = bpool_.last_valid_page(file_id_);
    assert(pg.first != nullptr);
    bchunk_ptr ch = reinterpret_cast<bchunk_ptr>(pg.first->payload);
    std::size_t idx = (pg.second - 1) * elems_per_chunk_ + ch->last_used();
    return idx;
  }

  /**
   * Returns true if the slot at position i is used by a record.
   * The position is a global offset in the buffered_vec.
   */
  inline bool is_used(std::size_t i) const {
    auto ch = find_chunk(i);
    offset_t pos = i % elems_per_chunk_;
    return ch->slots_.test(pos);
  }

  /**
   * Return a const ref to the element stored at the given position (index) or
   * raises an exception if the index is invalid (or the slot is not used). The
   * offset is relative to the begining of the buffered_vec. The first element of
   * the buffered_vec has always an offset=0.
   */
  const T &const_at(offset_t idx) const {
    auto ch = find_chunk(idx);
    offset_t pos = idx % elems_per_chunk_;
    if (!ch->is_used(pos))
        throw unknown_id();
    return ch->data_[pos];
  }

  /**
   * Return a reference to the element stored at the given position (index) or
   * raises an exception if the index is invalid. The offset is relative to the
   * begining of the chunked_vec. The first element of the buffered_vec has
   * always an offset=0.
   * Note, that the corresponding slot is not marked as used.
   */
  T &at(offset_t idx) {
    auto ch = find_chunk(idx);
    offset_t pos = idx % elems_per_chunk_;
    if (!ch->is_used(pos))
      throw unknown_id();
    return ch->data_[pos];
  }

  /**
   * Resize the buffered_vec by the given number of additional chunks.
   */
  void resize(int nchunks) {
    spdlog::debug("resize paged file #{}", file_id_);
    std::lock_guard<std::mutex> lk(hd_mtx_);
    for (auto i = 0; i < nchunks; i++) {
      auto pg = bpool_.allocate_page(file_id_);
      // initialize pg with chunk
      auto chk = new(pg.first->payload) bchunk<T, num_entries>();
      chk->slots_.reset();
      chk->first_ = 0;
      spdlog::debug("resize -> {} add {}", pg.second, pg.second * elems_per_chunk_);
      add_to_free_list(pg.second * elems_per_chunk_-1); // calculate a record offset
      
      capacity_ += elems_per_chunk_;
      available_slots_ += elems_per_chunk_;
    }
  }

  /**
   * Return the capacity of the buffered_vec, which is the total number of
   * elements to be stored. Note, that this includes the number of already
   * existing elements.
   */
  offset_t capacity() const { return capacity_; }

  /**
   * Return true if the buffered_vec does not contain any empty slot anymore.
   */
  bool is_full() const {
    std::lock_guard<std::mutex> lk(hd_mtx_);
    return available_slots_ == 0;
  }

  /**
   * Return the number of occupied chunks.
   */
  std::size_t num_chunks() const { return bpool_.get_file(file_id_)->num_pages(); }

  /**
   * Return the number of records stored per chunk.
   */
  uint32_t elements_per_chunk() const { return elems_per_chunk_; }

  uint32_t real_chunk_size() const { return sizeof(bchunk<T, num_entries>); }

private:

  void remove_from_free_list(offset_t idx) {
    paged_file::page_id pid = idx / elems_per_chunk_ + 1;
    spdlog::debug("remove_from_free_list: {}", pid);
    std::lock_guard<std::shared_mutex> lk(fl_mtx_);
    freelist_.set(pid-1, false);
  }

  void add_to_free_list(offset_t idx) {
    paged_file::page_id pid = idx / elems_per_chunk_ + 1;
    spdlog::debug("add_to_free_list: {}", pid);
    std::lock_guard<std::shared_mutex> lk(fl_mtx_);
    freelist_.set(pid-1);
    // std::cout << "freelist: " << freelist_.to_string() << std::endl;
  }

  paged_file::page_id find_in_free_list() const {
    std::shared_lock<std::shared_mutex> lk(fl_mtx_);
    for (auto i = 0u; i < freelist_.size(); i++)
      if (freelist_.test(i))
        return i+1;
    return 0;
  }


  /**
   * Finds the chunk that stores the record at the given index or raises
   * an exception if the index is out of range.
   */
  bchunk_ptr find_chunk(offset_t idx, bool modify = false) const {
    auto page_id = idx / elems_per_chunk_ + 1;
    spdlog::debug("buffered_vec::find_chunk page #{}", page_id);
    auto pg = bpool_.fetch_page(page_id | file_mask_);
    
    // std::cout << "find_chunk: " << page_id << ", addr=" << (uint64_t)pg->payload << std::endl;

    if (modify) {
      spdlog::debug("buffered_vec::find_chunk page #{} marked as dirty", page_id | file_mask_);
      bpool_.mark_dirty(page_id | file_mask_);
    }
    return reinterpret_cast<bchunk_ptr>(pg->payload);
  }

  bchunk_ptr load_chunk(paged_file::page_id pid, bool modify = false) const {
    if (!bpool_.get_file(file_id_)->is_valid(pid)) {
      spdlog::debug("buffered_vec::load_chunk invalid page request #{}", pid);
      return nullptr;
    }
    spdlog::debug("buffered_vec::load_chunk page #{}", pid);

    auto pg = bpool_.fetch_page(pid | file_mask_);
    if (modify) {
      spdlog::debug("buffered_vec::load_chunk page #{} marked as dirty in file {}", pid | file_mask_, file_id_);
      bpool_.mark_dirty(pid | file_mask_);
    }
    return reinterpret_cast<bchunk_ptr>(pg->payload);
  }

  bchunk_ptr get_last_chunk(bool modify = false) const {
    auto pg = bpool_.last_valid_page(file_id_);
    if (modify && pg.first) {
      spdlog::debug("buffered_vec::get_last_chunk page #{} marked as dirty in file {}", pg.second | file_mask_, file_id_);
      bpool_.mark_dirty(pg.second | file_mask_);
    }
    return pg.first != nullptr ? reinterpret_cast<bchunk_ptr>(pg.first->payload) : nullptr;
  }

  //--
  bufferpool& bpool_;
  uint64_t file_id_, file_mask_;
  offset_t available_slots_; // total number of available slots for records
  uint32_t elems_per_chunk_; // number of elements per chunk
  offset_t capacity_; // total capacity of the chunked_vec in number of records
  std::bitset<65536> freelist_; // a bitset representing chunks containing available slots (1 = slots available, 0 = not)
                                // - will be written to the file header 65536
  //--
  mutable std::shared_mutex fl_mtx_;  // mutex for accessing the free list
  mutable std::mutex hd_mtx_;  // mutex for accessing header information
  mutable std::mutex rsz_mtx_;                // mutex for resizing
};


#endif
