/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef chunked_vec_hpp_
#define chunked_vec_hpp_

#include <array>
#include <bitset>
#include <cassert>
#include <list>

#include <iostream>

#ifdef USE_PMDK
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/utils.hpp>
#endif

#include "defs.hpp"
#include "exceptions.hpp"

#define DEFAULT_CHUNK_SIZE 4096 // 1048576

/**
 * chunk is a contiguous buffer of a fixed size which stores records (byte
 * sequences) of the type given as template parameter. chunks can form a linked
 * list.
 * template parameters: T = type to records, num_records = number of records to
 *                      store per chunk
 */
template <typename T, int num_records> struct chunk {
  p<std::bitset<num_records>>
      slots_; // bitstring representing empty slots (0), used slots (1)
#ifdef USE_PMDK
  pmem::obj::array<T, num_records> data_;
  pmem::obj::persistent_ptr<chunk<T, num_records>> next_;
#else
  std::array<T, num_records> data_; // the array of data
  chunk<T, num_records> *next_;     // pointer to the successor chunk
#endif

  /**
   * Create a new chunk, allocate and initialize the memory.
   */
  chunk() : next_(nullptr) {}

  /**
   * Deallocate the memory.
   */
  ~chunk() { next_ = nullptr; }

  /**
   * Returns true if the slot at position i is used by a record.
   */
  inline bool is_used(std::size_t i) const {
#ifdef USE_PMDK
    return slots_.get_ro().test(i);
#else
    return slots_.test(i);
#endif
  }

  /**
   * Sets the slot at position i to used (b = true) or available (b = false);
   */
  inline void set(std::size_t i, bool b) {
#ifdef USE_PMDK
    slots_.get_rw().set(i, b);
#else
    slots_.set(i, b);
#endif
  }

  /**
   * Returns the first available slot or SIZE_MAX if no slot is available
   * anymore.
   */
  std::size_t first_available() const {
#ifdef USE_PMDK
    if (slots_.get_ro().all())
#else
    if (slots_.all())
#endif
      return SIZE_MAX;
    for (auto i = 0u; i < num_records; i++)
#ifdef USE_PMDK
      if (!slots_.get_ro().test(i))
#else
      if (!slots_.test(i))
#endif
        return i;
    return SIZE_MAX;
  }

  /**
   * Returns true if all slots of the chunk are used, i.e. no slots are
   * available.
   */
  inline bool is_full() const {
#ifdef USE_PMDK
    return slots_.get_ro().all();
#else
    return slots_.all();
#endif
  }

  /**
   * Returns true if none of the slots of the chunk are used, i.e. the chunk is
   * empty.
   */
  inline bool is_empty() const {
#ifdef USE_PMDK
    return slots_.get_ro().none();
#else
    return slots_.none();
#endif
  }
};

/**
 * chunked_vec is a class implementing a persistent vector as a sequence of
 * fixed-size elements. The vector consists of a linked list of chunks where
 * each chunk represents a contiguous memory region. In this way, the entire
 * sequence does not require to be physically contiguous stored in memory and
 * allows to extend the vector more easily without copying the entire data.
 * Elements of the vector are of type T which can be addressed via their index
 * (starting at 0). There are two ways to add element to chunked_vec: either via
 * store_at() to store an element at a given position or via append() where the
 * element is added to the end of the vector. For iterating over chunked_vec
 * also two methods are available: begin()/end() are used for an element-wise
 * iteration while range allows to specifiy a start and end chunk for the
 * iteration.
 */
template <typename T, int chunk_size = DEFAULT_CHUNK_SIZE> class chunked_vec {
  /// The type for pointers to single chunks.
#ifdef USE_PMDK
  using chunk_ptr = pmem::obj::persistent_ptr<chunk<T, chunk_size / sizeof(T)>>;
#else
  using chunk_ptr = chunk<T, chunk_size / sizeof(T)> *;
#endif

public:
  /**
   * An implementation of an iterator for chunked_vec.
   */
  class iter {
  public:
    iter(chunk_ptr ptr, offset_t p = 0) : cptr_(ptr), pos_(p) {
      // make sure the element at pos_ isn't deleted
      if (cptr_ != nullptr) {
        while (pos_ < chunk_size / sizeof(T)) {
          if (cptr_->is_used(pos_))
            break;
          pos_++;
        }
        if (pos_ == chunk_size / sizeof(T)) {
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
        if (++pos_ == chunk_size / sizeof(T)) {
          cptr_ = cptr_->next_;
          pos_ = 0;
        }
        // make sure, cptr_[pos_] is valid
      } while (cptr_ != nullptr && !cptr_->is_used(pos_));
      return *this;
    }

  private:
    chunk_ptr cptr_; // pointer to the current chunk
    offset_t pos_;   // position within the current chunk
  };

  struct range_iter {
    range_iter(chunked_vec &v, std::size_t first, std::size_t last)
        : vec_(v), range_(first, last), current_chunk_(first),
          cptr_(vec_.chunk_list_[first]), pos_(0) {}

    operator bool() const { return current_chunk_ <= range_.second && cptr_; }

    T &operator*() const { return cptr_->data_[pos_]; }

    range_iter &operator++() {
      do {
        if (++pos_ == chunk_size / sizeof(T)) {
          cptr_ = cptr_->next_;
          current_chunk_++;
          pos_ = 0;
        }
        // make sure, cptr_[pos_] is valid
      } while (current_chunk_ <= range_.second && cptr_ &&
               !cptr_->is_used(pos_));
      return *this;
    }

  private:
    chunked_vec &vec_; // reference to the actual chunked_vec
    std::pair<std::size_t, std::size_t> range_; // range of chunks to visit
    std::size_t current_chunk_; // position of the current chunk (initially
                                // range_.first)
    chunk_ptr cptr_;            // pointer to the current chunk
    offset_t pos_;              // position within the current chunk
  };

  /**
   * Create a new empty vector.
   */
  chunked_vec()
      : capacity_(0), available_slots_(0),
        elems_per_chunk_(chunk_size / sizeof(T)) {}

  /**
   * Destructor.
   */
  ~chunked_vec() {
    // delete the chunks
#ifdef USE_PMDK
    auto pop = pmem::obj::pool_by_vptr(this);
    pmem::obj::transaction::run(pop, [&] {
      for (auto p : chunk_list_)
        pmem::obj::delete_persistent<chunk<T, chunk_size / sizeof(T)>>(p);
    });
#else
    for (auto p : chunk_list_)
      delete p;
#endif
  }

  /**
   * Delete all chunks of the vector and reset it to an empty vector.
   */
  void clear() {
#ifdef USE_PMDK
    auto pop = pmem::obj::pool_by_vptr(this);
    pmem::obj::transaction::run(pop, [&] {
      for (auto p : chunk_list_)
        pmem::obj::delete_persistent<chunk<T, chunk_size / sizeof(T)>>(p);
    });
#else
    for (auto p : chunk_list_)
      delete p;
#endif
    chunk_list_.clear();
    capacity_ = 0;
    available_slots_ = 0;
  }

  /**
   * Return an iterator pointing to the begin of the chunked_vec.
   */
  iter begin() {
    return iter(chunk_list_.empty() ? nullptr : chunk_list_.front());
  }

  /**
   * Return an iterator pointing to the end of the chunked_vec.
   */
  iter end() { return iter(nullptr); }

  range_iter range(std::size_t first_chunk, std::size_t last_chunk) {
    return range_iter(*this, first_chunk, last_chunk);
  }

  /**
   * Store the given record at position idx (note: move semantics) and mark this
   * slot as used.
   */
  void store_at(offset_t idx, T &&o) {
    auto ch = find_chunk(idx);
    offset_t pos = idx % elems_per_chunk_;
    ch->data_[pos] = o;
    bool is_used = ch->is_used(pos);
    ch->set(pos, true);
    if (!is_used)
      available_slots_--;
  }

  /**
   * Store the record at the end of the vector and return its position and a
   * pointer(!) to the record as a pair.
   */
  std::pair<offset_t, T *> append(T &&o) {
    if (is_full())
      resize(1);
    auto tail = chunk_list_.back();
    auto pos = tail->first_available();
    assert(pos != SIZE_MAX);
    available_slots_--;
    tail->set(pos, true);
    tail->data_[pos] = o;
    auto offs = (chunk_list_.size() - 1) * elems_per_chunk_;
    return std::make_pair(offs + pos, &tail->data_[pos]);
  }

  /**
   * Erase the record at the given position, i.e. mark the slot as available.
   */
  void erase(offset_t idx) {
    auto ch = find_chunk(idx);
    offset_t pos = idx % elems_per_chunk_;
    if (ch->is_used(pos))
      available_slots_++;
    ch->set(pos, false);
    // TODO: if (ch->empty()) delete ch;
  }

  /**
   * Return the index of the first available slot in any of the
   * chunks. The index is a global offset in the chunked_vec.
   */
  offset_t first_available() const {
    if (available_slots_ == 0)
      return UNKNOWN;

    chunk_ptr ptr = chunk_list_.front();
    offset_t offs = 0;
    while (ptr != nullptr) {
      auto first = ptr->first_available();
      if (first != SIZE_MAX) {
        return offs + first;
      }
      ptr = ptr->next_;
      offs += elems_per_chunk_;
    }
    return UNKNOWN;
  }

  /**
   * Return a const ref to the element stored at the given position (index) or
   * raises an exception if the index is invalid (or the slot is not used). The
   * offset is relative to the begining of the chunked_vec. The first element of
   * the chunked_vec has always an offset=0.
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
   * begining of the chunked_vec. The first element of the chunked_vec has
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
   * Resize the chunked_vec by the given number of additional chunks.
   */
  void resize(int nchunks) {
    int num = nchunks;
    chunk_ptr ptr = nullptr;

#if USE_PMDK
    auto pop = pmem::obj::pool_by_vptr(this);
#endif
    if (chunk_list_.empty()) {
#if USE_PMDK
      pmem::obj::transaction::run(pop, [&] {
        ptr = pmem::obj::make_persistent<chunk<T, chunk_size / sizeof(T)>>();
      });
#else
      ptr = new chunk<T, chunk_size / sizeof(T)>();
#endif
      chunk_list_.push_back(ptr);
      available_slots_ = capacity_ = elems_per_chunk_;
      num--;
    } else {
      // initialize ptr
      ptr = chunk_list_.back();
    }
    for (auto i = 0; i < num; i++) {
#if USE_PMDK
      chunk_ptr c;
      pmem::obj::transaction::run(pop, [&] {
        c = pmem::obj::make_persistent<chunk<T, chunk_size / sizeof(T)>>();
      });
#else
      auto c = new chunk<T, chunk_size / sizeof(T)>();
#endif
      ptr->next_ = c;
      ptr = c;
      chunk_list_.push_back(ptr);
      capacity_ += elems_per_chunk_;
      available_slots_ += elems_per_chunk_;
    }
  }

  /**
   * Return the capacity of the chunked_vec, which is the total number of
   * elements to be stored. Note, that this includes the number of already
   * existing elements.
   */
  offset_t capacity() const { return capacity_; }

  /**
   * Return true if the chunked_vec does not contain any empty slot anymore.
   */
  bool is_full() const { return available_slots_ == 0; }

  /**
   * Return the number of occupied chunks.
   */
  std::size_t num_chunks() const { return chunk_list_.size(); }

  /**
   * Return the number of records stored per chunk.
   */
  uint32_t elements_per_chunk() const { return elems_per_chunk_; }

private:
  /**
   * Finds the chunk that stores the record at the given index or raises
   * an exception if the index is out of range.
   */
  chunk_ptr find_chunk(offset_t idx) const {
    auto n = idx / elems_per_chunk_;
    if (n >= chunk_list_.size())
      throw index_out_of_range();
    return chunk_list_[n];
  }

  p<offset_t>
      capacity_; // total capacity of the chunked_vec in number of records
  p<offset_t> available_slots_; // total number of available slots for records
  p<uint32_t> elems_per_chunk_; // number of elements per chunk
#ifdef USE_PMDK
  pmem::obj::vector<chunk_ptr>
      chunk_list_; // the persistent list of pointers to all chunks
#else
  std::vector<chunk_ptr> chunk_list_; // the list of pointers to all chunks
#endif
};

#endif
