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

#ifndef PagedFileBPTree_hpp_
#define PagedFileBPTree_hpp_

#include <array>
#include "bufferpool.hpp"
#include "paged_file.hpp"
#include "spdlog/spdlog.h"

namespace pfbtree {

/**
 * An paged file implementation of a B+ tree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template <typename KeyType, typename ValueType, unsigned int N, unsigned int M, int NODE_ALIGNMENT = 64>
class BPTree {
  // we need at least two keys on a branch node to be able to split
  static_assert(N > 2, "number of branch keys has to be >2.");
  // we need at least one key on a leaf node
  static_assert(M > 0, "number of leaf keys should be >0.");
#ifndef UNIT_TESTS
 private:
#else
 public:
#endif

  // Forward declarations
  struct LeafNode;
  struct BranchNode;

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType key;       //< the key at which the node was split
    paged_file::page_id leftChild;   //< the resulting lhs child node
    paged_file::page_id rightChild;  //< the resulting rhs child node
  };

  bufferpool& bpool_;
  uint64_t file_id_, file_mask_;

  unsigned int depth;  //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)
  void *rootNode;  //< pointer to the root node (an instance of @c LeafNode or
                   //< @c BranchNode). This pointer is never @c nullptr.
  paged_file::page_id rootPid;

  void *load_node(paged_file::page_id pid, bool modify = false) const {
    if (!bpool_.get_file(file_id_)->is_valid(pid)) {
      spdlog::debug("pfbtree: invalid page {}", pid);
      return nullptr;
    }
    spdlog::debug("pfbtree::load_node page #{}", pid);

    auto pg = bpool_.fetch_page(pid | file_mask_);
    if (modify) {
      spdlog::debug("pfbtree::load_node page #{} marked as dirty in file {}", pid | file_mask_, file_id_);
      bpool_.mark_dirty(pid | file_mask_);
    }
    return reinterpret_cast<void *>(pg->payload);
  }

 public:
  /**
   * Typedef for a function passed to the scan method.
   */
  typedef std::function<void(const KeyType &key, const ValueType &val)>
      ScanFunc;

  /**
   * Constructor for creating a new B+ tree.
   */
  BPTree(bufferpool& pool, uint64_t file_id) : bpool_(pool), file_id_(file_id), file_mask_(file_id << 60), 
    depth(0), rootPid(1) {
    // std::cout << "sizeof(BranchNode) = " << sizeof(BranchNode)
    //  << ", sizeof(LeafNode) = " << sizeof(LeafNode) << std::endl;
    auto fptr = bpool_.get_file(file_id_);

    auto data = fptr->get_header_payload();
    memcpy(&depth, data, sizeof(unsigned int)); 
    memcpy(&rootPid, data + sizeof(unsigned int), sizeof(paged_file::page_id)); 
    spdlog::debug("read btree depth: {} and root: {}", depth, rootPid);

    if (fptr->num_pages() == 0) {
      spdlog::info("create a new btree...");
      // we create a new empty B+ tree
      auto pg = bpool_.allocate_page(file_id_);
      rootPid = 1;
      spdlog::debug("btree created with root={}", rootPid);
      rootNode = new(pg.first->payload) LeafNode(rootPid);
    }
    else {
      spdlog::debug("restore a btree with root #{}", rootPid);
      spdlog::debug("B+tree: leaf={}, branch={}", sizeof(LeafNode), sizeof(BranchNode));
      // otherwise we load the root node
      rootNode = load_node(rootPid);
    }
  }

  /**
   * Destructor for the B+ tree. Should delete all allocated nodes.
   */
  ~BPTree() {
    close();
  }

  void close() {
    sync();
    // Nodes are deleted automatically by releasing leafPool and branchPool.
    bpool_.flush_all();
  }

  /**
   * Insert an element (a key-value pair) into the B+ tree. If the key @c key
   * already exists, the corresponding value is replaced by @c val.
   *
   * @param key the key of the element to be inserted
   * @param val the value that is associated with the key
   */
  void insert(const KeyType &key, const ValueType &val) {
    SplitInfo splitInfo;
    bool wasSplit = false;

    // make sure we reload the root node in case the page was evicted
    rootNode = load_node(rootPid);
    if (depth == 0) {
      // the root node is a leaf node
      auto n = reinterpret_cast<LeafNode *>(rootNode);
      assert(n->ntype == 0);
      wasSplit = insertInLeafNode(n, key, val, &splitInfo);
    } else {
      // the root node is a branch node
      auto n = reinterpret_cast<BranchNode *>(rootNode);
      assert(n->ntype == 1);
      wasSplit = insertInBranchNode(n, depth, key, val, &splitInfo);
    }
    if (wasSplit) {
      // we had an overflow in the node and therefore the node is split
      auto root = newBranchNode();
      root->keys[0] = splitInfo.key;
      root->children[0] = splitInfo.leftChild;
      root->children[1] = splitInfo.rightChild;

      root->numKeys++;
      rootNode = root;
      rootPid = root->pid;
      depth++;
      spdlog::debug("insert -> split: create new root: {}:{}, depth = {}", root->pid, root->ntype, depth);
      assert(root->ntype == 1);
      bpool_.mark_dirty(root->pid | file_mask_);
    }
  }

  /**
   * Find the given @c key in the B+ tree and if found return the
   * corresponding value.
   *
   * @param key the key we are looking for
   * @param[out] val a pointer to memory where the value is stored
   *                 if the key was found
   * @return true if the key was found, false otherwise
   */
  bool lookup(const KeyType &key, ValueType *val) {
    assert(val != nullptr);
    bool result = false;

    // make sure we reload the root node in case the page was evicted
    rootNode = load_node(rootPid);
    auto leafNode = findLeafNode(key);
    auto pos = lookupPositionInLeafNode(leafNode, key);
    if (pos < leafNode->numKeys && leafNode->keys[pos] == key) {
      // we found it!
      *val = leafNode->values[pos];
      result = true;
    }
    return result;
  }

  /**
   * Delete the entry with the given key @c key from the tree.
   *
   * @param key the key of the entry to be deleted
   * @return true if the key was found and deleted
   */
  bool erase(const KeyType &key) {
    // make sure we reload the root node in case the page was evicted
    rootNode = load_node(rootPid);
    if (depth == 0) {
      // special case: the root node is a leaf node and
      // there is no need to handle underflow
      LeafNode *node = reinterpret_cast<LeafNode *>(rootNode);
      assert(node != nullptr);
      return eraseFromLeafNode(node, key);
    } else {
      BranchNode *node = reinterpret_cast<BranchNode *>(rootNode);
      assert(node != nullptr);
      return eraseFromBranchNode(node, depth, key);
    }
  }

  /**
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    std::cout << "rootNode #" << rootPid << "(" << depth << ")" << std::endl;
    if (depth == 0) {
      // the trivial case
      printLeafNode(0, reinterpret_cast<LeafNode *>(rootNode));
    } else {
      void *n = rootNode;
      printBranchNode(0u, reinterpret_cast<BranchNode *>(n));
    }
  }

  /**
   * Perform a scan over all key-value pairs stored in the B+ tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) {
    // make sure we reload the root node in case the page was evicted
    rootNode = load_node(rootPid);
    void *node = rootNode;
    auto d = depth;
    // we traverse to the leftmost leaf node
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      BranchNode *n = reinterpret_cast<BranchNode *>(node);
      assert(n->ntype == 1);
      auto pid = n->children[0];
      node = load_node(pid);
    }
    auto leaf = reinterpret_cast<LeafNode *>(node);
    assert(leaf->ntype == 0);
    while (leaf != nullptr) {
      assert(leaf->ntype == 0);
      // for each key-value pair call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        auto &key = leaf->keys[i];
        auto &val = leaf->values[i];
        func(key, val);
      }
      // move to the next leaf node
      leaf = reinterpret_cast<LeafNode *>(load_node(leaf->nextLeaf));
    }
  }

  /**
   * Perform a range scan over all elements within the range [minKey, maxKey]
   * and for each element call the given function @c func.
   *
   * @param minKey the lower boundary of the range
   * @param maxKey the upper boundary of the range
   * @param func the function called for each entry
   */
  void scan(const KeyType &minKey, const KeyType &maxKey, ScanFunc func) const {
    // make sure we reload the root node in case the page was evicted
    rootNode = load_node(rootPid);
    auto leaf = findLeafNode(minKey);

    while (leaf != nullptr) {
      // for each key-value pair within the range call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        auto &key = leaf->keys[i];
        if (key > maxKey) return;

        auto &val = leaf->values[i];
        func(key, val);
      }
      // move to the next leaf node
      leaf = reinterpret_cast<LeafNode *>(load_node(leaf->nextLeaf));
      assert(leaf->ntype == 0);
    }
  }

#ifndef UNIT_TESTS
 private:
#endif

  void sync() {
    auto fptr = bpool_.get_file(file_id_);
    auto data = fptr->get_header_payload();
    spdlog::debug("write btree depth: {} and root: {}", depth, rootPid);
    memcpy(data, &depth, sizeof(unsigned int));
    memcpy(data + sizeof(unsigned int), &rootPid, sizeof(paged_file::page_id));
  }

  /* ------------------------------------------------------------------- */
  /*                        DELETE AT LEAF LEVEL                         */
  /* ------------------------------------------------------------------- */

  /**
   * Delete the element with the given key from the given leaf node.
   *
   * @param node the leaf node from which the element is deleted
   * @param key the key of the element to be deleted
   * @return true of the element was deleted
   */
  bool eraseFromLeafNode(LeafNode *node, const KeyType &key) {
    bool deleted = false;
    auto pos = lookupPositionInLeafNode(node, key);
    if (node->keys[pos] == key) {
      for (auto i = pos; i < node->numKeys - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->values[i] = node->values[i + 1];
      }
      node->numKeys--;
      bpool_.mark_dirty(node->pid | file_mask_);
      deleted = true;
    }
    return deleted;
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c leaf
   * occured. If possible this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @leaf in the @c children array of
   * the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(BranchNode *node, unsigned int pos, LeafNode *leaf) {
    assert(pos <= node->numKeys);

    unsigned int middle = (M + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings
    // but only if both nodes have the same direct parent
    auto prevLeaf = reinterpret_cast<LeafNode *>(load_node(leaf->prevLeaf));
    assert(prevLeaf->ntype == 0);
    auto nextLeaf = reinterpret_cast<LeafNode *>(load_node(leaf->nextLeaf));
    assert(nextLeaf->ntype == 0);
    if (pos > 0 && prevLeaf->numKeys > middle) {
      // we have a sibling at the left for rebalancing the keys
      balanceLeafNodes(prevLeaf, leaf);
      node->keys[pos] = leaf->keys[0];
    } else if (pos < node->numKeys && nextLeaf->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      balanceLeafNodes(nextLeaf, leaf);
      node->keys[pos] = nextLeaf->keys[0];
    } else {
      // 2. if this fails we have to merge two leaf nodes
      // but only if both nodes have the same direct parent
      LeafNode *survivor = nullptr;
      if (pos > 0 && prevLeaf->numKeys <= middle) {
        survivor = mergeLeafNodes(prevLeaf, leaf);
        deleteLeafNode(leaf);
      } else if (pos < node->numKeys && nextLeaf->numKeys <= middle) {
        // because we update the pointers in mergeLeafNodes
        // we keep it here
        // auto l = leaf->nextLeaf;
        survivor = mergeLeafNodes(leaf, nextLeaf);
        deleteLeafNode(nextLeaf);
      } else {
        // this shouldn't happen?!
        assert(false);
      }
      if (node->numKeys > 1) {
        if (pos > 0) pos--;
        // just remove the child node from the current branch node
        for (auto i = pos; i < node->numKeys - 1; i++) {
          node->keys[i] = node->keys[i + 1];
          node->children[i + 1] = node->children[i + 2];
        }
        node->children[pos] = survivor->pid;
        node->numKeys--;
      } else {
        // This is a special case that happens only if
        // the current node is the root node. Now, we have
        // to replace the branch root node by a leaf node.
        rootNode = survivor;
        rootPid = survivor->pid;
        depth--;
      }
    }
  }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to
   * @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  LeafNode *mergeLeafNodes(LeafNode *node1, LeafNode *node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    assert(node1->numKeys + node2->numKeys <= M);

    // we move all keys/values from node2 to node1
    for (auto i = 0u; i < node2->numKeys; i++) {
      node1->keys[node1->numKeys + i] = node2->keys[i];
      node1->values[node1->numKeys + i] = node2->values[i];
    }
    node1->numKeys += node2->numKeys;
    node1->nextLeaf = node2->nextLeaf;
    node2->numKeys = 0;
    if (node2->nextLeaf != 0) {
      auto leaf = reinterpret_cast<LeafNode *>(load_node(node2->nextLeaf));
      assert(leaf->ntype == 0);
      leaf->prevLeaf = node1->pid;
    }
    bpool_.mark_dirty(node1->pid | file_mask_);
    return node1;
  }

  /**
   * Redistribute (key, value) pairs from the leaf node @c donor to
   * the leaf node @c receiver such that both nodes have approx. the same
   * number of elements. This method is used in case of an underflow
   * situation of a leaf node.
   *
   * @param donor the leaf node from which the elements are taken
   * @param receiver the sibling leaf node getting the elements from @c donor
   */
  void balanceLeafNodes(LeafNode *donor, LeafNode *receiver) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys[0] < receiver->keys[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0, j = 0;
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side
        receiver->keys[i + toMove - 1] = receiver->keys[i - 1];
        receiver->values[i + toMove - 1] = receiver->values[i - 1];
      }
      // move toMove keys/values from donor to receiver
      for (i = balancedNum; i < donor->numKeys; i++, j++) {
        receiver->keys[j] = donor->keys[i];
        receiver->values[j] = donor->values[i];
        receiver->numKeys++;
      }
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0;
      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->keys[receiver->numKeys] = donor->keys[i];
        receiver->values[receiver->numKeys] = donor->values[i];
        receiver->numKeys++;
      }
      // on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {
        donor->keys[i] = donor->keys[toMove + i];
        donor->values[i] = donor->values[toMove + i];
      }
    }
    donor->numKeys -= toMove;
    bpool_.mark_dirty(donor->pid | file_mask_);
    bpool_.mark_dirty(receiver->pid | file_mask_);
  }

  /* ------------------------------------------------------------------- */
  /*                        DELETE AT INNER LEVEL                        */
  /* ------------------------------------------------------------------- */
  /**
   * Delete an entry from the tree by recursively going down to the leaf level
   * and handling the underflows.
   *
   * @param node the current branch node
   * @param d the current depth of the traversal
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromBranchNode(BranchNode *node, unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    // try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    void *n = load_node(node->children[pos]);
    if (d == 1) {
      // the next level is the leaf level
      LeafNode *leaf = reinterpret_cast<LeafNode *>(n);
      assert(leaf->ntype == 0);
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      unsigned int middle = (M + 1) / 2;
      if (leaf->numKeys < middle) {
        // handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      BranchNode *child = reinterpret_cast<BranchNode *>(n);
      assert(child->ntype == 1);
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      unsigned int middle = (N + 1) / 2;
      if (child->numKeys < middle) {
        // handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && node->numKeys == 0) {
          // special case: the root node is empty now
          rootNode = child;
          rootPid = child->pid;
          depth--;
        }
      }
    }
    return deleted;
  }

  /**
   * Merge two branch nodes my moving all keys/children from @c node to @c
   * sibling and put the key @c key from the parent node in the middle. The node
   * @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeBranchNodes(BranchNode *sibling, const KeyType &key,
                       BranchNode *node) {
    assert(key <= node->keys[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    assert(sibling->keys[sibling->numKeys - 1] < key);

    sibling->keys[sibling->numKeys] = key;
    sibling->children[sibling->numKeys + 1] = node->children[0];
    for (auto i = 0u; i < node->numKeys; i++) {
      sibling->keys[sibling->numKeys + i + 1] = node->keys[i];
      sibling->children[sibling->numKeys + i + 2] = node->children[i + 1];
    }
    sibling->numKeys += node->numKeys + 1;
    bpool_.mark_dirty(sibling->pid | file_mask_);
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c child
   * occured where @c node is the parent node. If possible this is handled
   * (1) by rebalancing the elements among the node @c child and one of its
   * siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @child in the @c children array
   * of the branch node
   * @param child the node at which the underflow occured
   * @return the (possibly new) child node (in case of a merge)
   */
  BranchNode *underflowAtBranchLevel(BranchNode *node, unsigned int pos,
                             BranchNode *child) {
    assert(node != nullptr);
    assert(child != nullptr);
    
    BranchNode *newChild = child;
    unsigned int middle = (N + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 &&
        reinterpret_cast<BranchNode *>(node->children[pos - 1])->numKeys >
            middle) {
      // we have a sibling at the left for rebalancing the keys
      BranchNode *sibling =
          reinterpret_cast<BranchNode *>(node->children[pos - 1]);
      balanceBranchNodes(sibling, child, node, pos - 1);
      // node->keys[pos] = child->keys[0];
      return newChild;
    } else if (pos<node->numKeys &&reinterpret_cast<BranchNode *>(
                           node->children[pos + 1])
                       ->numKeys>
                   middle) {
      // we have a sibling at the right for rebalancing the keys
      BranchNode *sibling =
          reinterpret_cast<BranchNode *>(node->children[pos + 1]);
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else {
      // 2. if this fails we have to merge two branch nodes
      BranchNode *lSibling = nullptr, *rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {
        lSibling = reinterpret_cast<BranchNode *>(node->children[pos - 1]);
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {
        rSibling = reinterpret_cast<BranchNode *>(node->children[pos + 1]);
        nextKeys = rSibling->numKeys;
      }

      BranchNode *witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        mergeBranchNodes(lSibling, node->keys[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        // pos -= 1;
      } else if (nextKeys > 0) {
        mergeBranchNodes(child, node->keys[pos], rSibling);
        witnessNode = rSibling;
      } else
        // shouldn't happen
        assert(false);

      // remove node->keys[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {
        node->keys[i] = node->keys[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys)
          node->children[i] = node->children[i + 1];
      }
      node->numKeys--;
      bpool_.mark_dirty(node->pid | file_mask_);

      deleteBranchNode(witnessNode);
      return newChild;
    }
  }

  /* ---------------------------------------------------------------------- */
  /**
   * Rebalance two branch nodes by moving some key-children pairs from the node
   * @c donor to the node @receiver via the parent node @parent. The position of
   * the key between the two nodes is denoted by @c pos.
   *
   * @param donor the branch node from which the elements are taken
   * @param receiver the sibling branch node getting the elements from @c donor
   * @param parent the parent node of @c donor and @c receiver
   * @param pos the position of the key in node @c parent that lies between
   *      @c donor and @c receiver
   */
  void balanceBranchNodes(BranchNode *donor, BranchNode *receiver,
                         BranchNode *parent, unsigned int pos) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys[0] < receiver->keys[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // 1. make room
      receiver->children[receiver->numKeys + toMove] =
          receiver->children[receiver->numKeys];
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side
        receiver->keys[i + toMove - 1] = receiver->keys[i - 1];
        receiver->children[i + toMove - 1] = receiver->children[i - 1];
      }
      // 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->children[i] =
            donor->children[donor->numKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; i++) {
        receiver->keys[i] = donor->keys[donor->numKeys - toMove + 1 + i];
      }
      receiver->keys[toMove - 1] = parent->keys[pos];
      assert(parent->numKeys > pos);
      parent->keys[pos] = donor->keys[donor->numKeys - toMove];
      receiver->numKeys += toMove;
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiver->numKeys;

      // 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->children[n + 1 + i] = donor->children[i];
        receiver->keys[n + 1 + i] = donor->keys[i];
      }
      // 2. we have to move via the parent node: take the key from
      // parent->keys[pos]
      receiver->keys[n] = parent->keys[pos];
      receiver->numKeys += toMove;
      KeyType key = donor->keys[toMove - 1];

      // 3. on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {
        donor->keys[i] = donor->keys[toMove + i];
        donor->children[i] = donor->children[toMove + i];
      }
      donor->children[donor->numKeys - toMove] =
          donor->children[donor->numKeys];
      // and replace this key by donor->keys[0]
      assert(parent->numKeys > pos);
      parent->keys[pos] = key;
    }
    donor->numKeys -= toMove;
    bpool_.mark_dirty(donor->pid | file_mask_);
    bpool_.mark_dirty(receiver->pid | file_mask_);
  }

  /* ---------------------------------------------------------------------- */
  /*                                   DEBUGGING                            */
  /* ---------------------------------------------------------------------- */

  /**
   * Print the given branch node @c node and all its children
   * to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printBranchNode(unsigned int d, BranchNode *node) const {
    assert(node->ntype == 1);
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "B:" << node->pid << ":" << d << " { ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys[k] << ":" << node->children[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      auto pid = node->children[k];
      auto n = load_node(pid);
      if (d + 1 < depth) {
        auto child = reinterpret_cast<BranchNode *>(n);
        assert(child->ntype == 1);
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = reinterpret_cast<LeafNode *>(n);
        assert(leaf->ntype == 0);
        printLeafNode(d + 1, leaf);
      }
    }
  }

  /**
   * Print the keys of the given branch node @c node to standard
   * output.
   *
   * @param node the tree node to print
   */
  void printBranchNodeKeys(BranchNode *node) const {
    std::cout << node->pid << " { ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys[k];
    }
    std::cout << " }" << std::endl;
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(unsigned int d, LeafNode *node) const {
    assert(node->ntype == 0);
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "L:[" << node->pid << "|" << node->prevLeaf << "|" << node->nextLeaf << " : ";
    for (auto i = 0u; i < node->numKeys; i++) {
      if (i > 0) std::cout << ", ";
      std::cout << "{" << node->keys[i] << " -> " << node->values[i] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   INSERT                               */
  /* ---------------------------------------------------------------------- */

  /**
   * Insert a (key, value) pair into the corresponding leaf node. It is the
   * responsibility of the caller to make sure that the node @c node is
   * the correct node. The key is inserted at the correct position.
   *
   * @param node the node where the key-value pair is inserted.
   * @param key the key to be inserted
   * @param val the value associated with the key
   * @param splitInfo information about a possible split of the node
   */
  bool insertInLeafNode(LeafNode *node, const KeyType &key,
                        const ValueType &val, SplitInfo *splitInfo) {
    bool split = false;
    auto pos = lookupPositionInLeafNode(node, key);
    if (pos < node->numKeys && node->keys[pos] == key) {
      // handle insert of duplicates
      node->values[pos] = val;
      bpool_.mark_dirty(node->pid | file_mask_);
      return false;
    }
    if (node->numKeys == M) {
      // the node is full, so we must split it
      // determine the split position
      unsigned int middle = (M + 1) / 2;
      // move all entries behind this position to a new sibling node
      LeafNode *sibling = newLeafNode();
      bpool_.mark_dirty(sibling->pid | file_mask_);
      sibling->numKeys = node->numKeys - middle;
      for (auto i = 0u; i < sibling->numKeys; i++) {
        sibling->keys[i] = node->keys[i + middle];
        sibling->values[i] = node->values[i + middle];
      }
      node->numKeys = middle;

      // insert the new entry
      if (pos < middle)
        insertInLeafNodeAtPosition(node, pos, key, val);
      else
        insertInLeafNodeAtPosition(sibling, pos - middle, key, val);

      // setup the list of leaf nodes
      node->nextLeaf = sibling->pid;
      sibling->prevLeaf = node->pid;
      bpool_.mark_dirty(node->pid | file_mask_);

      // and inform the caller about the split
      split = true;
      splitInfo->leftChild = node->pid;
      splitInfo->rightChild = sibling->pid;
      splitInfo->key = sibling->keys[0];
    } else {
      // otherwise, we can simply insert the new entry at the given position
      insertInLeafNodeAtPosition(node, pos, key, val);
    }
    return split;
  }

  /**
   * Insert a (key, value) pair at the given position @c pos into the leaf node
   * @c node. The caller has to ensure that
   * - there is enough space to insert the element
   * - the key is inserted at the correct position according to the order of
   * keys
   *
   * @oaram node the leaf node where the element is to be inserted
   * @param pos the position in the leaf node (0 <= pos <= numKeys < M)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtPosition(LeafNode *node, unsigned int pos,
                                  const KeyType &key, const ValueType &val) {
    assert(pos < M);
    assert(pos <= node->numKeys);
    assert(node->numKeys < M);
    // we move all entries behind pos by one position
    for (unsigned int i = node->numKeys; i > pos; i--) {
      node->keys[i] = node->keys[i - 1];
      node->values[i] = node->values[i - 1];
    }
    // and then insert the new entry at the given position
    node->keys[pos] = key;
    node->values[pos] = val;
    node->numKeys++;
    // spdlog::info("mark page #{} as dirty", node->pid | file_mask_);
    bpool_.mark_dirty(node->pid | file_mask_);
  }

  /**
   * Split the given branch node @c node in the middle and move
   * half of the keys/children to the new sibling node.
   *
   * @param node the branch node to be split
   * @param splitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(BranchNode *node, const KeyType &splitKey,
                      SplitInfo *splitInfo) {
    assert(node->ntype == 1);
    // we have an overflow at the branch node, let's split it
    // determine the split position
    unsigned int middle = (N + 1) / 2;
    // adjust the middle based on the key we have to insert
    if (splitKey > node->keys[middle]) middle++;
    // move all entries behind this position to a new sibling node
    BranchNode *sibling = newBranchNode();
    sibling->numKeys = node->numKeys - middle;
    for (auto i = 0u; i < sibling->numKeys; i++) {
      sibling->keys[i] = node->keys[middle + i];
      sibling->children[i] = node->children[middle + i];
    }
    sibling->children[sibling->numKeys] = node->children[node->numKeys];
    bpool_.mark_dirty(sibling->pid | file_mask_);
 
    node->numKeys = middle - 1;
    bpool_.mark_dirty(node->pid | file_mask_);

    splitInfo->key = node->keys[middle - 1];
    splitInfo->leftChild = node->pid;
    splitInfo->rightChild = sibling->pid;
  }

  /**
   * Insert a (key, value) pair into the tree recursively by following the path
   * down to the leaf level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param depth the current depth of the tree (0 == leaf level)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool insertInBranchNode(BranchNode *node, unsigned int depth,
                         const KeyType &key, const ValueType &val,
                         SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    if (node == nullptr)
      print();
    assert(node != nullptr);
    assert(node->ntype == 1);
    auto pos = lookupPositionInBranchNode(node, key);
    auto pid = node->children[pos];
    auto n = load_node(pid);
    if (depth - 1 == 0) {
      // case #1: our children are leaf nodes
      auto child = reinterpret_cast<LeafNode *>(n);
      assert(child->ntype == 0);
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      // case #2: our children are branch nodes
      auto child = reinterpret_cast<BranchNode *>(n);
      assert(child->ntype == 1);
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      BranchNode *host = node;
      // the child node was split, thus we have to add a new entry
      // to our branch node

      if (node->numKeys == N) {
        splitBranchNode(node, childSplitInfo.key, splitInfo);

        host = reinterpret_cast<BranchNode *>(load_node(key < splitInfo->key
                                                 ? splitInfo->leftChild
                                                 : splitInfo->rightChild));
        // spdlog::info("split: page_id={}, ntype={}", host->pid, host->ntype);                                         
        assert(host->ntype == 1);

        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      if (pos < host->numKeys) {
        // if the child isn't inserted at the rightmost position
        // then we have to make space for it
        host->children[host->numKeys + 1] = host->children[host->numKeys];
        for (auto i = host->numKeys; i > pos; i--) {
          host->children[i] = host->children[i - 1];
          host->keys[i] = host->keys[i - 1];
        }
      }
      // finally, add the new entry at the given position
      host->keys[pos] = childSplitInfo.key;
      // spdlog::info("insertInBranchNode {}|{}:{}", childSplitInfo.key, childSplitInfo.leftChild, childSplitInfo.rightChild);
      host->children[pos] = childSplitInfo.leftChild;
      host->children[pos + 1] = childSplitInfo.rightChild;
      host->numKeys++;
      bpool_.mark_dirty(host->pid | file_mask_);

    }
    return split;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   LOOKUP                               */
  /* ---------------------------------------------------------------------- */

  /**
   * Traverse the tree starting at the root until the leaf node is found that
   * could contain the given @key. Note, that always a leaf node is returned
   * even if the key doesn't exist on this node.
   *
   * @param key the key we are looking for
   * @return the leaf node that would store the key
   */
  LeafNode *findLeafNode(const KeyType &key) const {
    void *node = rootNode;
    auto d = depth;
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      BranchNode *n = reinterpret_cast<BranchNode *>(node);
      assert(n->ntype == 1);
      auto pos = lookupPositionInBranchNode(n, key);
      auto pid = n->children[pos];
      assert(pid != 0);
      node = load_node(pid);
    }
    auto lnode = reinterpret_cast<LeafNode *>(node);
    assert(lnode->ntype == 0);
    return lnode;
  }

  /**
   * Lookup the search key @c key in the given branch node and return the
   * position which is the position in the list of keys + 1. in this way, the
   * position corresponds to the position of the child pointer in the
   * array @children.
   * If the search key is less than the smallest key, then @c 0 is returned.
   * If the key is greater than the largest key, then @c numKeys is returned.
   *
   * @param node the branch node where we search
   * @param key the search key
   * @return the position of the key + 1 (or 0 or @c numKey)
   */
  unsigned int lookupPositionInBranchNode(BranchNode *node,
                                         const KeyType &key) const {
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    assert(node != nullptr);
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    for (; pos < num && node->keys[pos] <= key; pos++)
      ;
    return pos;
  }

  /**
   * Lookup the search key @c key in the given leaf node and return the
   * position.
   * If the search key was not found, then @c numKeys is returned.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key  (or @c numKey if not found)
   */
  unsigned int lookupPositionInLeafNode(LeafNode *node,
                                        const KeyType &key) const {
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    assert(node != nullptr);
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    for (; pos < num && node->keys[pos] < key; pos++)
      ;
    return pos;
  }

  /* ---------------------------------------------------------------------- */

  /**
   * Create a new empty leaf node
   */
  LeafNode *newLeafNode() {
    auto pg = bpool_.allocate_page(file_id_);
    auto node = new(pg.first->payload) LeafNode(pg.second);
    node->ntype = 0;
    return node;
  }

  void deleteLeafNode(LeafNode *node) {
    // TODO
    // delete node;
     bpool_.free_page(node->pid | file_mask_);
  }

  /**
   * Create a new empty branch node
   */
  BranchNode *newBranchNode() {
    auto pg = bpool_.allocate_page(file_id_);
    auto node = new(pg.first->payload) BranchNode(pg.second);
    node->ntype = 1;
    return node;
  }

  void deleteBranchNode(BranchNode *node) {
    // TODO
    // delete node;
    bpool_.free_page(node->pid | file_mask_);
  }
  /* -----------------------------------------------------------------------
   */

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode(paged_file::page_id id) : ntype(0), pid(id), numKeys(0), nextLeaf(0), prevLeaf(0) {}

    uint8_t ntype;                    //< node type for consistency check (0=leaf, 1=branch)
    paged_file::page_id pid;          //< the page_id of the underlying page
    unsigned int numKeys;             //< the number of currently stored keys
    std::array<KeyType, M> keys;      //< the actual keys
    std::array<ValueType, M> values;  //< the actual values
    paged_file::page_id nextLeaf;     //< pointer to the subsequent sibling
    paged_file::page_id prevLeaf;     //< pointer to the preceeding sibling
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode(paged_file::page_id id) : ntype(1), pid(id), numKeys(0) {}

    // 1 + 8 + 8 + N*8 + (N+1)*8 bytes
    u_int8_t ntype;               //< node type for consistency check (0=leaf, 1=branch)
    paged_file::page_id pid;      //< the page_id of the underlying page
    unsigned int numKeys;         //< the number of currently stored keys
    std::array<KeyType, N> keys;  //< the actual keys
    std::array<paged_file::page_id, N + 1>
        children;                 //< pointers to child nodes (BranchNode or LeafNode)
  };
};

} /* end namespace pfbtree */

#endif

