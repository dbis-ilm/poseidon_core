#ifndef analytics_hpp_
#define analytics_hpp_

#include "defs.hpp"

struct node;
struct relationship;

/**
 * Typedef for a function that computes the weight of a relationship.
 */
using rship_weight = std::function<double(relationship&)>;

/**
 * Typedef for a predicate to check that a relationship is followed via the search.
 */
using rship_predicate = std::function<bool(relationship&)>;

/**
 * Typedef for a node visitor callback.
 */
using node_visitor = std::function<void(node&)>;

/**
 * Typedef for a node visitor callback which receives the full path.
 */
using path = std::vector<offset_t>;

using path_visitor = std::function<void(node&, const path&)>;

#endif