#ifndef gc_hpp_
#define gc_hpp_

#include <list>
#include "defs.hpp"
#include "transaction.hpp"

struct gc_item {
    xid_t txid;
    offset_t oid;
    enum { gc_node = 1, gc_rship = 2 } itype; 
};

using gc_list = std::list<gc_item>;

#endif