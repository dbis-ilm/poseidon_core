#include "jit_cache.hpp"

#ifdef USE_PMDK
namespace pmemobj_exp = pmem::obj::experimental;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
using pmem::obj::pool;
using pmem::obj::concurrent_hash_map;
using pmem::obj::array;
#endif
#include <cstring>

using namespace llvm;
using namespace llvm::orc;

std::map<std::string, std::string> code_cache_ = {};

#ifdef USE_PMDK
static inline int file_exists(const char *file) {
                return access(file, F_OK);
}

PJitObjectCache::PJitObjectCache(std::string dir) : dir_(dir) {
    /*if(file_exists(dir_.c_str()) != 0) {
        cache_pool_ = pool<PCache>::create(dir_.c_str(), "", POOLSIZE, S_IRWXU);
        transaction::run(cache_pool_, [&] {
            cache_pool_.root()->cache = make_persistent<PObjCache>();
            cache_pool_.root()->cache->runtime_initialize();
        });
    } else {
        cache_pool_ = pool<PCache>::open(dir_.c_str(), "");
        cache_pool_.root()->cache->runtime_initialize();
        cache_pool_.root()->cache->defragment();
    }*/

}

#endif

void PJitObjectCache::notifyObjectCompiled(const Module *M, MemoryBufferRef Obj) {
/*
#ifdef USE_PMDK
    auto pc = cache_pool_.root();

    PObjCache::accessor acc;
    pc->cache->insert(acc, string_t(std::string(M->getModuleIdentifier())));
    acc->second = string_t(Obj.getBufferStart(), Obj.getBufferSize());
    acc.release();
    cache_pool_.close();
#else
    std::string strBuf = std::string(Obj.getBufferStart(), Obj.getBufferSize());
    code_cache_.insert({std::string(M->getModuleIdentifier()), strBuf});
#endif*/
}

std::unique_ptr<MemoryBuffer> PJitObjectCache::getObject(const Module *M) {
    assert(M && "Lookup requires module");
    auto mid = M->getModuleIdentifier();
#ifdef USE_PMDK
    auto pc = cache_pool_.root();
    char* ret;
    int buf_size;
    {
        PObjCache::accessor acc;
        auto p = pc->cache->find(acc, string_t(std::string(mid)));

        if(p) {
            buf_size = acc->second.size();
            ret = new char[buf_size];
            std::memcpy(ret, (char*)acc->second.c_str(), buf_size);
        } else {
            return nullptr;
        }
    }
    cache_pool_.close();
    return MemoryBuffer::getMemBufferCopy(std::string(ret, buf_size));
#else
    if(code_cache_.find(mid) == code_cache_.end()) {
        return nullptr;
    } else {
        return MemoryBuffer::getMemBufferCopy(code_cache_[mid]);
    }
#endif
}

Expected<Optional<std::unique_ptr<MemoryBuffer>>> PJitObjectCache::getCachedObject(const Module &M) {
    auto cached = getObject(&M);
    if(!cached) {
        return None;
    }
    return std::forward<std::unique_ptr<MemoryBuffer>>(cached);
}