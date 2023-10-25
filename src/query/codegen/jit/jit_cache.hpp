#ifndef POSEIDON_CORE_JIT_CACHE_HPP
#define POSEIDON_CORE_JIT_CACHE_HPP

#ifdef USE_PMDK
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmem.h>
#include "polymorphic_string.hpp"
#endif

#include "llvm/ADT/Optional.h"
#include "llvm/ADT/StringMap.h"
#include "llvm/ExecutionEngine/ObjectCache.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/iterator_range.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
// #include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/RTDyldMemoryManager.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include <llvm/ExecutionEngine/JITEventListener.h>
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Mangler.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Target/TargetMachine.h"
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/IR/LegacyPassManager.h>

#include <unistd.h>
#include <stdio.h>
#include <sstream>
#include <cstdint>
#include <defs.hpp>

#ifdef USE_PMDK
#define POOLSIZE ((unsigned long)(1024 * 1024 * 50)) /* 256 MB */

#endif

/*
 * Cache for generated LLVM IR code.
 *
*/
extern std::map<std::string, std::string> code_cache_;

class PJitObjectCache : public llvm::ObjectCache {
#ifdef USE_PMDK
    using PObjCache = pmem::obj::concurrent_hash_map<string_t, string_t, string_hasher>;
    struct PCache {
        pmem::obj::persistent_ptr<PObjCache> cache;
    };

    pmem::obj::pool<PCache> cache_pool_;
    std::string dir_;
#else
   
#endif

public:
#ifdef USE_PMDK
    PJitObjectCache(std::string dir);
#else
    PJitObjectCache() {
    }
#endif



    /*
     * Method invoked by the JIT compiler after IR code compilation.
     * Returns a MemoryBufferRef object that contains a pointer to raw memory
     * @param M
     * @param Obj
     */
    void notifyObjectCompiled(const llvm::Module *M, llvm::MemoryBufferRef Obj) override;

    /**
     * Method to fetch compiled code.
     * Return a MemoryBuffer object with a pointer to the raw memory if the
     * Module exists, otherwise a nullptr.
     * @param M
     * @return
     */
    std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module *M) override;

    llvm::Expected<llvm::Optional<std::unique_ptr<llvm::MemoryBuffer>>> getCachedObject(const llvm::Module &M);

};


#endif //POSEIDON_CORE_JIT_CACHE_HPP
