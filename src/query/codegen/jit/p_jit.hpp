#ifndef PJIT_JITFROMSCRATCH_HPP
#define PJIT_JITFROMSCRATCH_HPP

#include <llvm/ADT/StringRef.h>
#include <llvm/ADT/Triple.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/IRTransformLayer.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/Orc/CompileOnDemandLayer.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/Error.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/Error.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <functional>
#include <memory>
#include <string>
#include <iostream>

#include "jit_cache.hpp"

/**
 * Helper class to manage the IR code optimization passes and optimization level
 */
class ir_optimizer {
public:
    ir_optimizer(unsigned level) { B.OptLevel = level; }

    /**
     * Process the given optimization passes
     */
    llvm::Expected<llvm::orc::ThreadSafeModule>
    operator()(llvm::orc::ThreadSafeModule TSM,
               const llvm::orc::MaterializationResponsibility &);

private:
    llvm::PassManagerBuilder B;
};;

/*
 * The actual JIT compiler class, providing methods to compile
 * a LLVM module and obtaining the function pointer of generated functions
 */
class p_jit {
public:
    p_jit(llvm::ExitOnError exitOnError);

    ~p_jit();

    /*
    * Deleted constructors and methods
    */
    p_jit(const p_jit &) = delete;
    p_jit &operator=(const p_jit &) = delete;
    p_jit(p_jit &&) = delete;
    p_jit &operator=(p_jit &&) = delete;

    llvm::DataLayout getDataLayout() const {
        return TM->createDataLayout();
    }

    /*
     * The actual method in order to obtain a function pointer to a JITed function
     */
    template<class Signature_t>
    llvm::Expected<std::function<Signature_t>> getFunction(llvm::StringRef Name) {
        if(auto A = getFunctionAddr(Name))
            return std::function<Signature_t>(
                    llvm::jitTargetAddressToPointer<Signature_t *>(*A));
        else
            return A.takeError();
    }

    /*
     * Method to obtain a raw (C-style) function pointer of a JITed function
     */
    template<class Signature_t>
    llvm::Expected<Signature_t> getFunctionRaw(llvm::StringRef Name) {
        if(auto A = getFunctionAddr(Name))
            return llvm::jitTargetAddressToPointer<Signature_t>(*A);
        else
            return A.takeError();
    }

    /*
     * Get the TargetMachine triple
     */
    const llvm::Triple &getTargetTriple() const {
        return TM->getTargetTriple();
    }

    /*
     * Returns the LLVM context of the target
     */
    llvm::LLVMContext &getContext() {
        return *ctx.getContext();
    }

    /*
     * Return the target machine information of the current target
     */
    llvm::TargetMachine &getTargetMachine() {
        return *TM;
    }

    /*
     * Actual method to compile a IR module into machine code
     */
    llvm::Error addModule(std::unique_ptr<llvm::Module> M);

    /*
     * Returns a global Exit object for error handling
     */
    llvm::ExitOnError get_exit() {
        return E_ERR;
    }

    /*
     * String reference to function address used in order to obtain a function pointer.
     */
    llvm::Expected<llvm::JITTargetAddress> getFunctionAddr(llvm::StringRef Name);
private:

    /*
    * LLVM Context and machine information
    */
    llvm::orc::ThreadSafeContext ctx;
    std::unique_ptr<llvm::orc::ExecutionSession> ES;
    std::unique_ptr<llvm::TargetMachine> TM;
    //llvm::JITEventListener *GDBListener;

    /*
    * JIT compiler error object
    */
    llvm::ExitOnError E_ERR;

    /*
    * LLVM IR transformation layers
    */
    llvm::orc::RTDyldObjectLinkingLayer ObjLinkingLayer;

    /*
    * The actual cache for compiled query IR code
    */
    std::unique_ptr<PJitObjectCache> ObjCache;

    llvm::orc::IRCompileLayer CompileLayer;
    llvm::orc::IRTransformLayer OptimizeLayer;

    /*
    * Target machine initialisation methods
    */
    std::unique_ptr<llvm::orc::DynamicLibrarySearchGenerator> createHostProcessResolver();
    std::unique_ptr<llvm::TargetMachine> createTargetMachine(llvm::ExitOnError ExitOnErr);
    llvm::orc::RTDyldObjectLinkingLayer::GetMemoryManagerFunction createMemoryManagerFtor();

    std::string mangle(llvm::StringRef UnmangledName);
    llvm::Error applyDataLayout(llvm::Module &M);

    /**
     * The main JIT Dylib of the compiler
     */
    llvm::orc::JITDylib &MainJD;

    /*
    * Notification method used by the GDB event listener
    
    llvm::orc::RTDyldObjectLinkingLayer::NotifyLoadedFunction createNotifyLoadedFtor() {
        using namespace std::placeholders;
        return std::bind(&llvm::JITEventListener::notifyObjectLoaded,
                         GDBListener, _1, _2, _3);
    }
    */


};


#endif //LLVMTEST_JITFROMSCRATCH_HPP