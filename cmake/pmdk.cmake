  # ========================================================================== #
  # Peristent Memory Development Kit (pmem.io)                                 #
  # ========================================================================== #

if(USE_PMDK MATCHES ON)

find_package(PkgConfig REQUIRED)
  message(STATUS "Searching for Intel PMDK")
  find_path(PMDK_INCLUDE_DIR libpmem.h)
  pkg_check_modules(PMDK REQUIRED libpmemobj++>=1.5)
  set(PMDK_INCLUDE_DIRS ${PMDK_INCLUDE_DIRS} ${PMDK_INCLUDE_DIR})
  if(NOT PMDK_INCLUDE_DIRS OR "${PMDK_INCLUDE_DIRS}" STREQUAL "")
    message(FATAL_ERROR "ERROR: libpmem include directory not found.")
  endif()
  message(STATUS "  libpmem.h found in ${PMDK_INCLUDE_DIRS}")
  add_definitions( "-DUSE_PMDK" )
  mark_as_advanced(PMDK_LIBRARIES PMDK_INCLUDE_DIRS)
endif()
