
# Find the Condor utils library

FIND_PATH(CONDOR_BASE_INCLUDES condor_includes/condor_common.h
  HINTS
  ${CONDOR_DIR}
  $ENV{CONDOR_DIR}
  /usr
  PATH_SUFFIXES include src/
)

SET(CONDOR_INCLUDES ${CONDOR_BASE_INCLUDES}/condor_includes ${CONDOR_BASE_INCLUDES}/condor_daemon_client ${CONDOR_BASE_INCLUDES}/condor_io ${CONDOR_BASE_INCLUDES}/condor_utils ${CONDOR_BASE_INCLUDES}/condor_includes)

FIND_LIBRARY(CONDOR_LIB condor_utils_7_9_3
  HINTS
  ${CONDOR_DIR}
  $ENV{CONDOR_DIR}
  /usr
  PATH_SUFFIXES lib src/condor_utils/
)

# Until we figure out the Condor defines...
#if (OS_NAME STREQUAL "LINUX")
add_definitions(-DLINUX -DHAVE_SYS_TYPES_H -DHAVE_INT64_T -DSIZEOF_LONG=8 -DHAVE_UNSETENV -DGLIBC)
#endif (OS_NAME STREQUAL "LINUX")

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Condor DEFAULT_MSG CONDOR_LIB CONDOR_INCLUDES)

