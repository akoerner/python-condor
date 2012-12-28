
#include <boost/python.hpp>

#include "export_headers.h"

using namespace boost::python;

BOOST_PYTHON_MODULE(condor)
{
    scope().attr("__doc__") = "Utilities for interacting with the HTCondor system.";

    import("classad");

    docstring_options local_docstring_options(true, false, false);

    export_config();
    export_daemon_and_ad_types();
    export_collector();
    export_schedd();
}
