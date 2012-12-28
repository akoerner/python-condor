
#include "condor_query.h"
#include "condor_q.h"
#include "daemon_list.h"
#include "dc_collector.h"

#include "classad_wrapper.h"
#include <classad/classad.h>
#include <boost/python.hpp>

using namespace boost::python;

#define PARSE_COLLECTOR_QUERY_ERRORS(result) \
    switch (result) { \
    case Q_OK: \
        break; \
    case Q_INVALID_CATEGORY: \
        PyErr_SetString(PyExc_RuntimeError, "Category not supported by query type."); \
        boost::python::throw_error_already_set(); \
    case Q_MEMORY_ERROR: \
        PyErr_SetString(PyExc_MemoryError, "Memory allocation error."); \
        boost::python::throw_error_already_set(); \
    case Q_PARSE_ERROR: \
        PyErr_SetString(PyExc_SyntaxError, "Query constraints could not be parsed."); \
        boost::python::throw_error_already_set(); \
    case Q_COMMUNICATION_ERROR: \
        PyErr_SetString(PyExc_IOError, "Failed communication with collector."); \
        boost::python::throw_error_already_set(); \
    case Q_INVALID_QUERY: \
        PyErr_SetString(PyExc_RuntimeError, "Invalid query."); \
        boost::python::throw_error_already_set(); \
    case Q_NO_COLLECTOR_HOST: \
        PyErr_SetString(PyExc_RuntimeError, "Unable to determine collector host."); \
        boost::python::throw_error_already_set(); \
    default: \
        PyErr_SetString(PyExc_RuntimeError, "Unknown error from collector query."); \
        boost::python::throw_error_already_set(); \
    } \


object queryCollector(const std::string &pool="", const std::string &constraint="", list attrs=list())
{
    CondorQuery query(ANY_AD);
    if (constraint.length())
    {
        query.addANDConstraint(constraint.c_str());
    }
    if (len(attrs))
    {
        std::vector<std::string> attrs_str; attrs_str.reserve(len(attrs));
        for (int i=0; i<len(attrs); i++)
            attrs_str[i] = extract<std::string>(attrs[i]);
        std::vector<const char *> attrs_char; attrs_char.reserve(len(attrs)+1);
        attrs_char[len(attrs)] = NULL;
        for (std::vector<std::string>::const_iterator it=attrs_str.begin(); it!=attrs_str.end(); it++)
            attrs_char[0] = it->c_str();
        query.setDesiredAttrs(&attrs_char[0]);
    }
    ClassAdList adList;

    QueryResult result;
    if (pool.length())
    {
        result = query.fetchAds(adList, pool.c_str(), NULL);
    }
    else
    {
        CollectorList * collectors = CollectorList::create();
        result = collectors->query(query, adList, NULL);
        delete collectors;
    }

    PARSE_COLLECTOR_QUERY_ERRORS(result);

    list retval;
    ClassAd * ad;
    adList.Open();
    while ((ad = adList.Next()))
    {
        boost::shared_ptr<ClassAdWrapper> wrapper(new ClassAdWrapper());
        wrapper->CopyFrom(*ad);
        retval.append(wrapper);
    }
    return retval;
}

// Overloads for queryCollector; can't be done in boost.python and provide
// docstrings.
object queryCollector0()
{
    return queryCollector();
}
object queryCollector1(const std::string &pool)
{
    return queryCollector(pool);
}
object queryCollector2(const std::string &pool, const std::string &constraint)
{
    return queryCollector(pool, constraint);
}
object queryCollector3(const std::string &pool, const std::string &constraint, list attrs)
{
    return queryCollector(pool, constraint, attrs);
}

struct JobQuery {

    // Silly functions below are so Python can figure out the overloads.
    // BOOST.python can do this ... but then docstrings break :(
    object run1(const ClassAdWrapper &scheddLocation)
    {
        return run(scheddLocation);
    }
    object run2(const ClassAdWrapper &scheddLocation, const std::string &constraint)
    {
        return run(scheddLocation, constraint);
    }

    object run(const ClassAdWrapper &scheddLocation, const std::string &constraint="", list attrs=list())
    {
        CondorQ q;

        if (constraint.size())
            q.addAND(constraint.c_str());

        std::string scheddAddr, scheddName, scheddMachine, scheddVersion;
        if ( ! (scheddLocation.EvaluateAttrString(ATTR_SCHEDD_IP_ADDR, scheddAddr)  &&
                scheddLocation.EvaluateAttrString(ATTR_NAME, scheddName) &&
                scheddLocation.EvaluateAttrString(ATTR_VERSION, scheddVersion) ) )
        {
            PyErr_SetString(PyExc_RuntimeError, "Unable to locate schedd address.");
            throw_error_already_set();
        }

        StringList attrs_list;
        for (int i=0; i<len(attrs); i++)
        {
            std::string attrName = extract<std::string>(attrs[i]);
            attrs_list.append(attrName.c_str());
        }

        ClassAdList jobs;

        int fetchResult = q.fetchQueueFromHost(jobs, attrs_list, scheddAddr.c_str(), scheddVersion.c_str(), NULL);
        switch (fetchResult)
        {
        case Q_OK:
            break;
        case Q_PARSE_ERROR:
        case Q_INVALID_CATEGORY:
            PyErr_SetString(PyExc_RuntimeError, "Parse error in constraint.");
            throw_error_already_set();
            break;
        default:
            PyErr_SetString(PyExc_IOError, "Failed to fetch ads from schedd.");
            throw_error_already_set();
            break;
        }

        list retval;
        ClassAd *job;
        jobs.Open();
        while ((job = jobs.Next()))
        {
            boost::shared_ptr<ClassAdWrapper> wrapper(new ClassAdWrapper());
            wrapper->CopyFrom(*job);
            retval.append(wrapper);
        }
        return retval;
    }

    ClassAdWrapper *locateLocal()
    {
        Daemon schedd( DT_SCHEDD, 0, 0 );

        ClassAdWrapper *wrapper = new ClassAdWrapper();
        if (schedd.locate())
        {
            classad::ClassAd *daemonAd;
            if ((daemonAd = schedd.daemonAd()))
            {
                wrapper->CopyFrom(*daemonAd);
            }
            else
            {
                std::string addr = schedd.addr();
                if (!schedd.addr() || !wrapper->InsertAttr(ATTR_SCHEDD_IP_ADDR, addr))
                {
                    PyErr_SetString(PyExc_RuntimeError, "Unable to locate schedd address.");
                    throw_error_already_set();
                }
                std::string name = schedd.name() ? schedd.name() : "Unknown";
                if (!wrapper->InsertAttr(ATTR_NAME, name))
                {
                    PyErr_SetString(PyExc_RuntimeError, "Unable to insert schedd name.");
                    throw_error_already_set();
                }
                std::string hostname = schedd.fullHostname() ? schedd.fullHostname() : "Unknown";
                if (!wrapper->InsertAttr(ATTR_MACHINE, hostname))
                {
                    PyErr_SetString(PyExc_RuntimeError, "Unable to insert schedd hostname.");
                    throw_error_already_set();
                }
                std::string version = schedd.version() ? schedd.version() : "";
                if (!wrapper->InsertAttr(ATTR_VERSION, version))
                {
                    PyErr_SetString(PyExc_RuntimeError, "Unable to insert schedd version.");
                    throw_error_already_set();
                }
            }
        }
        else
        {
            PyErr_SetString(PyExc_RuntimeError, "Unable to locate local schedd");
            boost::python::throw_error_already_set();
        }
        return wrapper;
    }

    object locate(const std::string & pool, const std::string &name)
    {
        std::string constraint = ATTR_NAME " == '" + name + "'";
        list result = locateConstrained(pool, constraint);
        if (len(result) >= 1) {
            return result[0];
        }
        PyErr_SetString(PyExc_ValueError, "Unable to find schedd.");
        throw_error_already_set();
        return object();
    }

    list locateAll(const std::string & pool)
    {
        std::string constraint = ATTR_TOTAL_JOB_ADS " > 0";
        return locateConstrained(pool, constraint);
    }

    list locateConstrained(const std::string &pool, const std::string &constraint)
    {
        CondorQuery scheddQuery(SCHEDD_AD);
        QueryResult result = scheddQuery.addANDConstraint( constraint.c_str() );
        if (result != Q_OK) {
            PyErr_SetString(PyExc_ValueError, "Unable to add trivial constraint.");
            throw_error_already_set();
        }

        CollectorList Collectors;
        Collectors.append(new DCCollector(pool.c_str()));

        ClassAdList scheddList;
        result = Collectors.query(scheddQuery, scheddList);

        PARSE_COLLECTOR_QUERY_ERRORS(result);

        list retval;
        ClassAd * schedd;
        scheddList.Open();
        while ((schedd = scheddList.Next()))
        {
            boost::shared_ptr<ClassAdWrapper> wrapper(new ClassAdWrapper());
            wrapper->CopyFrom(*schedd);
            retval.append(wrapper);
        }
        return retval;
    }

};


BOOST_PYTHON_MODULE(condor)
{
    scope().attr("__doc__") = "Utilities for interacting with the HTCondor system.";

    import("classad");

    docstring_options local_docstring_options(true, false, false);

    def("query_collector", queryCollector0);
    def("query_collector", queryCollector1);
    def("query_collector", queryCollector2);
    def("query_collector", queryCollector3);
    def("query_collector", queryCollector,
        "Query the contents of a collector.\n"
        ":param pool: Name of pool; if not specified, uses the local one.\n"
        ":param constraint: A constraint for the ad query; defaults to true.\n"
        ":param attrs: A list of attributes; if specified, the returned ads will be "
        "projected along these attributes.\n"
        ":return: A list of ads in the collector matching the constraint.");

    class_<JobQuery>("JobQuery", "Class for querying an HTCondor schedd.", init<>())
        .def("run", &JobQuery::run1)
        .def("run", &JobQuery::run2)
        .def("run", &JobQuery::run,
            "Run a query against a schedd.\n"
            ":param schedd: A ClassAd containing the location information for the schedd"
            " (use one of the locate methods to generate the location information).\n"
            ":param constraint: An expression specifying a constraint for the ClassAd query."
            "  All returned jobs will match this constraint.  Defaults to true.\n"
            ":param attribute_list: A list of attributes; returned ClassAds will only "
            "contain these attributes plus a minimal set of defaults.\n"
            ":return: A list of jobs in the schedd matching the constraints.\n")
        .def("locateLocal", &JobQuery::locateLocal, return_value_policy<manage_new_object>(),
            "Locate the local schedd using the local HTCondor configuration.\n"
            ":return: A ClassAd describing the local schedd's location.")
        .def("locate", &JobQuery::locate,
            "Locate a schedd with a given name and pool.\n"
            ":param pool: A hostname of the pool to query.\n"
            ":param name: The name of the schedd to locate.\n"
            ":return: A ClassAd describing the local schedd's location.\n")
        .def("locateAll", &JobQuery::locateAll,
            "Locate all schedds in a given pool.\n"
            ":param pool: A hostname of the pool to query.\n"
            ":return: A list of ClassAds describing schedd locations.\n")
        ;
}
