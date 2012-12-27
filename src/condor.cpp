
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


object queryCollector(const std::string &pool, const std::string &constraint="", list attrs=list())
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
    //CondorError errStack;
    QueryResult result = query.fetchAds(adList, pool.c_str(), NULL);

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

BOOST_PYTHON_FUNCTION_OVERLOADS(queryCollector_overloads, queryCollector, 1, 3);

struct JobQuery {

    object run(ClassAdWrapper &scheddLocation, const std::string &constraint="", list attrs=list())
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

    BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(run_overloads, run, 1, 3);
};

BOOST_PYTHON_MODULE(condor)
{
    import("classad");

    def("query_collector", queryCollector, queryCollector_overloads());

    class_<JobQuery>("JobQuery")
        .def("run", &JobQuery::run, JobQuery::run_overloads())
        .def("locateLocal", &JobQuery::locateLocal, return_value_policy<manage_new_object>())
        .def("locate", &JobQuery::locate)
        .def("locateAll", &JobQuery::locateAll)
        ;
}
