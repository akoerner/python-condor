
#include "condor_query.h"
#include "classad_wrapper.h"
#include <classad/classad.h>
#include <boost/python.hpp>

using namespace boost::python;

object queryCollector(const std::string &pool, const std::string &constraint="", const std::vector<std::string> *attrs=NULL)
{
    CondorQuery query(ANY_AD);
    if (constraint.length())
    {
        query.addANDConstraint(constraint.c_str());
    }
    if (attrs && attrs->size())
    {
        std::vector<const char *> attrs_char; attrs_char.reserve(attrs->size()+1);
        attrs_char[attrs->size()] = NULL;
        for (std::vector<std::string>::const_iterator it=attrs->begin(); it!=attrs->end(); it++)
            attrs_char[0] = it->c_str();
        query.setDesiredAttrs(&attrs_char[0]);
    }
    ClassAdList adList;
    //CondorError errStack;
    QueryResult result = query.fetchAds(adList, pool.c_str(), NULL);

    switch (result) {
    case Q_OK:
        break;
    case Q_INVALID_CATEGORY:
        PyErr_SetString(PyExc_RuntimeError, "Category not supported by query type.");
        boost::python::throw_error_already_set();
    case Q_MEMORY_ERROR:
        PyErr_SetString(PyExc_MemoryError, "Memory allocation error.");
        boost::python::throw_error_already_set();
    case Q_PARSE_ERROR:
        PyErr_SetString(PyExc_SyntaxError, "Query constraints could not be parsed.");
        boost::python::throw_error_already_set();
    case Q_COMMUNICATION_ERROR:
        PyErr_SetString(PyExc_IOError, "Failed communication with collector.");
        boost::python::throw_error_already_set();
    case Q_INVALID_QUERY:
        PyErr_SetString(PyExc_RuntimeError, "Invalid query.");
        boost::python::throw_error_already_set();
    case Q_NO_COLLECTOR_HOST:
        PyErr_SetString(PyExc_RuntimeError, "Unable to determine collector host.");
        boost::python::throw_error_already_set();
    default:
        PyErr_SetString(PyExc_RuntimeError, "Unknown error from collector query.");
        boost::python::throw_error_already_set();
    }

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

BOOST_PYTHON_MODULE(condor)
{
    import("classad");

    def("query_collector", queryCollector, queryCollector_overloads());
}
