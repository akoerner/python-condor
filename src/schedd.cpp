
#include "condor_attributes.h"
#include "condor_q.h"
#include "daemon.h"
#include "daemon_types.h"

#include <boost/python.hpp>

#include "classad_wrapper.h"

using namespace boost::python;

struct Schedd {

    Schedd()
    {
        Daemon schedd( DT_SCHEDD, 0, 0 );

        if (schedd.locate())
        {
            if (schedd.addr())
            {
                m_addr = schedd.addr();
            }
            else
            {
                PyErr_SetString(PyExc_RuntimeError, "Unable to locate schedd address.");
                throw_error_already_set();
            }
            m_name = schedd.name() ? schedd.name() : "Unknown";
            m_version = schedd.version() ? schedd.version() : "";
        }
        else
        {
            PyErr_SetString(PyExc_RuntimeError, "Unable to locate local daemon");
            boost::python::throw_error_already_set();
        }
    }

    Schedd(const ClassAdWrapper &ad)
      : m_addr(), m_name("Unknown"), m_version("")
    {
        if (!ad.EvaluateAttrString(ATTR_SCHEDD_IP_ADDR, m_addr))
        {
            PyErr_SetString(PyExc_ValueError, "Schedd address not specified.");
            throw_error_already_set();
        }
        ad.EvaluateAttrString(ATTR_NAME, m_name);
        ad.EvaluateAttrString(ATTR_VERSION, m_version);
    }

    object query(const std::string &constraint="", list attrs=list())
    {
        CondorQ q;

        if (constraint.size())
            q.addAND(constraint.c_str());

        StringList attrs_list(NULL, "\n");
        // Must keep strings alive; StringList does not create an internal copy.
        std::vector<std::string> attrs_str; attrs_str.reserve(len(attrs));
        for (int i=0; i<len(attrs); i++)
        {
            std::string attrName = extract<std::string>(attrs[i]);
            attrs_str.push_back(attrName);
            attrs_list.append(attrs_str[i].c_str());
        }

        ClassAdList jobs;

        int fetchResult = q.fetchQueueFromHost(jobs, attrs_list, m_addr.c_str(), m_version.c_str(), NULL);
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


private:
    std::string m_addr, m_name, m_version;
};

BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(query_overloads, query, 0, 2);

void export_schedd()
{
    class_<Schedd>("Schedd", "A client class for the HTCondor schedd")
        .def(init<const ClassAdWrapper &>(":param ad: An ad containing the location of the schedd"))
        .def("query", &Schedd::query, query_overloads("Query the HTCondor schedd for jobs.\n"
            ":param constraint: An optional constraint for filtering out jobs; defaults to 'true'\n"
            ":param attr_list: A list of attributes for the schedd to project along.  Defaults to having the schedd return all attributes.\n"
            ":return: A list of matching jobs, containing the requested attributes."))
        ;
}

