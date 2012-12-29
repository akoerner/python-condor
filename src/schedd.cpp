
#include "condor_attributes.h"
#include "condor_q.h"
#include "daemon.h"
#include "daemon_types.h"
#include "enum_utils.h"
#include "dc_schedd.h"

#include <boost/python.hpp>

#include "classad_wrapper.h"

using namespace boost::python;

#define DO_ACTION(action_name) \
    reason_str = extract<std::string>(reason); \
    if (use_ids) \
        result = schedd. action_name (&ids, reason_str.c_str(), NULL, AR_TOTALS); \
    else \
        result = schedd. action_name (constraint.c_str(), reason_str.c_str(), NULL, AR_TOTALS);

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

    object actOnJobs(JobAction action, object job_spec, object reason=object())
    {
        if (reason == object())
        {
            reason = object("Python-initiated action");
        }
        StringList ids;
        std::vector<std::string> ids_list;
        std::string constraint, reason_str, reason_code;
        bool use_ids = false;
        extract<std::string> constraint_extract(job_spec);
        if (constraint_extract.check())
        {
            constraint = constraint_extract();
        }
        else
        {
            int id_len = len(job_spec);
            ids_list.reserve(id_len);
            for (int i=0; i<id_len; i++)
            {
                std::string str = extract<std::string>(job_spec[i]);
                ids_list.push_back(str);
                ids.append(ids_list[i].c_str());
            }
            use_ids = true;
        }
        DCSchedd schedd(m_addr.c_str());
        ClassAd *result = NULL;
        VacateType vacate_type;
        tuple reason_tuple;
        const char *reason_char, *reason_code_char = NULL;
        extract<tuple> try_extract_tuple(reason);
        switch (action)
        {
        case JA_HOLD_JOBS:
            if (try_extract_tuple.check())
            {
                reason_tuple = extract<tuple>(reason);
                if (len(reason_tuple) != 2)
                {
                    PyErr_SetString(PyExc_ValueError, "Hold action requires (hold string, hold code) tuple as the reason.");
                    throw_error_already_set();
                }
                reason_str = extract<std::string>(reason_tuple[0]); reason_char = reason_str.c_str();
                reason_code = extract<std::string>(reason_tuple[1]); reason_code_char = reason_code.c_str();
            }
            else
            {
                reason_str = extract<std::string>(reason);
                reason_char = reason_str.c_str();
            }
            if (use_ids)
                result = schedd.holdJobs(&ids, reason_char, reason_code_char, NULL, AR_TOTALS);
            else
                result = schedd.holdJobs(constraint.c_str(), reason_char, reason_code_char, NULL, AR_TOTALS);
            break;
        case JA_RELEASE_JOBS:
            DO_ACTION(releaseJobs)
            break;
        case JA_REMOVE_JOBS:
            DO_ACTION(removeJobs)
            break;
        case JA_REMOVE_X_JOBS:
            DO_ACTION(removeXJobs)
            break;
        case JA_VACATE_JOBS:
        case JA_VACATE_FAST_JOBS:
            vacate_type = action == JA_VACATE_JOBS ? VACATE_GRACEFUL : VACATE_FAST;
            if (use_ids)
                result = schedd.vacateJobs(&ids, vacate_type, NULL, AR_TOTALS);
            else
                result = schedd.vacateJobs(constraint.c_str(), vacate_type, NULL, AR_TOTALS);
            break;
        case JA_SUSPEND_JOBS:
            DO_ACTION(suspendJobs)
            break;
        case JA_CONTINUE_JOBS:
            DO_ACTION(continueJobs)
            break;
        default:
            PyErr_SetString(PyExc_NotImplementedError, "Job action not implemented.");
            throw_error_already_set();
        }
        if (!result)
        {
            PyErr_SetString(PyExc_RuntimeError, "Error when querying the schedd.");
            throw_error_already_set();
        }

        boost::shared_ptr<ClassAdWrapper> wrapper(new ClassAdWrapper());
        wrapper->CopyFrom(*result);
        object wrapper_obj(wrapper);

        boost::shared_ptr<ClassAdWrapper> result_ptr(new ClassAdWrapper());
        object result_obj(result_ptr);

        result_obj["TotalError"] = wrapper_obj["result_total_0"];
        result_obj["TotalSuccess"] = wrapper_obj["result_total_1"];
        result_obj["TotalNotFound"] = wrapper_obj["result_total_2"];
        result_obj["TotalBadStatus"] = wrapper_obj["result_total_3"];
        result_obj["TotalAlreadyDone"] = wrapper_obj["result_total_4"];
        result_obj["TotalPermissionDenied"] = wrapper_obj["result_total_5"];
        result_obj["TotalJobAds"] = wrapper_obj["TotalJobAds"];
        result_obj["TotalChangedAds"] = wrapper_obj["ActionResult"];
        return result_obj;
    }

    object actOnJobs2(JobAction action, object job_spec)
    {
        return actOnJobs(action, job_spec, object("Python-initiated action."));
    }

private:
    std::string m_addr, m_name, m_version;
};

BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(query_overloads, query, 0, 2);

void export_schedd()
{
    enum_<JobAction>("JobAction")
        .value("Hold", JA_HOLD_JOBS)
        .value("Release", JA_RELEASE_JOBS)
        .value("Remove", JA_REMOVE_JOBS)
        .value("RemoveX", JA_REMOVE_X_JOBS)
        .value("Vacate", JA_VACATE_JOBS)
        .value("VacateFast", JA_VACATE_FAST_JOBS)
        .value("Suspend", JA_SUSPEND_JOBS)
        .value("Continue", JA_CONTINUE_JOBS)
        ;

    class_<Schedd>("Schedd", "A client class for the HTCondor schedd")
        .def(init<const ClassAdWrapper &>(":param ad: An ad containing the location of the schedd"))
        .def("query", &Schedd::query, query_overloads("Query the HTCondor schedd for jobs.\n"
            ":param constraint: An optional constraint for filtering out jobs; defaults to 'true'\n"
            ":param attr_list: A list of attributes for the schedd to project along.  Defaults to having the schedd return all attributes.\n"
            ":return: A list of matching jobs, containing the requested attributes."))
        .def("act", &Schedd::actOnJobs2)
        .def("act", &Schedd::actOnJobs, "Change status of job(s) in the schedd.\n"
            ":param action: Action to perform; must be from enum JobAction.\n"
            ":param job_spec: Job specification; can either be a list of job IDs or a string specifying a constraint to match jobs.\n"
            ":return: Number of jobs changed.")
        ;
}

