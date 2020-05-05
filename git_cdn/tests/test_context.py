# -*- coding: utf-8 -*-
# Third Party Libraries
from git_cdn.app import extract_headers_to_context
from multidict import CIMultiDict

# RSWL Dependencies
from logging_configurer import context


async def test_update_context():
    d = CIMultiDict(
        **{
            "X-CI-job-URL": "1",
            "X-CI-project-PATH": "2",
            "X-REPO-joburl": "3",
            "X-forwarded-FOR": "4",
        }
    )
    extract_headers_to_context(d)
    assert context.get_copy() == {
        "request_header": {
            "X-CI-JOB-URL": "1",
            "X-CI-PROJECT-PATH": "2",
            "X-FORWARDED-FOR": "4",
            "X-REPO-JOBURL": "3",
        }
    }
