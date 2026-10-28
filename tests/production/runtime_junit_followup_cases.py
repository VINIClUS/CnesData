"""Fixtures for the runtime follow-up JUnit aggregate cases."""

_FOLLOWUP_JUNIT_POSITIVE = {
    "raiz_com_suites_irmas_e_agregado": (
        '<testsuites tests="2" skipped="0" failures="0" errors="0">'
        '<testsuite tests="1" skipped="0" failures="0" errors="0">'
        '<testcase name="primeiro"/></testsuite>'
        '<testsuite tests="1" skipped="0" failures="0" errors="0">'
        '<testcase name="segundo"/></testsuite>'
        "</testsuites>"
    ),
    "raiz_e_intermedio_com_agregados": (
        '<testsuites tests="2" skipped="0" failures="0" errors="0">'
        '<testsuites tests="2" skipped="0" failures="0" errors="0">'
        '<testsuite tests="1" skipped="0" failures="0" errors="0">'
        '<testcase name="primeiro"/></testsuite>'
        '<testsuite tests="1" skipped="0" failures="0" errors="0">'
        '<testcase name="segundo"/></testsuite>'
        "</testsuites></testsuites>"
    ),
}
