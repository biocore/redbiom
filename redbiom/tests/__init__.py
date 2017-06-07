def assert_test_env():
    import os
    import redbiom
    conf = redbiom.get_config()
    if not conf['hostname'].startswith('http://127.0.0.1'):
        if not os.environ.get('REDBIOM_OVERRIDE_HOST_AND_TEST', False):
            raise ValueError("It appears the REDBIOM_HOST is not 127.0.0.1. "
                             "By default, the tests will not run on outside "
                             "of localhost, however if you're sure you want "
                             "to run the tests against the set host, please "
                             "set the environment variable "
                             "REDBIOM_OVERRIDE_HOST_AND_TEST")
