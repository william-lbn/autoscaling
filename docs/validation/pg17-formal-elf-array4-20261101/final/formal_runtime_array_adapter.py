# TEST_ONLY: 原runtime uid1000名为postgres；不修改Git测试/PG，只绑定fixture原OS账户。
import pwd, unittest
import test_array_root as tests
tests.ACCOUNT = pwd.getpwnam('postgres')
assert tests.ACCOUNT.pw_uid == 1000 and tests.ACCOUNT.pw_gid == 1000
tests.early.USER = tests.ACCOUNT.pw_name
unittest.main(module=tests, verbosity=2)
