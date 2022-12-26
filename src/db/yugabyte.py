import os
import re
import shutil
import subprocess
from time import sleep
from typing import List

from config import ConnectionConfig
from db.postgres import Postgres, PostgresExecutionPlan, PLAN_TREE_CLEANUP, PostgresQuery
from objects import ExecutionPlan, ListOfQueries, ResultsLoader

DEFAULT_USERNAME = 'yugabyte'
DEFAULT_PASSWORD = 'yugabyte'

JDBC_STRING_PARSE = r'\/\/(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)):(\d+)\/([a-z]+)(\?user=([a-z]+)&password=([a-z]+))?'

ENABLE_STATISTICS_HINT = "SET yb_enable_optimizer_statistics = true;"

PLAN_CLEANUP_REGEX = r"\s\(actual time.*\)|\s\(never executed\)|\s\(cost.*\)|" \
                     r"\sMemory:.*|Planning Time.*|Execution Time.*|Peak Memory Usage.*|" \
                     r"Read RPC Count:.*|Read RPC Wait Time:.*|DocDB Scanned Rows:.*|" \
                     r".*Partial Aggregate:.*|YB\s|Remote\s|" \
                     r"JIT:.*|\s+Functions:.*|\s+Options:.*|\s+Timing:.*"  # PG14 JIT info
PLAN_RPC_CALLS = r"\nRead RPC Count:\s(\d+)"
PLAN_RPC_WAIT_TIMES = r"\nRead RPC Wait Time:\s([+-]?([0-9]*[.])?[0-9]+)"
PLAN_DOCDB_SCANNED_ROWS = r"\nDocDB Scanned Rows:\s(\d+)"
PLAN_PEAK_MEMORY = r"\nPeak memory:\s(\d+)"


def yb_db_factory(config):
    if not config.revision:
        return Yugabyte(config)
    elif 'tar' in config.revision:
        return YugabyteLocalCluster(config)
    else:
        return YugabyteLocalRepository(config)


class Yugabyte(Postgres):
    def establish_connection_from_output(self, out: str):
        self.logger.info("Reinitializing connection based on cluster creation output")
        parsing = re.findall(JDBC_STRING_PARSE, out)[0]

        self.config.connection = ConnectionConfig(host=parsing[0], port=parsing[4],
                                                  username=parsing[7] or DEFAULT_USERNAME,
                                                  password=parsing[8] or DEFAULT_PASSWORD,
                                                  database=self.config.connection.database or
                                                           parsing[5], )

        self.logger.info(f"Connection - {self.config.connection}")

    def change_version_and_compile(self, revision_or_path=None):
        pass

    def destroy(self):
        pass

    def start_database(self):
        pass

    def stop_database(self):
        pass

    def call_upgrade_ysql(self):
        pass

    def get_execution_plan(self, execution_plan: str):
        return YugabyteExecutionPlan(execution_plan)


class YugabyteQuery(PostgresQuery):
    execution_plan: 'YugabyteExecutionPlan' = None


class YugabyteExecutionPlan(PostgresExecutionPlan):
    def get_rpc_calls(self, execution_plan: 'ExecutionPlan' = None):
        try:
            return int(re.sub(
                PLAN_RPC_CALLS, '',
                execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_rpc_wait_times(self, execution_plan: 'ExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_RPC_WAIT_TIMES, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_scanned_rows(self, execution_plan: 'ExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_DOCDB_SCANNED_ROWS, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_peak_memory(self, execution_plan: 'ExecutionPlan' = None):
        try:
            return int(
                re.sub(PLAN_PEAK_MEMORY, '',
                       execution_plan.full_str if execution_plan else self.full_str).strip())
        except Exception:
            return 0

    def get_no_tree_plan_str(self, plan_str):
        return re.sub(PLAN_TREE_CLEANUP, '\n', plan_str).strip()

    def get_clean_plan(self, execution_plan: 'ExecutionPlan' = None):
        no_tree_plan = re.sub(PLAN_TREE_CLEANUP, '\n',
                              execution_plan.full_str if execution_plan else self.full_str).strip()
        return re.sub(PLAN_CLEANUP_REGEX, '', no_tree_plan).strip()


class YugabyteLocalCluster(Yugabyte):
    def __init__(self, config):
        super().__init__(config)
        self.path = None

    def unpack_release(self, path):
        if not path:
            raise AttributeError("Can't pass empty path into unpack_release method")

        self.logger.info(f"Cleaning /tmp/taqo directory and unpacking {path}")
        shutil.rmtree('/tmp/taqo', ignore_errors=True)
        os.mkdir('/tmp/taqo')
        subprocess.call(['tar', '-xf', path, '-C', '/tmp/taqo'])

        self.path = '/tmp/taqo/' + list(os.walk('/tmp/taqo'))[0][1][0]

    def start_database(self):
        self.logger.info(f"Starting Yugabyte cluster with {self.config.num_nodes} nodes")

        launch_cmds = [
            'python3',
            'bin/yb-ctl',
            '--replication_factor',
            str(self.config.num_nodes),
            'create'
        ]

        if self.config.tserver_flags:
            launch_cmds.append(f'--tserver_flags={self.config.tserver_flags}')

        if self.config.master_flags:
            launch_cmds.append(f'--master_flags={self.config.master_flags}')

        out = subprocess.check_output(launch_cmds,
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'For more info, please use: yb-ctl status' not in str(out):
            self.logger.error(f"Failed to start Yugabyte cluster\n{str(out)}")
            exit(1)

        self.establish_connection_from_output(str(out))

        self.logger.info("Waiting for 15 seconds for connection availability")
        sleep(15)

    def destroy(self):
        if self.config.allow_destroy_db:
            self.logger.info("Destroying existing Yugabyte var/ directory")

            out = subprocess.check_output(['python3', 'bin/yb-ctl', 'destroy'],
                                          stderr=subprocess.PIPE,
                                          cwd=self.path, )

            if 'error' in str(out.lower()):
                self.logger.error(f"Failed to destroy Yugabyte\n{str(out.lower())}")

    def stop_database(self):
        self.logger.info("Stopping Yugabyte node if exists")
        out = subprocess.check_output(['python3', 'bin/yb-ctl', 'stop'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to stop Yugabyte\n{str(out.lower())}")

    def change_version_and_compile(self, revision_or_path=None):
        self.unpack_release(revision_or_path)

    def call_upgrade_ysql(self):
        self.logger.info("Calling upgrade_ysql and trying to upgrade metadata")

        out = subprocess.check_output(
            ['bin/yb-admin', 'upgrade_ysql', '-master_addresses', f"{self.config.host}:7100"],
            stderr=subprocess.PIPE,
            cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to upgrade YSQL\n{str(out)}")


class YugabyteLocalRepository(Yugabyte):
    def __init__(self, config):
        super().__init__(config)

        self.path = self.config.source_path

    def change_version_and_compile(self, revision_or_path=None):
        if revision_or_path:
            self.logger.info(f"Checkout revision '{revision_or_path}' for yugabyte repository")
            try:
                subprocess.check_output(['git', 'fetch'],
                                        stderr=subprocess.STDOUT,
                                        cwd=self.path,
                                        universal_newlines=True)
            except subprocess.CalledProcessError as exc:
                self.logger.error(f"Failed to fetch \n{exc.returncode}, {exc.output}")

            try:
                subprocess.check_output(['git', 'checkout', revision_or_path],
                                        stderr=subprocess.STDOUT,
                                        cwd=self.path,
                                        universal_newlines=True)
            except subprocess.CalledProcessError as exc:
                self.logger.error(
                    f"Failed to checkout revision '{revision_or_path}'\n{exc.returncode}, {exc.output}")

        self.logger.info(f"Building yugabyte from source code '{self.path}'")
        subprocess.call(['./yb_build.sh',
                         'release',
                         '--clean-force' if self.config.clean_build else '',
                         '--build-yugabyted-ui',
                         '--no-tests',
                         '--skip-java-build'],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
                        cwd=self.path)

    def call_upgrade_ysql(self):
        pass

    def destroy(self):
        if self.config.allow_destroy_db:
            self.logger.info("Destroying existing Yugabyte var/ directory")

            out = subprocess.check_output(['python3', 'bin/yugabyted', 'destroy'],
                                          stderr=subprocess.PIPE,
                                          cwd=self.path, )

            if 'error' in str(out.lower()):
                self.logger.error(f"Failed to destroy Yugabyte\n{str(out.lower())}")

    def start_database(self):
        self.logger.info("Starting Yugabyte node")

        subprocess.call(['python3', 'bin/yugabyted', 'start'],
                        # stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
                        cwd=self.path)

        out = subprocess.check_output(['python3', 'bin/yugabyted', 'status'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'Running.' not in str(out):
            self.logger.error(f"Failed to start Yugabyte\n{str(out)}")
            exit(1)

        self.establish_connection_from_output(str(out))

        self.logger.info("Waiting for 15 seconds for connection availability")
        sleep(15)

    def stop_database(self):
        self.logger.info("Stopping Yugabyte node if exists")
        out = subprocess.check_output(['python3', 'bin/yugabyted', 'stop'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to stop Yugabyte\n{str(out.lower())}")

        self.logger.info("Killing all master and tserver processes")
        subprocess.call(["pkill yb-master"],
                        shell=True)
        subprocess.call(["pkill yb-tserver"],
                        shell=True)


class YugabyteListOfQueries(ListOfQueries):
    queries: List[YugabyteQuery] = None


class YugabyteResultsLoader(ResultsLoader):

    def __init__(self):
        super().__init__()
        self.clazz = YugabyteListOfQueries
