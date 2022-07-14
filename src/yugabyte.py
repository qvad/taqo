import os
import re
import shutil
import subprocess
from time import sleep

from config import Connection
from database import DEFAULT_USERNAME, DEFAULT_PASSWORD

JDBC_STRING_PARSE = r'\/\/(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)):(\d+)\/([a-z]+)(\?user=([a-z]+)&password=([a-z]+))?'


def factory(config):
    if config.yugabyte_code_path is not None:
        return YugabyteRepository(config)

    if config.num_nodes > 1:
        return YugabyteCluster(config)
    else:
        return YugabyteDistributive(config)


class Yugabyte:
    path = None

    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

    def get_connection_from_output(self, out):
        self.logger.info("Reinitializing connection based on cluster creation output")
        # parsing jdbc:postgresql://127.0.0.1:5433/yugabyte
        parsing = re.findall(JDBC_STRING_PARSE, str(out))[0]

        self.config.connection = Connection(host=parsing[0],
                                            port=parsing[4],
                                            username=parsing[7] or DEFAULT_USERNAME,
                                            password=parsing[8] or DEFAULT_PASSWORD,
                                            database=parsing[5], )

        self.logger.info(f"Connection - {self.config.connection}")

    def change_version_and_compile(self, revision_or_path=None):
        pass

    def destroy(self):
        self.logger.info("Destroying existing Yugabyte var/ directory")

        out = subprocess.check_output(['bin/yugabyted', 'destroy'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to destroy Yugabyte\n{str(out.lower())}")

    def start_node(self):
        self.logger.info("Starting Yugabyte node")

        out = subprocess.check_output(['bin/yugabyted', 'start'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'YugabyteDB started successfully!' not in str(out):
            self.logger.error(f"Failed to start Yugabyte\n{str(out)}")
            exit(1)

        self.get_connection_from_output(out)

        self.logger.info("Waiting for 15 seconds for connection availability")
        sleep(15)

    def stop_node(self):
        self.logger.info("Stopping Yugabyte node if exists")
        out = subprocess.check_output(['bin/yugabyted', 'stop'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to stop Yugabyte\n{str(out.lower())}")

        self.logger.info("Killing all master and tserver processes")
        subprocess.call(["pkill yb-master"],
                        shell=True)
        subprocess.call(["pkill yb-tserver"],
                        shell=True)

    def call_upgrade_ysql(self):
        pass


class YugabyteDistributive(Yugabyte):
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

    def unpack_release(self, path):
        if not path:
            raise AttributeError("Can't pass empty path into unpack_release method")

        self.logger.info(f"Cleaning /tmp/taqo directory and unpacking {path}")
        shutil.rmtree('/tmp/taqo', ignore_errors=True)
        os.mkdir('/tmp/taqo')
        subprocess.call(['tar', '-xf', path, '-C', '/tmp/taqo'])

        self.path = '/tmp/taqo/' + list(os.walk('/tmp/taqo'))[0][1][0]


class YugabyteCluster(YugabyteDistributive):
    def start_node(self):
        self.logger.info(f"Starting Yugabyte cluster with {self.config.num_nodes} nodes")

        launch_cmds = ['bin/yb-ctl',
                       '--replication_factor',
                       self.config.num_nodes,
                       'create']

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

        self.get_connection_from_output(out)

        self.logger.info("Waiting for 15 seconds for connection availability")
        sleep(15)

    def destroy(self):
        self.logger.info("Destroying existing Yugabyte var/ directory")

        out = subprocess.check_output(['bin/yb-ctl', 'destroy'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to destroy Yugabyte\n{str(out.lower())}")

    def stop_node(self):
        self.logger.info("Stopping Yugabyte node if exists")
        out = subprocess.check_output(['bin/yb-ctl', 'stop'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path, )

        if 'error' in str(out.lower()):
            self.logger.error(f"Failed to stop Yugabyte\n{str(out.lower())}")


class YugabyteRepository(Yugabyte):
    def __init__(self, config):
        super().__init__(config)

        self.path = self.config.yugabyte_code_path

    def change_version_and_compile(self, revision_or_path=None):
        if revision_or_path:
            self.logger.info(f"Checkout revision '{revision_or_path}' for yugabyte repository")
            try:
                subprocess.check_output(['git', 'checkout', revision_or_path],
                                        stderr=subprocess.PIPE,
                                        cwd=self.path)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to checkout revision '{revision_or_path}'\n{e}")

        self.logger.info(f"Building yugabyte from source code '{self.path}'")
        out = subprocess.check_output(['./yb_build.sh',
                                       'release', '--no-tests', '--skip-java-build'],
                                      stderr=subprocess.PIPE,
                                      cwd=self.path)

        if 'Built target initial_sys_catalog_snapshot' not in str(out):
            self.logger.error(f"Failed to build Yugabyte\n{str(out)}")
            exit(1)

    def call_upgrade_ysql(self):
        pass
