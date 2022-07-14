import os
import shutil
import subprocess


def factory(config):
    if config.yugabyte_code_path is None:
        return YugabyteDistributive(config)
    else:
        return YugabyteRepository(config)


class Yugabyte:
    path = None

    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

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


class YugabyteRepository(Yugabyte):
    def __init__(self, config):
        super().__init__(config)

        self.path = self.config.yugabyte_code_path

    def change_version_and_compile(self, revision_or_path=None):
        if revision_or_path:
            self.logger.info(f"Checkout revision {revision_or_path} for yugabyte repository")
            try:
                subprocess.check_output(['git', 'checkout', revision_or_path],
                                        stderr=subprocess.PIPE,
                                        cwd=self.path)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to checkout revision {revision_or_path}\n{e}")

        self.logger.info(f"Building yugabyte from source code {self.path}")
        subprocess.check_output(['./yb_build.sh',
                                 'release', '--no-tests', '--skip-java-build'],
                                stderr=subprocess.PIPE,
                                cwd=self.path)

    def call_upgrade_ysql(self):
        pass
