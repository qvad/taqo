import os
import shutil
import subprocess
from time import sleep


def factory(config):
    if config.yugabyte_code_path is None:
        return YugabyteDistributive(config)
    else:
        return YugabyteRepository(config)


class Yugabyte:
    path = None

    def __init__(self, config):
        pass

    def change_version_and_compile(self, revision=None):
        pass

    def destroy(self):
        subprocess.call(['bin/yugabyted', 'destroy'],
                        cwd=self.path, )

    def start_node(self):
        subprocess.call(['bin/yugabyted', 'start'],
                        cwd=self.path, )

        sleep(30)

    def stop_node(self):
        subprocess.call(['bin/yugabyted', 'stop'],
                        cwd=self.path, )

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
        subprocess.call(['bin/yb-admin', 'upgrade_ysql'],
                        cwd=self.path, )

    def unpack_release(self, path):
        print(f"Cleaning /tmp/taqo direactory and unzip {path}")
        shutil.rmtree('/tmp/taqo', ignore_errors=True)
        os.mkdir('/tmp/taqo')
        subprocess.call(['tar', '-xf', path, '-C', '/tmp/taqo'])

        self.path = '/tmp/taqo/' + list(os.walk('/tmp/taqo'))[0][1][0]


class YugabyteRepository(Yugabyte):
    def __init__(self, config):
        super().__init__(config)

        self.path = config.yugabyte_code_path

    def change_version_and_compile(self, revision_or_path=None):
        if revision_or_path:
            subprocess.call(['git', 'checkout', revision_or_path], cwd=self.path)
        subprocess.call(['./yb_build.sh',
                         'release', '--no-tests', '--skip-java-build'],
                        cwd=self.path)

    def call_upgrade_ysql(self):
        pass
