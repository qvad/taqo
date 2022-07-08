import subprocess
from time import sleep


class Yugabyte:
    def __init__(self, path):
        self.path = path

    def compile(self, revision=None):
        if revision:
            subprocess.call(['git', 'checkout', revision], cwd=self.path)
        subprocess.call(['./yb_build.sh',
                         'release', '--no-tests', '--skip-java-build'],
                        cwd=self.path)

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

        print("Sleep fo 60 seconds till continue test")
        sleep(60)

    def upgrade(self):
        subprocess.call(['bin/yb-admin', 'upgrade_ysql'],
                        cwd=self.path, )

