import os

from logger import logger


class HostManager:
    def __init__(self):
        self.hosts = os.environ.get("HOSTS").split(",")
        self.hostindex = 0
        self.currentstate = "success"

    def get_host(self):
        return self.hosts[self.hostindex]

    def conn_fail(self):
        self.currentstate = "fail"

    def decide(self):
        if self.currentstate == "success":
            # reset hostindex to 0 because we prefer 0 and want to try if its back up again
            self.hostindex = 0
        else:
            # increment hostindex if connection failed
            # and loop back to 0 if we reached the end
            logger.debug(f"Encountered connection error with host {self.hosts[self.hostindex]}")
            self.hostindex += 1
            if self.hostindex >= len(self.hosts):
                self.hostindex = 0
            logger.debug(f"Trying host {self.hosts[self.hostindex]}")
        self.currentstate = "success"
