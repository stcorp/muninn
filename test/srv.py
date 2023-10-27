import rpyc
from rpyc.utils.server import ThreadedServer

import muninn

@rpyc.service
class DatabaseService(rpyc.Service):
    @rpyc.exposed
    def summary(self, where="", parameters=None, aggregates=None, group_by=None, group_by_tag=False,
                having=None, order_by=None):
        aggregates = list(aggregates) if aggregates else None
        group_by = list(group_by) if group_by else None

        return db.summary(where=where, parameters=parameters, aggregates=aggregates,
                          group_by=group_by, group_by_tag=group_by_tag, having=having,
                          order_by=order_by)

    @rpyc.exposed
    def search(self, *args, **kwargs):
        return db.search(*args, **kwargs)


if __name__ == "__main__":
    server = ThreadedServer(DatabaseService, port = 23456)
    db = muninn.open('my_arch')._database
    server.start()
