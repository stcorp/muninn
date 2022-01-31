class MyHookExtension(object):
    def post_create_hook(self, archive, product):
        pass

    def post_ingest_hook(self, archive, product):
        pass

    def post_pull_hook(self, archive, product):
        pass

    def post_remove_hook(self, archive, product):
        pass


_hook_extensions = {
    'myhooks': MyHookExtension()
}


def hook_extensions():
    return _hook_extensions.keys()


def hook_extension(name):
    return _hook_extensions[name]
