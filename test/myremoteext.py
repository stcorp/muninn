class MyRemoteExtension:
    def identify(self, url):
        return url.startswith('myremote:')

    def pull(self, archive, product, target_dir):
        assert False



_remote_extensions = {
    'myremote': MyRemoteExtension()
}

def remote_backends():
    return _remote_extensions.keys()

def remote_backend(name, configuration):
    assert int(configuration['timeout']) == 17

    return _remote_extensions[name]
