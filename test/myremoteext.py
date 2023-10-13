class MyRemoteExtension:
    def identify(self, url):
        return url.startswith('myremote:')

    def pull(self, archive, product, target_dir):
        assert False

    def set_configuration(self, configuration):
        assert int(configuration['timeout']) == 17


_remote_extensions = {
    'myremote': MyRemoteExtension()
}

def remote_backends():
    return _remote_extensions.keys()

def remote_backend(name):
    return _remote_extensions[name]
