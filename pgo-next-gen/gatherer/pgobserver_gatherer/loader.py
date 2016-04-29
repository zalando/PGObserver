import importlib
#from pgobserver_gatherer.plugins.console

# module = importlib.import_module('my_package.my_module')
# my_class = getattr(module, 'MyClass')
# my_instance = my_class()
def run():
    plugins = ['console']
    module = importlib.import_module('pgobserver_gatherer.plugins.console.handler')

    my_class = getattr(module, 'Handler')
    my_instance = my_class()
    my_instance.handle('data')
