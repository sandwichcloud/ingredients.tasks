"""
The code is derived from DJango's :module:`django.conf`.
"""
import importlib
import os

SETTINGS_MODULE_ENV = "SETTINGS_MODULE"


class SettingsLoader(object):
    def __init__(self):
        self.settings_module = os.environ.get(SETTINGS_MODULE_ENV)
        if not self.settings_module:
            raise ValueError(
                "Settings Environment Variable " + SETTINGS_MODULE_ENV + " is not set. Cannot load settings.")

    def load(self):
        global SETTINGS
        SETTINGS = Settings(self.settings_module)


class Settings(object):
    def __init__(self, settings_module):
        self.settings_module = settings_module
        mod = importlib.import_module(self.settings_module)

        for setting in dir(mod):
            if setting.isupper():
                setting_value = getattr(mod, setting)
                setattr(self, setting, setting_value)

    def __repr__(self):
        return '<%(cls)s "%(settings_module)s">' % {
            'cls': self.__class__.__name__,
            'settings_module': self.settings_module,
        }


SETTINGS = None
loader = SettingsLoader()
loader.load()
