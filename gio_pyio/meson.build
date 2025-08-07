module = python.extension_module('_gio_pyio',
  sources: files(
    'gio_pyio.c',
    'streamwrapper.c',
  ),
  dependencies: [glib, gio, gio_unix, pygobject, python.dependency()],
       install: true,
        subdir: 'gio_pyio',
)

python.install_sources(
  files('__init__.py'),
  subdir: 'gio_pyio',
)
