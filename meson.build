project(
  'gio_pyio',
  'c',
  version: run_command(
    find_program('python3', 'python'), '-m', 'setuptools_scm', check: true).stdout().strip(),
  default_options: ['warning_level=3'],
)

glib = dependency('glib-2.0')
gio = dependency('gio-2.0')
gio_unix = dependency('gio-unix-2.0')
pygobject = dependency('pygobject-3.0')
python = import('python').find_installation('python3', pure: false)

subdir('gio_pyio')
