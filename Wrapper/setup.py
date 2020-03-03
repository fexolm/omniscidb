from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

ext_modules = [Extension("dbe",
                     ["dbe.pyx"],
                     language='c++',
                     extra_compile_args=["-std=c++17", "-fPIC", "-pie", "-lc"],
                     include_dirs=['../', '/nfs/site/home/gpetrova/miniconda3/envs/ibis-dev/include/'],
                     library_dirs=['./', '../build/Wrapper', '/usr/local/mapd-deps/lib', '/lib/x86_64-linux-gnu', '/usr/lib/x86_64-linux-gnu'],
                     libraries=['DBEngine', 'arrow'],
                     )]

setup(
  name = 'dbe',
  cmdclass = {'build_ext': build_ext},
  ext_modules = cythonize(ext_modules, compiler_directives={"c_string_type": "str", "c_string_encoding": "utf8", "language_level": "3"})
)