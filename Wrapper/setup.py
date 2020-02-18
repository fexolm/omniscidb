from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

ext_modules = [Extension("dbe",
                     ["dbe.pyx"],
                     language='c++',
                     extra_compile_args=["-std=c++17"],
                     include_dirs=['../'],
                     library_dirs=['./', '/usr/local/mapd-deps/lib', '/localdisk/gal/root/igalink/omniscidb/build/Wrapper'],
                     libraries=['DBEngine'],
                     )]

setup(
  name = 'dbe',
  cmdclass = {'build_ext': build_ext},
  ext_modules = cythonize(ext_modules, compiler_directives={"c_string_type": "str", "c_string_encoding": "utf8", "language_level": "3"})
)