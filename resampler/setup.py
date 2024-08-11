from distutils.core import setup, Extension
from Cython.Build import cythonize
import numpy
import sys
import socket
import pyarrow as pa


# python setup.py build_ext --inplace" 

hostname = socket.gethostname()


python_module_link_args = list()
python_module_link_args.append("-Wl,-rpath,$ORIGIN")
pyarrow_module_link_args = python_module_link_args + ["-Wl,-rpath,$ORIGIN/pyarrow"]



if hostname.startswith('ip'):
    pyarrow_module_link_args.append("-L./home/ubuntu/miniconda/envs/options/lib/python3.9/site-packages/pyarrow/")

elif hostname == 'app':
    pyarrow_module_link_args.append("-L./opt/conda/envs/xlab/lib/python3.9/site-packages/pyarrow/")

else:
    pyarrow_module_link_args.append("-L/Users/krsna/miniconda3/envs/py3/lib/python3.10/site-packages/pyarrow")



if __name__ == '__main__':

    numpy_include_dir = numpy.get_include()
    debug = '--debug' in sys.argv


    print(debug)

    compiler_directives = {
        'embedsignature': True,
        'boundscheck': False,
        'wraparound': False,
        'language_level': 3,
    }

    if debug:
        compiler_directives.update({
            'profile': True,
            'linetrace': True,
            'binding': True
        })

        define_macros = [
            ('CYTHON_TRACE', 1),
        ]


        extra_compile_args = ['-O0']

    else:
        define_macros = []
        extra_compile_args = ['-O3']

    print(pa.get_libraries(), pa.get_library_dirs())
    ext_modules = [
        Extension(
            name=f.replace('.pyx', ''),
            sources=[f],
            language="c++",
            include_dirs=[numpy_include_dir, pa.get_include()],
            libraries=pa.get_libraries(),
            library_dirs=pa.get_library_dirs(),
            define_macros=define_macros,
            extra_compile_args=extra_compile_args,
            extra_link_args=pyarrow_module_link_args,

        )
        for f in ['resampler.pyx']
        if 'pyx' in f
    ]
    setup(ext_modules=cythonize(ext_modules,
                                compiler_directives=compiler_directives,
                                annotate=True
                                ))


