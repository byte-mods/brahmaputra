from setuptools import setup, Extension

module1 = Extension('_bitpacker',
                    sources = ['bitpacker.c'])

setup (name = 'BitPacker',
       version = '1.0',
       description = 'BitPacker C Extension',
       ext_modules = [module1])
