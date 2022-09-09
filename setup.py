import io
import setuptools
from setuptools import setup

def readme():
    with io.open('README.md', 'r', encoding='utf-8') as f:
        return f.read()
    
def readversion():
    with io.open('VERSION', 'r', encoding='utf-8') as f:
        return f.read().strip()

params = {
    'name':'pgeodistance',
    'version':readversion(),
    'description':'Compute global postal code geo distance',
    'long_description':readme(),
    'long_description_content_type':"text/markdown",
    'classifiers':[
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Operating System :: MacOS'
    ],
    'keywords':'metric',
    'url':'',
    'author':'Sparkle',
    'author_email':'jyao@squareup.com',
    'packages':setuptools.find_packages(),
    'install_requires':[],
    'include_package_data':True,
    'zip_safe':False
}

if __name__ == '__main__':
    setup(**params)
