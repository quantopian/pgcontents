from __future__ import print_function
from setuptools import setup
from os.path import join, dirname, abspath


def main():
    reqs_file = join(dirname(abspath(__file__)), 'requirements.txt')
    with open(reqs_file) as f:
        requirements = [req.strip() for req in f.readlines()]

    setup(
        name='pgcontents',
        version='0.2',
        description="A Postgres-backed ContentsManager for IPython.",
        author="Scott Sanderson",
        author_email="ssanderson@quantopian.com",
        packages=[
            'pgcontents',
            'pgcontents/alembic',
            'pgcontents/alembic/versions',
            'pgcontents/tests/',
            'pgcontents/utils/',
        ],
        license='Apache 2.0',
        include_package_data=True,
        zip_safe=False,
        url="https://github.com/quantopian/pgcontents",
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Framework :: IPython',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python',
            'Topic :: Database',
        ],
        install_requires=requirements,
        scripts=[
            'bin/pgcontents',
        ],
    )


if __name__ == '__main__':
    main()
