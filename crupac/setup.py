import setuptools

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(
    name="samuko", # Replace with your username
    version="0.0.1",
    author="Sam Kohler",
    author_email="sam.kohler@cru.org",
    description="Cru custom Python code for modules and Utils a PyQt5 GUI wrapper for Cru Ansible playbooks",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/CruGlobal/cru-ansible-modules/crupac",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT (X11) License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
