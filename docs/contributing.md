# Contributing to cmsgov

Please note that if you are not a contributor on this repository, you can
still create pull requests, however you will need to fork this project, push
changes to your fork, and create a pull request from your forked repository.

## Creating a Pull Request

1.  Install [hatch](https://hatch.pypa.io/latest/install/), if you have not
    already done so.

2.  If you are using Windows, make sure you've installed `make`:

    ```bash
    winget install -e --id ezwinports.make
    ```

3.  Clone and Install

    To install this project for development of *this library*,
    clone this repository (replacing "~/Code", below, with the directory
    under which you want your project to reside), then run `make`:

    ```bash
    cd ~/Code && \
    git clone\
    github.com/enorganic/cmsgov.git cmsgov && \
    cd cmsgov && \
    make
    ```

4.  Create a new branch for your changes (replacing "descriptive-branch-name"
    with a *descriptive branch name*):

    ```bash
    git branch descriptive-branch-name
    ```

5.  Set environment variables for the following API keys (or create a `.env`
    file in the project root) with the following environment variables set:

    ```text
    CMS_GOV_MARKETPLACE_API_KEY=***
    ```

6.  Make some changes.

7.  Format and lint your code:

    ```bash
    make format
    ```

6.  Test your changes:

    ```bash
    make test
    ```

7.  Push your changes and create a pull request.

## Adding a CMS.gov API

Each CMS.gov API should have a client and model module for each major API
version, and these should be automatically generated as part of
`scripts/remodel.py`, which is run with the `make remodel` target.
Modify `scripts/remodel.py` to add new APIs, referencing the inline
documentation/comments in that file for implementation details. Client and
model modules should not be edited manually after creation except to rename
model classes (renamed model classes will retain their names when
`make remodel` is subsequently executed, as will client parameters
derived from these mdoel classes).

### Package Structure

CMS.gov has multiple APIs, each described by an OpenAPI document. Each
API corresponds to a python sub-package under `cmsgov`, which you can find
in `src/cmsgov` in this repository. Within each API package, there will be
a version package, under which you will find a client and model module,
for example, the following shows the directory structure for the Provider Data
API:

```bash
$ tree -a -I '*.pyc' -I '__pycache__' src/cmsgov/provider_data
src/cmsgov/provider_data
├── __init__.py
├── py.typed
└── v1
    ├── __init__.py
    ├── client.py
    ├── model.py
    └── py.typed
```

### API Versions

A versioned (v1, v2, etc.) sub-package will correspond to each API major
version of a CMS.gov API, each generated from their own Open API document. Any
modules authored directly under the API sub-package, outside of a version
sub-package, should be authored such as to support all published API versions
which support the operation, varying logic based on on the source of the client
or model instance passed to the function or class, and raising errors when
attempting unsupported operations for the API version.

