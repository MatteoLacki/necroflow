# Bioconda Packaging

This document records the initial Bioconda submission and the maintenance
workflow for Necroflow. PyPI remains the upstream release source; Bioconda
packages the PyPI source distribution for Conda users.

## Initial Submission

The initial recipe has been prepared in the separate fork
[`MatteoLacki/bioconda-recipes`](https://github.com/MatteoLacki/bioconda-recipes):

- Branch: `add-necroflow`
- Commit: `cf48d09c Add necroflow recipe`
- Recipe: `recipes/necroflow/meta.yaml`
- Packaged version: `0.0.3`
- PyPI sdist SHA-256:
  `84447a6521359c9df6846c4743f85805e16aefe3839876a3a2abffd1ce30c197`

The recipe is a pure-Python `noarch: python` package. It declares Python
`>=3.10`, `tomlkit`, and `exceptiongroup`; it tests the import, the
`necroflow --help` command, and `pip check`.

Open the pull request with:

<https://github.com/bioconda/bioconda-recipes/compare/master...MatteoLacki:bioconda-recipes:add-necroflow?expand=1>

Use `bioconda/bioconda-recipes:master` as the base, title it `Add necroflow`,
and state that the recipe builds the PyPI source distribution and was tested
locally. Bioconda CI runs linting plus platform builds and tests automatically.
When all checks are green, comment:

```text
@BiocondaBot please add label
```

This requests review by a Bioconda maintainer. Address review comments; a
Bioconda maintainer will merge the recipe.

## Local Validation

The commands below use Micromamba and the same `bioconda-utils` tooling used
by Bioconda. Keep the recipe fork separate from the Necroflow checkout.

```bash
git clone git@github.com:MatteoLacki/bioconda-recipes.git /tmp/bioconda-recipes
cd /tmp/bioconda-recipes
git switch add-necroflow

micromamba create -y -p /tmp/necroflow-bioconda-env \
  -c conda-forge -c bioconda bioconda-utils

micromamba run -p /tmp/necroflow-bioconda-env \
  conda build recipes/necroflow --override-channels -c conda-forge -c bioconda
```

`conda build` runs the recipe's `test:` section. The first solve can spend a
few minutes at `Attempting to finalize metadata for necroflow`; high CPU use at
that stage is expected. The generic warning about a default NumPy variant is
also harmless: Necroflow does not use NumPy, and the recipe must not add it.

The resulting artifact is normally:

```text
/tmp/necroflow-bioconda-env/conda-bld/noarch/necroflow-0.0.3-py_0.conda
```

Verify the artifact rather than the source checkout in a fresh environment:

```bash
micromamba create -y -p /tmp/necroflow-bioconda-test \
  -c file:///tmp/necroflow-bioconda-env/conda-bld \
  -c conda-forge -c bioconda necroflow=0.0.3

micromamba run -p /tmp/necroflow-bioconda-test \
  python -c 'import necroflow; print(necroflow.__version__)'
micromamba run -p /tmp/necroflow-bioconda-test necroflow --help
micromamba run -p /tmp/necroflow-bioconda-test python -m pip check
```

The initial recipe passed all of these checks.

## After Merge

Once Bioconda publishes the merged recipe, users can install it with:

```bash
micromamba create -n necroflow -c conda-forge -c bioconda necroflow
```

Bioconda supports Linux and macOS. It does not provide native Windows packages;
use WSL on Windows, consistent with Necroflow's POSIX support.

## Release Updates

Publish each new Necroflow version to PyPI with an sdist. The main Bioconda
recipe uses a standard PyPI source URL, so Bioconda's autobump service should
detect a new release, update the recipe version and SHA-256, and open an update
pull request. Review that PR before merge, particularly when dependencies or
packaging metadata changed: the updater does not reason fully about dependency
changes.

If an update PR is not created, update `version` and `sha256` in
`recipes/necroflow/meta.yaml`, set `build.number` to `0`, validate locally, and
open an ordinary Bioconda PR. Do not change a published package's version in
place.
