# Generic-Sage two-pass recalibration example (see ../../../necroflowpaper's Example S4).
# Ships a pinned, checksummed Sage release binary + the small Python deps this example's
# helper scripts need, plus a bundled public smoke-test dataset (lazear/sage's own CI
# fixture). See README.md for how to point this at your own data instead.
#
# Build context is the necroflow/ repo root (see docker-compose.yml's `context: ../..`),
# not this directory -- necroflow is installed from local source, not PyPI, because this
# example's `spectra: MzMlSpectra | MgfSpectra` union input needs a resolve_command()
# fix (necroflow/src/necroflow/dag.py) that isn't in a released version yet. Once a
# necroflow release including that fix ships, this should go back to
# `pip install necroflow==X.Y.Z` (see the diff this comment came from for the old form)
# and docker-compose.yml's build context back to `.`.

FROM python:3.12-slim

ARG SAGE_VERSION=v0.14.7
ARG SAGE_SHA256=e3dc6b41015cb167574f6c82525b75e946c094f30bd700271b05c051c30cbe8a
ARG SAGE_ASSET=sage-${SAGE_VERSION}-x86_64-unknown-linux-gnu.tar.gz

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pinned, checksum-verified Sage binary -- not `cargo install` from git HEAD, which drifts
# with no version pin. Fails closed (sha256sum -c) if the download doesn't match.
RUN curl -fL -o /tmp/sage.tar.gz \
      "https://github.com/lazear/sage/releases/download/${SAGE_VERSION}/${SAGE_ASSET}" \
    && echo "${SAGE_SHA256}  /tmp/sage.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/sage.tar.gz -C /tmp \
    && mv "/tmp/sage-${SAGE_VERSION}-x86_64-unknown-linux-gnu/sage" /usr/local/bin/sage \
    && chmod +x /usr/local/bin/sage \
    && rm -rf /tmp/sage.tar.gz "/tmp/sage-${SAGE_VERSION}-x86_64-unknown-linux-gnu"

# necroflow from local source (see comment at top of file for why, not a version pin).
# psims backs pyteomics' mzML reader.
COPY pyproject.toml README.md /tmp/necroflow-src/
COPY src/ /tmp/necroflow-src/src/
RUN pip install --no-cache-dir /tmp/necroflow-src pyteomics psims pandas numpy \
    && rm -rf /tmp/necroflow-src

WORKDIR /work
COPY examples/sage_recal/pipeline.py /work/pipeline.py
COPY examples/sage_recal/scripts/ /work/scripts/
COPY examples/sage_recal/jobs/q99536_example/ /work/jobs/q99536_example/
COPY examples/sage_recal/jobs/q99536_synthetic_100/ /work/jobs/q99536_synthetic_100/

ENTRYPOINT ["necroflow"]
