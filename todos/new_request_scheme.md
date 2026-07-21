# Necroflow Design Note: Tuple-Based Pipeline Keys

## Goal

Provide a simple, Pythonic mechanism for identifying pipeline results
that:

-   does not reinvent filesystem paths;
-   works naturally with loops;
-   is easy to serialize into `job.toml`;
-   keeps computational identity separate from user-facing identifiers.

## Core Principle

Pipeline keys are **tuples**.

For convenience, singleton keys supplied in Python are automatically
converted into 1-element tuples.

Examples:

``` python
P["merged"] = merged          # internally ("merged",)
P[sample.name] = bam          # internally (sample.name,)
P[sample.name, method.name] = result
```

Internally:

``` python
def _normalize_key(key):
    return key if isinstance(key, tuple) else (key,)
```

Thus the canonical key representation is always:

``` python
tuple[Hashable, ...]
```

## Motivation

Loops naturally generate indexed computations:

``` python
for sample in samples:
    for method in methods:
        P[sample.name, method.name] = align(...)
```

The tuple is simply the user's chosen identifier. Necroflow does not
interpret the meaning of individual components.

Unlike Snakemake:

-   keys are **not** output paths;
-   keys do **not** generate jobs;
-   keys do **not** determine storage locations.

Jobs already exist because ordinary Python executed the loop.

## Keys are Aliases

Pipeline keys are user-facing aliases only.

They must **not** influence cache fingerprints or computational
identity.

Node identity continues to depend only on:

-   rule
-   parameters
-   dependencies
-   execution context

## Uniqueness

Each key must be unique within a pipeline.

Attempting to bind two different nodes to the same key is an error.

## Job Configuration

The job file serializes tuple keys directly.

``` toml
request = [
    ["merged"],
    ["sample_A", "standard"],
    ["sample_B", "filtered", "replicate_2"],
]
```

Each inner array becomes a tuple and is resolved as:

``` python
P["merged"]
P["sample_A", "standard"]
P["sample_B", "filtered", "replicate_2"]
```

## Advantages

-   Extremely small conceptual model.
-   No filesystem-like namespaces.
-   No subpipeline abstraction.
-   No wildcard language.
-   Natural support for loop-generated results.
-   Pythonic API using normal indexing syntax.
-   Straightforward serialization.

## Open Questions

-   Support partial tuple matching later?
-   Restrict key components to TOML-serializable scalar types?
-   Allow multiple aliases for the same node?
