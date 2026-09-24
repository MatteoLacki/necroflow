"""Build a rule chain with a rebound local variable and one public label."""

from necroflow import DAG, NodeType, Pipeline, command, output, text_file, workflow


class Text(NodeType):
    filename = "text.txt"


@text_file
def write_text(text: str):
    """Write the configured source text."""
    result = output(Text)
    return result


@command("tr '[:lower:]' '[:upper:]' < {source} > {result}")
def uppercase(source: Text):
    """Convert the source text to uppercase."""
    result = output(Text)
    return result


@command("sed 's/^/RESULT: /' {source} > {result}")
def add_prefix(source: Text):
    """Prefix every line of the source text."""
    result = output(Text)
    return result


@workflow
def local_variable_pipeline(P: Pipeline, config: dict) -> None:
    """Rebind one local name through the chain and label only its result."""
    current = write_text(text=config["text"] + "\n")
    current = uppercase(current)
    current = add_prefix(current)
    # THIS WILL NOT WORK WITH P.current:
    # P.current = write_text(text=config["text"] + "\n")
    # P.current = uppercase(current) # BOOM!
    # second time P.current occurs as assignment against the key uniqueness principle
    # needed to assure pipeline nodes are uniquely requestable.
    P.result = current


def build(outdir="nodes") -> tuple[DAG, Pipeline]:
    """Construct the example DAG without executing it."""
    dag = DAG(outdir)
    pipeline = Pipeline(dag)
    local_variable_pipeline(pipeline, {"text": "hello"})
    pipeline.finish()
    dag.require(pipeline.sinks())
    return dag, pipeline


if __name__ == "__main__":
    dag, pipeline = build()
    dag.run()
    print(pipeline.result.path.read_text(), end="")
