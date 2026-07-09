from pathlib import Path


def validate(config: dict) -> None:
    if "input" not in config:
        raise ValueError("missing required key: input")
    if not Path(config["input"]).exists():
        raise ValueError(f"input file does not exist: {config['input']}")
