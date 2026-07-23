from necroflow.tools.count_loc import count_lines, main


def test_count_lines_excludes_blanks_comments_and_generated_files(tmp_path):
    """Counts distinguish Python code from physical and non-Python text lines."""
    (tmp_path / "module.py").write_text("value = 1\n\n# comment\nvalue += 1\n")
    (tmp_path / "notes.md").write_text("heading\n")
    generated = tmp_path / "__pycache__"
    generated.mkdir()
    (generated / "ignored.py").write_text("ignored = True\n")
    egg_info = tmp_path / "package.egg-info"
    egg_info.mkdir()
    (egg_info / "ignored.txt").write_text("ignored\n")

    assert count_lines(tmp_path) == (2, 4, 5)


def test_main_prints_counts(tmp_path, capsys):
    """The command prints all three line-count summaries."""
    (tmp_path / "module.py").write_text("value = 1\n")

    main([str(tmp_path)])

    assert capsys.readouterr().out.splitlines() == [
        "Python code:          1",
        "Python physical:      1",
        "All supported text:   1",
    ]
