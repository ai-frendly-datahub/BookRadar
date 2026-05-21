from __future__ import annotations

from bookradar.logger import configure_logging, get_logger


def test_configure_logging_filters_below_level(capsys) -> None:
    configure_logging(log_level="WARNING", use_json=True)
    logger = get_logger("bookradar.tests.logger")

    logger.debug("debug_should_not_print")
    logger.info("info_should_not_print")
    logger.warning("warning_should_print")

    captured = capsys.readouterr()
    assert "warning_should_print" in captured.err
    assert "debug_should_not_print" not in captured.err
    assert "info_should_not_print" not in captured.err
