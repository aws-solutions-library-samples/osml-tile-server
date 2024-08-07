#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from threading import Thread
from unittest import TestCase, main

from pythonjsonlogger.jsonlogger import JsonFormatter


class TestThread(TestCase):
    """Unit tests for threading and logger configuration utilities."""

    def test_filter_adds_thread_local_context(self):
        """Test that ThreadingLocalContextFilter adds thread-local context to log records."""
        from aws.osml.tile_server.utils import ThreadingLocalContextFilter

        context_filter = ThreadingLocalContextFilter(attribute_names=["context_value"])

        # Set context in the main thread
        context_filter.set_context({"context_value": "A"})
        test_log_record = logging.LogRecord("test-name", logging.DEBUG, "some_module.py", 1, "test message", None, None)

        self.assertTrue(context_filter.filter(test_log_record))
        self.assertEqual(test_log_record.context_value, "A")

        # Set context in a separate thread
        thread_log_record = logging.LogRecord("test-name", logging.DEBUG, "some_module.py", 1, "test message", None, None)

        def sample_task():
            context_filter.set_context({"context_value": "B"})
            self.assertTrue(context_filter.filter(thread_log_record))

        thread = Thread(target=sample_task)
        thread.start()
        thread.join()

        self.assertEqual(thread_log_record.context_value, "B")

        # Ensure the main thread context is unchanged
        test_log_record = logging.LogRecord("test-name", logging.DEBUG, "some_module.py", 1, "test message", None, None)
        self.assertTrue(context_filter.filter(test_log_record))
        self.assertEqual(test_log_record.context_value, "A")

    def test_configure_logger(self):
        """Test the logger configuration with a custom formatter and filter."""
        from aws.osml.tile_server.utils import ThreadingLocalContextFilter, configure_logger

        logger = logging.getLogger("config_test")
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)

        default_formatter = JsonFormatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
        worker_filter = ThreadingLocalContextFilter(["correlation_id"])

        configure_logger(logger, logging.INFO, log_formatter=default_formatter, log_filter=worker_filter)

        self.assertEqual(logger.handlers[0].formatter, default_formatter)
        self.assertEqual(logger.filters, [worker_filter])


if __name__ == "__main__":
    main()
