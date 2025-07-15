"""
miniDB - A simple modular database project.

Contains the core database functionality, CLI interface, and support modules.
"""

from .core.core import Database, Table, Field
from .core.cli import main  # entry point for CLI

__all__ = [
    "Database",
    "Table",
    "Field",
    "main",
]
