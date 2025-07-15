from typing import List, Dict, Optional
from .models import Field, Table
from ..storage.file_storage import FileStorage


class Database:
    def __init__(self, storage_dir: Optional[str]):
        self.tables: Dict[str, Table] = {}
        if storage_dir:
            self.storage = FileStorage(storage_dir)
            self._load_tables()
        else:
            self.storage = None

    def _load_tables(self):
        import os

        for fname in os.listdir(self.storage.storage_dir):
            if fname.endswith(".schema.json"):
                # remove suffix and convert
                table_name = fname[:-12].replace("_", " ")
                try:
                    table = self.storage.load_table(table_name)
                    self.tables[table.name] = table
                except Exception as e:
                    print(f"Failed to load table {table_name}: {e}")

    def create_table(self, name: str, fields: List[Field]) -> Table:
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists.")
        table = Table(name, fields)
        self.tables[name] = table
        if self.storage:
            self.storage.save_table(table)
        return table

    def drop_table(self, name: str):
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist.")
        del self.tables[name]
        if self.storage:
            self.storage.delete_table(name)

    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)

    def save_table(self, table: Table):
        if self.storage:
            self.storage.save_table(table)

    def __repr__(self):
        table_list = ", ".join(self.tables.keys())
        return f"<Database tables: {table_list}>"
