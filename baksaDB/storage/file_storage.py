import csv
import json
import os
from typing import Any
from ..core.models import Table, Field


class FileStorage:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _table_csv_path(self, table_name: str) -> str:
        safe_name = table_name.replace(" ", "_")
        return os.path.join(self.storage_dir, f"{safe_name}.csv")

    def _table_schema_path(self, table_name: str) -> str:
        safe_name = table_name.replace(" ", "_")
        return os.path.join(self.storage_dir, f"{safe_name}.schema.json")

    def save_table(self, table: Table) -> None:
        # Save schema metadata
        schema_path = self._table_schema_path(table.name)
        schema_data = {
            "fields": [
                {"name": f.name, "type": f.type, "is_primary": f.is_primary}
                for f in table.fields
            ]
        }
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(schema_data, f, indent=2)

        # Save data rows into CSV
        csv_path = self._table_csv_path(table.name)
        with open(csv_path, "w", newline='', encoding="utf-8") as csvfile:
            fieldnames = [f.name for f in table.fields]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in table.data:
                # Convert all values to strings for CSV
                writer.writerow(
                    {k: str(v) if v is not None else "" for k, v in row.items()})

    def load_table(self, table_name: str) -> Table:
        schema_path = self._table_schema_path(table_name)
        csv_path = self._table_csv_path(table_name)

        if not os.path.isfile(schema_path):
            raise FileNotFoundError(f"Schema file for table '{
                                    table_name}' not found")
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"Data CSV file for table '{
                                    table_name}' not found")

        # Load schema
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        fields = [
            Field(f["name"], f["type"], f["is_primary"]) for f in schema_json["fields"]
        ]
        table = Table(table_name, fields)

        # Load rows from CSV
        with open(csv_path, "r", newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Convert values from string to their proper type
                typed_row = {}
                for field in table.fields:
                    val = row.get(field.name, None)
                    if val == "":
                        typed_val = None
                    else:
                        typed_val = self._convert_value(val, field.type)
                    typed_row[field.name] = typed_val
                table.data.append(typed_row)

        # for rebuilding hash index
        table.rebuild_hash_index()
        return table

    def delete_table(self, table_name: str) -> None:
        for path_func in [self._table_csv_path, self._table_schema_path]:
            path = path_func(table_name)
            if os.path.isfile(path):
                os.remove(path)

    def _convert_value(self, val: str, typ: str) -> Any:
        if not typ:
            print("Got typ as none")
            return "None"

        if typ == "int":
            return int(val)
        elif typ == "double":
            return float(val)
        elif typ == "bool":
            return bool(val)
        elif typ == "string":
            return val
        else:
            return val
