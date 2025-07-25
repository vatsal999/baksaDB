from typing import List, Dict, Any, Optional
from ..indexing.hash_index import HashIndex


class Field:
    def __init__(self, name: str, typ: str, is_primary: bool = False):
        self.name = name
        self.type = typ
        self.is_primary = is_primary

    def __repr__(self):
        pk = " (PRIMARY KEY)" if self.is_primary else ""
        return f"\"{self.name}\" {self.type}{pk}"


class Table:
    """
    params:
    name : str = table name
    fields : List[Field] = list of Fields of the table
    data : List[Dict[str,Any]] = data of the table
    """

    def __init__(self, name: str, fields: List[Field]):
        self.name = name
        self.field_list = fields
        self.data: List[Dict[str, Any]] = []
        self.hash_index = HashIndex()

        # fill the fields
        for f in fields:
            if f.is_primary:
                self.primary_key_field = f.name

        if not self.primary_key_field:
            raise ValueError(
                f"Table '{name}' must have one primary key field.")

        # fill hash index
        for row in self.data:
            # NOTE: this is important
            # we are inserting the reference, not the WHOLE row data
            self.hash_index.insert(row[self.primary_key_field], row)

    def __repr__(self):
        fields_repr = "\n".join(repr(f) for f in self.field_list)
        return f"<Table \"{self.name}\">\n{fields_repr}"

    def insert(self, row: Dict[str, Any]):
        # check any missing fields
        for field in self.field_list:
            if field.name not in row:
                raise ValueError(f"Missing value for field '{field.name}'")

        self.data.append(row)

    def delete_row(self, pk_value: Any) -> bool:
        initial_len = len(self.data)
        self.data = [
            r for r in self.data if r[self.primary_key_field] != pk_value]
        return len(self.data) < initial_len

    def update_row(self, pk_value: Any, updates: Dict[str, Any]) -> bool:
        updated = False
        for row in self.data:
            if row[self.primary_key_field] == pk_value:
                for k, v in updates.items():
                    if k == self.primary_key_field:
                        raise ValueError("Cannot update primary key field")
                    if k not in [f.name for f in self.field_list]:
                        raise ValueError(
                            f"Field '{k}' does not exist in table")
                    row[k] = v
                updated = True
                break
        return updated
