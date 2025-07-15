import sys
from typing import List, Dict
from .core import Database, Field

STORAGE_DIR = "./.baksadb_files"


def validate_type(typ: str) -> bool:
    return typ in {"int", "double", "bool", "string"}


def parse_schema(schema_str: str) -> List[Field]:
    fields = []
    parts = schema_str.split(',')

    for part in parts:
        part = part.strip()
        if not part:
            continue

        is_primary = False
        if part.startswith('@'):
            is_primary = True
            part = part[1:]

        if '=' in part:
            name, field_type = map(str.strip, part.split('=', 1))
        elif ':' in part:
            name, field_type = map(str.strip, part.split(':', 1))
        else:
            raise ValueError(f"Invalid field format: '{
                             part}', expected name=type or name:type")

        if not validate_type(field_type):
            raise ValueError(f"Unsupported type '{field_type}'")

        fields.append(Field(name, field_type, is_primary))

    return fields


def parse_key_value_pairs(pairs_str: str) -> Dict[str, str]:
    """
    Parses key=value,key2=value2 strings into dictionary.
    """
    updates = {}
    pairs = pairs_str.split(',')
    for pair in pairs:
        pair = pair.strip()
        if not pair:
            continue
        if '=' not in pair:
            raise ValueError(f"Expected key=value format in '{pair}'")
        k, v = map(str.strip, pair.split('=', 1))
        updates[k] = v
    return updates


def parse_args(args: List[str]) -> Dict:
    if len(args) < 2:
        raise ValueError("Not enough arguments provided.")

    operation = args[1].upper()

    result = {"operation": operation}

    match operation:
        case "TABLES":
            pass
        case "CREATE":
            if len(args) < 4:
                raise ValueError("CREATE requires table name and schema.")
            table_name = args[2]
            schema_str = args[3]
            schema = parse_schema(schema_str)
            result.update({"table_name": table_name, "schema": schema})
        case "DELETE":
            if len(args) < 3:
                raise ValueError(
                    "DELETE requires 'TABLE' or 'ROW' + arguments")
            sub_operation = args[2].upper()
            result["sub_operation"] = sub_operation

            if sub_operation == "TABLE":
                if len(args) < 4:
                    raise ValueError("DELETE TABLE requires table name")
                table_name = args[3]
                result["table_name"] = table_name

            elif sub_operation == "ROW":
                if len(args) < 6:
                    raise ValueError(
                        "DELETE ROW requires table name and primary key value")
                table_name = args[3]
                pk_value = args[4]
                result.update(
                    {"table_name": table_name, "pk_value": pk_value})

            else:
                raise ValueError(
                    f"Unsupported DELETE sub-operation '{sub_operation}'")
        case "UPDATE":
            if len(args) < 3:
                raise ValueError(
                    "UPDATE requires 'SCHEMA' or 'ROW' + arguments")
            sub_operation = args[2].upper()
            result["sub_operation"] = sub_operation

            if sub_operation == "SCHEMA":
                if len(args) < 5:
                    raise ValueError(
                        "UPDATE SCHEMA requires table name and schema to add")
                table_name = args[3]
                schema_str = args[4]
                schema = parse_schema(schema_str)
                result.update({"table_name": table_name, "schema": schema})

            elif sub_operation == "ROW":
                if len(args) < 6:
                    raise ValueError(
                        "UPDATE ROW requires table name, primary key and key=value pairs")
                table_name = args[3]
                pk_value = args[4]
                updates_str = args[5]
                updates = parse_key_value_pairs(updates_str)
                result.update(
                    {"table_name": table_name, "pk_value": pk_value, "updates": updates})
            else:
                raise ValueError(
                    f"Unsupported UPDATE sub-operation '{sub_operation}'")
        case "INSERT":
            if len(args) < 4:
                raise ValueError(
                    "INSERT requires table name and key=value pairs")
            table_name = args[2]
            row_str = args[3]
            row = parse_key_value_pairs(row_str)
            result.update({"table_name": table_name, "row": row})
        case _:
            raise ValueError(f"Unsupported operation '{operation}'")

    return result


def main():
    db = Database(storage_dir=STORAGE_DIR)

    try:
        cmd = parse_args(sys.argv)
        op = cmd["operation"]

        if op == "CREATE":
            table = db.create_table(cmd["table_name"], cmd["schema"])
            print(f"Created table:\n{table}")

        elif op == "DELETE":
            sub_op = cmd["sub_operation"]
            if sub_op == "TABLE":
                db.drop_table(cmd["table_name"])
                print(f"Dropped table '{cmd['table_name']}'")
            elif sub_op == "ROW":
                table = db.get_table(cmd["table_name"])
                if not table:
                    print(f"Table '{cmd['table_name']}' not found")
                    return
                success = table.delete_row(cmd["pk_value"])
                if success:
                    db.save_table(table)
                    print(f"Deleted row with primary key {
                          cmd['pk_value']} from {cmd['table_name']}")
                else:
                    print(f"Row with primary key {cmd['pk_value']} not found")

        elif op == "UPDATE":
            sub_op = cmd["sub_operation"]
            if sub_op == "SCHEMA":
                table = db.get_table(cmd["table_name"])
                if not table:
                    print(f"Table '{cmd['table_name']}' not found")
                    return
                for new_field in cmd["schema"]:
                    table.add_column(new_field)
                db.save_table(table)
                print(f"Updated schema of table '{
                      cmd['table_name']}'. Now fields are:")
                for f in table.fields:
                    print(f" - {f}")
            elif sub_op == "ROW":
                table = db.get_table(cmd["table_name"])
                if not table:
                    print(f"Table '{cmd['table_name']}' not found")
                    return
                success = table.update_row(cmd["pk_value"], cmd["updates"])
                if success:
                    db.save_table(table)
                    print(f"Updated row with primary key {
                          cmd['pk_value']} in {cmd['table_name']}")
                else:
                    print(f"Row with primary key {cmd['pk_value']} not found")

        elif op == "INSERT":
            table = db.get_table(cmd["table_name"])
            if not table:
                print(f"Table '{cmd['table_name']}' not found")
                return
            table.insert(cmd["row"])
            db.save_table(table)
            print(f"Inserted row into '{cmd['table_name']}': {cmd['row']}")
        elif op == "TABLES":
            if len(sys.argv) < 3:
                for x in db.tables.keys():
                    print(x)
            else:
                table_name = sys.argv[2]
                if table_name in db.tables:
                    print(db.tables[table_name])
        else:
            print(f"Operation {op} not implemented")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
