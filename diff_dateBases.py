import os
import re

def read_schema_file(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()

def compare_schemas(source_schema, target_schema):
    source_tables = re.findall(r"CREATE TABLE `(\w+)`", source_schema)
    target_tables = re.findall(r"CREATE TABLE `(\w+)`", target_schema)
    
    new_tables = set(source_tables) - set(target_tables)
    column_changes = {}

    for table_name in set(source_tables).intersection(target_tables):
        source_table_definition = re.search(rf"CREATE TABLE `{table_name}`(.*?);", source_schema, re.DOTALL).group(1)
        target_table_definition = re.search(rf"CREATE TABLE `{table_name}`(.*?);", target_schema, re.DOTALL).group(1)

        source_columns = re.findall(r"`(\w+)`\s+.*?(?:,\n|$)", source_table_definition)
        target_columns = re.findall(r"`(\w+)`\s+.*?(?:,\n|$)", target_table_definition)

        columns_diff = []
        new_columns = []

        for source_column in source_columns:
            if source_column not in target_columns:
                columns_diff.append(source_column)
                new_columns.append(re.search(rf"`{source_column}`\s+.*?(,\n|$)", source_table_definition).group(0))

        if columns_diff:
            column_changes[table_name] = new_columns

    return new_tables, column_changes


def generate_ddl(new_tables, column_changes, source_schema):
    ddl_changes = []

    for table_name in new_tables:
        create_table_definition = re.search(rf'CREATE TABLE `{table_name}`(.*?);', source_schema, re.DOTALL).group(1)
        ddl_changes.append(f"CREATE TABLE `{table_name}` ({create_table_definition});")

    for table_name, columns in column_changes.items():
        for column_definition in columns:
            ddl_changes.append(f"ALTER TABLE `{table_name}` ADD COLUMN {column_definition.strip()};")

    return "\n\n".join(ddl_changes)

def main():
    script_directory = os.path.dirname(os.path.abspath(__file__))
    source_schema_text = read_schema_file(os.path.join(script_directory, "source_schema.sql"))
    target_schema_text = read_schema_file(os.path.join(script_directory, "target_schema.sql"))

    new_tables, column_changes = compare_schemas(source_schema_text, target_schema_text)
    ddl_changes = generate_ddl(new_tables, column_changes, source_schema_text)

    output_file_path = os.path.join(script_directory, "ddl_changes.sql")
    with open(output_file_path, 'w', encoding='utf-8') as output_file:
        output_file.write(ddl_changes)

    print(f"DDL changes saved to: {output_file_path}")

if __name__ == "__main__":
    main()
