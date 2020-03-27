import contextlib
import datetime
import os
import shutil
import sqlite3

import click

from . import __version__

# Shotwell DB tables that contain media files.
# NOTE: Shotwell's database description is available here:
# https://wiki.gnome.org/Apps/Shotwell/Architecture/Database.
_MEDIA_TABLES = [
    {'name': 'PhotoTable', 'filepath_column': 'filename'},
    {'name': 'VideoTable', 'filepath_column': 'filename'},
    {'name': 'BackingPhotoTable', 'filepath_column': 'filepath'},
    {'name': 'TombstoneTable', 'filepath_column': 'filepath'},
]
# List of supported Shotwell DB schema versions.
_SUPPORTED_SCHEMA_VERSIONS = [
    22,
]

@click.command()
@click.option(
    '--db-file',
    type=click.Path(exists=True, dir_okay=False),
    default='~/.local/share/shotwell/photo.db',
    help="Path to Shotwell's DB file.",
    show_default=True,
)
@click.option(
    '--schema-check/--no-schema-check',
    default=True,
    help="Check if the version of Shotwell's DB schema is supported.",
    show_default=True,
)
@click.option(
    '--ignore-different-prefix',
    is_flag=True,
    help="Ignore media files in Shotwell's DB that don't start with OLD_PREFIX.",
    show_default=True,
)
@click.option(
    '--backup/--no-backup',
    default=True,
    help="Back up Shotwell's DB file.",
    show_default=True,
)
@click.argument(
    'old-prefix',
)
@click.argument(
    'new-prefix',
)
@click.version_option(version=__version__)
def main(db_file, schema_check, ignore_different_prefix, backup, old_prefix, new_prefix):
    """Tool for changing paths of media files stored in Shotwell's database."""
    # Create a backup of Shotwell's DB file.
    if backup:
        shutil.copy2(db_file, '{}.{}.backup'.format(db_file, datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
    with contextlib.closing(sqlite3.connect(db_file)) as con:
        # Use connection as a context manager so that changes are automatically
        # committed or rolled-back (in the event of an exception).
        with con:
            cur = con.cursor()
            # Check if db_file is a Shotwell DB.
            try:
                cur.execute('SELECT schema_version from VersionTable')
            except sqlite3.Error as e:
                raise click.ClickException(
                    f"Couldn't obtain VersionTable from the DB. Is {db_file} a Shotwell DB?"
                )
            # Check if schema version query results are valid.
            schema_version_rows = cur.fetchall()
            if len(schema_version_rows) != 1 or len(schema_version_rows[0]) != 1:
                raise click.ClickException(
                    f"Invalid schema version query results. File {db_file} might be corrupted."
                )
            schema_version = schema_version_rows[0][0]
            # Check if given schema version is supported.
            if schema_check and schema_version not in _SUPPORTED_SCHEMA_VERSIONS:
                raise click.ClickException(
                    "Schema version {} not supported (supported versions: {}).".format(
                        schema_version,
                        ", ".join(map(str, _SUPPORTED_SCHEMA_VERSIONS)),
                    )
                )

            # Check if there are media files that are not prefixed with the old path prefix.
            if not ignore_different_prefix:
                for table in _MEDIA_TABLES:
                    table_name, filepath_column = table['name'], table['filepath_column']
                    try:
                        # TODO: Figure out why using qmary style or named style placeholders don't work.
                        cur.execute(f"SELECT {filepath_column} from {table_name} WHERE {filepath_column} NOT LIKE '{old_prefix}%'")
                    except sqlite3.Error as e:
                        raise click.ClickException(
                            f"Couldn't obtain files from {table_name}:\n{e}"
                        )
                    files_with_different_prefix = cur.fetchall()
                    num_files_with_different_prefix = len(files_with_different_prefix)
                    if num_files_with_different_prefix > 0:
                        trim_length = 50
                        exception_suffix = "\n\n ... (output trimmed) ...\n"
                        if num_files_with_different_prefix < trim_length:
                            trim_length = num_files_with_different_prefix
                            exception_suffix = ""
                        raise click.ClickException(
                            f"Detected {num_files_with_different_prefix} files with different "
                            f"prefix in {table_name}:\n"
                            + "\n".join(map(lambda x: x[0], files_with_different_prefix[:trim_length]))
                            + exception_suffix
                        )

            # Replace old path prefix with new prefix in all media files tables.
            for table in _MEDIA_TABLES:
                table_name, filepath_column = table["name"], table["filepath_column"]
                try:
                    cur.execute(f"UPDATE {table_name} SET {filepath_column}=replace({filepath_column}, ?, ?)", (old_prefix, new_prefix))
                except sqlite3.Error as e:
                    raise click.ClickException(
                        f"Couldn't replace {old_prefix} with {new_prefix} for media files in {table_name}."
                    )

    click.echo(
        f"Successfully replaced old prefix {old_prefix} with new prefix {new_prefix} in media "
        f"files stored in Shotwell's database located at {db_file}."
    )
