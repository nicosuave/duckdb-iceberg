# /// script
# dependencies = ["fastavro", "pyarrow"]
# ///
"""Build moved positional and mixed DV fixtures from the existing moved DV table."""

import copy
import json
import shutil
from pathlib import Path

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data/persistent/moved_deletion_vector_path"
TARGET = ROOT / "data/persistent/moved_position_deletes"


def read_avro(path):
    with path.open("rb") as stream:
        reader = fastavro.reader(stream)
        return reader.writer_schema, dict(reader.metadata), list(reader)


def write_avro(path, schema, metadata, rows):
    metadata.pop("avro.schema", None)
    metadata.pop("avro.codec", None)
    with path.open("wb") as stream:
        fastavro.writer(
            stream, schema, rows, metadata=metadata, sync_marker=b"position-deletes"
        )


(TARGET / "data").mkdir(parents=True, exist_ok=True)
(TARGET / "metadata").mkdir(parents=True, exist_ok=True)
metadata = json.loads((SOURCE / "metadata/v2.metadata.json").read_text())
original_location = metadata["location"]
snapshot = metadata["snapshots"][-1]
list_schema, list_metadata, manifests = read_avro(
    SOURCE / "metadata" / Path(snapshot["manifest-list"]).name
)
data_manifest, dv_manifest = manifests

# Preserve original paths in both the copied data manifest and delete contents.
for manifest in manifests:
    source = SOURCE / "metadata" / Path(manifest["manifest_path"]).name
    shutil.copyfile(source, TARGET / "metadata" / source.name)
for source in (SOURCE / "data").iterdir():
    shutil.copyfile(source, TARGET / "data" / source.name)

delete_schema, delete_metadata, dv_rows = read_avro(
    SOURCE / "metadata" / Path(dv_manifest["manifest_path"]).name
)
original_data_path = dv_rows[0]["data_file"]["referenced_data_file"]
pos_path = TARGET / "data/positions.parquet"
schema = pa.schema(
    [
        pa.field(
            "file_path",
            pa.string(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483546"},
        ),
        pa.field(
            "pos",
            pa.int64(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483545"},
        ),
    ]
)
pq.write_table(
    pa.Table.from_arrays(
        [pa.array([original_data_path] * 2), pa.array([0, 50])], schema=schema
    ),
    pos_path,
)
position_rows = copy.deepcopy(dv_rows)
position_rows[0].update(sequence_number=1, file_sequence_number=1)
position_rows[0]["data_file"].update(
    file_path=f"{original_location}/data/positions.parquet",
    file_format="PARQUET",
    record_count=2,
    file_size_in_bytes=pos_path.stat().st_size,
    referenced_data_file=None,
    content_offset=None,
    content_size_in_bytes=None,
)
position_manifest_path = TARGET / "metadata/positions.avro"
write_avro(position_manifest_path, delete_schema, delete_metadata, position_rows)
position_manifest = copy.deepcopy(dv_manifest)
position_manifest.update(
    manifest_path=f"{original_location}/metadata/positions.avro",
    manifest_length=position_manifest_path.stat().st_size,
    added_rows_count=2,
    sequence_number=1,
    min_sequence_number=1,
)
for name, selected in [
    ("positions", [data_manifest, position_manifest]),
    ("mixed", [data_manifest, position_manifest, dv_manifest]),
]:
    write_avro(
        TARGET / f"metadata/{name}-list.avro",
        list_schema,
        list_metadata.copy(),
        selected,
    )
    selected_metadata = copy.deepcopy(metadata)
    selected_snapshot = selected_metadata["snapshots"][-1]
    selected_snapshot["manifest-list"] = (
        f"{original_location}/metadata/{name}-list.avro"
    )
    selected_snapshot["summary"] = {
        "operation": "delete",
        "total-records": "100",
        "total-data-files": "1",
        "total-delete-files": str(len(selected) - 1),
        "total-position-deletes": str(2 if name == "positions" else 3),
    }
    # Keep only the current snapshot; copied metadata-log paths are not needed.
    selected_metadata["snapshots"] = [selected_snapshot]
    selected_metadata["snapshot-log"] = selected_metadata["snapshot-log"][-1:]
    selected_metadata["metadata-log"] = []
    (TARGET / f"metadata/{name}.metadata.json").write_text(
        json.dumps(selected_metadata, indent=2) + "\n"
    )
