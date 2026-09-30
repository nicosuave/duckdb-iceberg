# /// script
# dependencies = ["pyiceberg[pyarrow]==0.11.1"]
# ///
"""Generate positional deletes without referenced paths, including an equal-sequence boundary."""

from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io import load_file_io
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestListWriterV2,
    ManifestWriterV2,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.sorting import SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField


class DeleteManifestWriter(ManifestWriterV2):
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


root = Path("data/persistent/positional_delete_index")
(root / "metadata").mkdir(parents=True, exist_ok=True)
(root / "data").mkdir(exist_ok=True)
schema = Schema(
    NestedField(1, "part", IntegerType()), NestedField(2, "id", IntegerType())
)
spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "part"))
io = load_file_io({"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})
snapshot_id = 100

data_schema = pa.schema(
    [
        pa.field("part", pa.int32(), metadata={"PARQUET:field_id": "1"}),
        pa.field("id", pa.int32(), metadata={"PARQUET:field_id": "2"}),
    ]
)
delete_schema = pa.schema(
    [
        pa.field("file_path", pa.string(), metadata={"PARQUET:field_id": "2147483546"}),
        pa.field("pos", pa.int64(), metadata={"PARQUET:field_id": "2147483545"}),
    ]
)


def entry(path, partition, content, count, sequence, lower=None, upper=None):
    file = DataFile.from_args(
        _table_format_version=2,
        content=content,
        file_path=str(path),
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=count,
        file_size_in_bytes=path.stat().st_size,
        lower_bounds=lower,
        upper_bounds=upper,
    )
    return ManifestEntry(
        ManifestEntryStatus.ADDED, snapshot_id, sequence, sequence, file
    )


def write_metadata(root, spec, data_entries, delete_entries):
    manifests = []
    for name, writer_class, entries in [
        ("data", ManifestWriterV2, data_entries),
        ("deletes", DeleteManifestWriter, delete_entries),
    ]:
        path = root / "metadata" / f"{name}.avro"
        with writer_class(
            spec, schema, io.new_output(str(path)), snapshot_id, "null"
        ) as writer:
            for item in entries:
                writer.add_entry(item)
            manifests.append(writer.to_manifest_file())

    manifest_list = root / "metadata" / "snap.avro"
    with ManifestListWriterV2(
        io.new_output(str(manifest_list)), snapshot_id, None, 3, "null"
    ) as writer:
        writer.add_manifests(manifests)

    metadata = TableMetadataV2(
        location=str(root),
        table_uuid=UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001"),
        last_updated_ms=1700000000000,
        last_column_id=2,
        schemas=[schema],
        partition_specs=[spec],
        default_spec_id=0,
        last_partition_id=1000,
        current_snapshot_id=snapshot_id,
        snapshots=[
            Snapshot(
                snapshot_id=snapshot_id,
                sequence_number=3,
                timestamp_ms=1700000000000,
                manifest_list=str(manifest_list),
                summary={"operation": "delete"},
                schema_id=0,
            )
        ],
        sort_orders=[SortOrder(order_id=0)],
        last_sequence_number=3,
    )
    (root / "metadata" / "v1.metadata.json").write_text(
        metadata.model_dump_json(indent=2)
    )


data_entries = []
delete_entries = []
for name, partition in [("null", None), ("one", 1), ("two", 2)]:
    data_path = root / "data" / f"{name}.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"part": partition, "id": 0}, {"part": partition, "id": 1}], data_schema
        ),
        data_path,
    )
    data_entries.append(entry(data_path, Record(partition), DataFileContent.DATA, 2, 2))
    for sequence, position in [(1, 1), (2, 0)]:
        delete_path = root / "data" / f"{name}-delete-{sequence}.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [{"file_path": str(data_path), "pos": position}], delete_schema
            ),
            delete_path,
        )
        delete_entries.append(
            entry(
                delete_path,
                Record(partition),
                DataFileContent.POSITION_DELETES,
                1,
                sequence,
            )
        )


write_metadata(root, spec, data_entries, delete_entries)

# Unpartitioned v2 deletes: exact filename bounds can identify one data file, while
# a range spanning multiple paths must still be supplied to both files.
root = Path("data/persistent/positional_delete_path_bounds")
(root / "metadata").mkdir(parents=True, exist_ok=True)
(root / "data").mkdir(exist_ok=True)
data_entries = []
delete_entries = []
paths = []
for name in ["a", "b"]:
    data_path = root / "data" / f"{name}.parquet"
    paths.append(str(data_path))
    pq.write_table(
        pa.Table.from_pylist([{"part": None, "id": i} for i in range(3)], data_schema),
        data_path,
    )
    data_entries.append(entry(data_path, Record(), DataFileContent.DATA, 3, 2))
    delete_path = root / "data" / f"{name}-delete.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"file_path": str(data_path), "pos": 0}], delete_schema),
        delete_path,
    )
    bounds = {2147483546: str(data_path).encode()}
    delete_entries.append(
        entry(
            delete_path,
            Record(),
            DataFileContent.POSITION_DELETES,
            1,
            2,
            bounds,
            bounds,
        )
    )

delete_path = root / "data" / "shared-delete.parquet"
pq.write_table(
    pa.Table.from_pylist(
        [{"file_path": path, "pos": 1} for path in paths], delete_schema
    ),
    delete_path,
)
delete_entries.append(
    entry(
        delete_path,
        Record(),
        DataFileContent.POSITION_DELETES,
        2,
        2,
        {2147483546: min(paths).encode()},
        {2147483546: max(paths).encode()},
    )
)
write_metadata(root, PartitionSpec(), data_entries, delete_entries)
