from ingestion.utils.checkpoint import FileCheckpointStore


def test_checkpoint_roundtrip(tmp_path):
    store = FileCheckpointStore(tmp_path)
    assert store.get_watermark("orders") is None
    store.set_watermark("orders", "2026-07-01T00:00:00")
    assert store.get_watermark("orders") == "2026-07-01T00:00:00"


def test_checkpoint_overwrite(tmp_path):
    store = FileCheckpointStore(tmp_path)
    store.set_watermark("orders", "a")
    store.set_watermark("orders", "b")
    assert store.get_watermark("orders") == "b"
