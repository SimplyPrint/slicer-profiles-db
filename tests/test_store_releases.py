from slicer_profiles_db.index import ProfileIndex, build_generic_profile_index
from slicer_profiles_db.mapping import _snapshot_filament_id
from slicer_profiles_db.models import ParsedProfile, ProfileType, SlicerType
from slicer_profiles_db.release_history import release_index
from slicer_profiles_db.store import ProfileStore

ORCA = SlicerType.ORCASLICER


def _filament(name, vendor="Prusa", **settings):
    return ParsedProfile(
        slicer=ORCA,
        profile_type=ProfileType.FILAMENT,
        name=name,
        vendor=vendor,
        settings={"name": name, **settings},
        filament_id=settings.get("filament_id"),
    )


def _get(store, name, vendor="Prusa"):
    return store.get(ORCA, "filament", vendor, name)


def test_renamed_from_lists_every_former_name(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA,
        "2.4.2",
        [_filament("Prusa Generic ABS @CORE One", temp=["250"], fan=["10"])],
        complete_release=True,
    )

    store.ingest_profiles(
        ORCA,
        "2.5.0",
        [
            _filament(
                "Generic ABS @Prusa CORE One",
                temp=["255"],
                renamed_from='Prusa Generic ABS CORE One;"Prusa Generic ABS @CORE One"',
            )
        ],
        complete_release=True,
    )

    assert _get(store, "Prusa Generic ABS @CORE One") is None
    renamed = _get(store, "Generic ABS @Prusa CORE One")
    assert renamed.first_seen == "2.4.2"
    assert renamed.renamed_from == "Prusa Generic ABS @CORE One"
    assert renamed.evaluate("2.4.2") == {
        "name": "Prusa Generic ABS @CORE One",
        "temp": ["250"],
        "fan": ["10"],
    }
    # A setting the renamed profile dropped does not live on.
    assert renamed.evaluate("2.5.0") == {
        "name": "Generic ABS @Prusa CORE One",
        "temp": ["255"],
    }


def test_a_profile_moved_to_another_vendor_keeps_its_history(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA,
        "2.4.2",
        [_filament("BETA PLA @BBL X1C", vendor="BBL", temp=["220"])],
        complete_release=True,
    )

    store.ingest_profiles(
        ORCA,
        "2.5.0",
        [_filament("BETA PLA @BBL X1C", vendor="OrcaFilamentLibrary", temp=["225"])],
        complete_release=True,
    )

    assert _get(store, "BETA PLA @BBL X1C", "BBL") is None
    moved = _get(store, "BETA PLA @BBL X1C", "OrcaFilamentLibrary")
    assert moved.first_seen == "2.4.2"
    assert moved.renamed_from is None
    assert moved.evaluate("2.4.2")["temp"] == ["220"]
    assert moved.evaluate("2.5.0")["temp"] == ["225"]


def test_a_move_is_only_taken_when_one_vendor_had_the_profile(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA,
        "2.4.2",
        [_filament("Generic PLA", vendor="A"), _filament("Generic PLA", vendor="B")],
        complete_release=True,
    )

    store.ingest_profiles(
        ORCA, "2.5.0", [_filament("Generic PLA", vendor="C")], complete_release=True
    )

    assert _get(store, "Generic PLA", "C").first_seen == "2.5.0"
    assert _get(store, "Generic PLA", "A").evaluate("2.5.0") == {}
    assert _get(store, "Generic PLA", "B").evaluate("2.5.0") == {}


def test_moves_need_a_complete_release(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(ORCA, "2.4.2", [_filament("BETA PLA", vendor="BBL")])

    store.ingest_profiles(
        ORCA, "2.5.0", [_filament("BETA PLA", vendor="OrcaFilamentLibrary")]
    )

    assert _get(store, "BETA PLA", "BBL").evaluate("2.5.0") == {"name": "BETA PLA"}
    assert _get(store, "BETA PLA", "OrcaFilamentLibrary").first_seen == "2.5.0"


def test_a_complete_release_tombstones_the_profiles_it_dropped(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA,
        "2.4.2",
        [_filament("Kept", temp=["200"]), _filament("Dropped", temp=["210"])],
        complete_release=True,
    )

    report = store.ingest_profiles(
        ORCA, "2.5.0", [_filament("Kept", temp=["200"])], complete_release=True
    )

    dropped = _get(store, "Dropped")
    assert report.changed["Dropped"] == ["__removed__"]
    assert dropped.last_seen == "2.5.0"
    assert dropped.evaluate("2.4.2") == {"name": "Dropped", "temp": ["210"]}
    assert dropped.evaluate("2.5.0") == {}
    assert dropped.evaluate_at_or_before("2.5.1") == {}
    assert release_index(
        dropped.settings, {"name": "Dropped", "temp": ["210"]}, ["2.4.2", "2.5.0"]
    ) == {"base": "2.4.2", "versions": {"2.4.2": {}, "2.5.0": None}}

    # It comes back when a later release has it again.
    store.ingest_profiles(
        ORCA,
        "2.5.1",
        [_filament("Kept", temp=["200"]), _filament("Dropped", temp=["215"])],
        complete_release=True,
    )
    assert _get(store, "Dropped").evaluate("2.5.1") == {
        "name": "Dropped",
        "temp": ["215"],
    }
    assert _get(store, "Dropped").evaluate("2.5.0") == {}


def test_only_complete_newest_stable_releases_tombstone(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA, "2.5.0", [_filament("A"), _filament("B")], complete_release=True
    )

    for version, complete in (
        ("2.5.1", False),
        ("2.6.0-beta", True),
        ("nightly-2026-11-09", True),
        ("2.4.2", True),
    ):
        store.ingest_profiles(
            ORCA, version, [_filament("A")], complete_release=complete
        )

    assert _get(store, "B").evaluate("2.6.0") == {"name": "B"}


def test_tombstones_stay_within_the_ingested_profile_types(tmp_path):
    store = ProfileStore(tmp_path)
    process = ParsedProfile(
        slicer=ORCA,
        profile_type=ProfileType.PRINT,
        name="0.20mm Standard",
        vendor="Prusa",
        settings={"layer_height": "0.2"},
    )
    store.ingest_profiles(ORCA, "2.4.2", [_filament("A"), process])

    store.ingest_profiles(ORCA, "2.5.0", [_filament("A")], complete_release=True)

    assert store.get(ORCA, "print", "Prusa", "0.20mm Standard").evaluate("2.5.0")


def test_release_index_marks_releases_before_a_profile_was_added():
    history = {"name": {"2.5.0": "New"}}

    assert release_index(history, {"name": "New"}, ["2.4.2", "2.5.0"]) == {
        "base": "2.5.0",
        "versions": {"2.4.2": None, "2.5.0": {}},
    }


def test_generic_ids_follow_the_exported_release(tmp_path):
    store = ProfileStore(tmp_path)
    store.ingest_profiles(
        ORCA,
        "2.4.2",
        [_filament("Generic PLA", filament_id="GFL99", filament_type=["PLA"])],
        complete_release=True,
    )
    store.ingest_profiles(
        ORCA,
        "2.5.0",
        [_filament("Generic PLA", filament_id="OFDSrzZ8", filament_type=["PLA"])],
        complete_release=True,
    )
    index = ProfileIndex(store)
    index.build([ORCA])

    generics = build_generic_profile_index(
        index,
        [ORCA],
        evaluate=lambda profile: profile.evaluate_at_or_before("2.4.2"),
        filament_id=_snapshot_filament_id,
    )

    assert generics[ORCA.value] == [("generic pla", "PLA", "GFL99")]
