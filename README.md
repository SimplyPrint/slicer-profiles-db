# slicer-profiles-db
An open database of printer and filament profiles for various 3D printing slicers. Integrated directly with the SimplyPrint slicer, allowing you to use PrusaSlicer, BambuStudio and OrcaSlicer in the browser. Contribute by adding profiles here that everyone can benefit from - for users and brands alike.

PrusaSlicer 3.0 profiles are ingested from the evaluated `profile-bundle.json`
published by `SimplyPrint/slicer-builds`. These profiles retain their upstream
native ID and explicit compatibility contexts. A stable storage key allows
multiple evaluated variants with the same display name to coexist; legacy 2.x
INI profiles continue to use their existing name-based layout. Mapping-friendly
machine-model roles are derived from evaluated machine variants, and each
process role carries its matching per-tool process configs in context so the
3.0 output does not flatten away the new split configuration model.

## Contributing

As we are working on the process, the simplest way to add profiles is to add them as "overlays" in the `overlay/` folder, these correspond 1:1 with the format from the slicer you'd want to add the profile to, and is the simplest way to integrate the profile consistently.

To correct a few keys of an upstream profile instead of replacing it, add
`<profile>.patch.json` beside where its `<profile>.json` sits upstream: its keys are
set on the squashed upstream profile, which otherwise keeps following upstream. A
patch whose profile a release does not have is logged and skipped. The store only
changes when a release is ingested, so re-ingest the latest one with `--force` when
adding a patch, and for an engine with release history rebuild its releases
with `slicer-profiles-db ingest-releases <engine> --rebuild`.

The `profiles/` folder is a generated, centralized result of ingesting multiple data sources, including the overlays, while manual edits are possible, they are not intended.

## Import bundle

`uv run slicer-profiles-db map` produces one deterministic
`dist/profiles.spdb` bundle. It replaces the duplicated per-model `out/` tree:
profiles are stored once under a stable producer ID, model relationships are ID
lists, and resources are content-addressed. The manifest records the stable and
lane engine inputs, G-code ABI, hashes, and a model-by-engine mapping
outcome for every SimplyPrint printer model. Bundle creation fails unless that
classification matrix is 100% complete; an unmatched model is valid only when
it has an explicit reason.

Engine targets live in `engines.lock.json`. A catalog lane is a complete snapshot
selected by an authoritative source format and is used only for a real topology
boundary such as PrusaSlicer 3. Ordinary setting compatibility is derived from
the versioned setting definitions and is never copied into profile rows.

### Per-release settings

An engine with `"release_history": true` exports its profiles at the locked
`version` and attaches `version_settings` to each one:

```json
{"base": "02.09.01.70", "renamed_from": "Old name",
 "versions": {"02.08.02.61": {"machine_start_gcode": "...", "removed_key": null},
              "02.09.01.70": {}, "02.06.01.55": null}}
```

`versions` has every stable release the store has ingested for the engine, oldest
first, independent of the releases anything slices with: per release, the settings that
differ from the export, `null` for a setting that release does not have, and `null`
instead of an object when the release does not have the profile at all. `base` is
the newest release whose settings are the export, and the locked `version` must be
one of the releases. A release's values are that release's own, G-code templates
included, byte for byte.

Each release must be exact, or the build fails. A complete ingest of a release
writes `profiles/<engine>/_releases/<release>.json`: a digest of every profile's
settings as parsed. Building the bundle evaluates the store at each ingested release
and compares it with that manifest; a release without one (ingested in part, or
before manifests existed), or one the store no longer reproduces, stops the build
and names the release. The store records history as changes from the release
ingested before, so whole releases must arrive oldest first: the store refuses an
older one, and `ingest-all` skips it.
`uv run slicer-profiles-db ingest-releases` re-ingests what the build asks for: it
finds the upstream tagged releases the way `ingest-all --all-versions` does (stable
tags from the engine's minimum version on), ingests those without a manifest from
their own tags, and when one of them is older than the store's newest, or the store
has an unproven release upstream no longer tags, it clears the engine and rebuilds it
from every release in order (`--rebuild` forces that, for example after changing an
overlay). The nightly workflow runs it before `ingest-all`. An exported profile that
no ingested release gives also fails the build, since web could not read it per
release.

The bundle manifest says which releases it describes and which one a nightly build
gets, so web never has to guess:

```json
"release_history": {"enabled": true, "profiles": 3204,
                    "releases": ["02.04.00.70", "...", "02.09.01.70"],
                    "nightly": "02.09.01.70"}
```

A nightly is built from the upstream default branch, whose profiles are not
ingested; it gets the newest ingested release's settings.

## Cura resources

Cura and `fdm_materials` are pinned together at 5.13.0 to match the cloud
CuraEngine build. Ingestion resolves machine/extruder inheritance and includes
hardware variants, materials, qualities, quality changes, and intents.

Runtime Cura roles use a common shape:

```json
{
  "data": {"layer_height": 0.2},
  "context": {"native_id": "quality:..."},
  "setting_scopes": {"layer_height": "global"}
}
```

`data` contains concrete CuraEngine settings only. Upstream identifiers and
compatibility metadata remain in `context`. The cloud runtime uses all merged
data as CuraEngine's global baseline; keys marked `extruder.0` are also applied
to the selected extruder as overrides. This duplication is required by Cura's
scene/mesh setting inheritance.
