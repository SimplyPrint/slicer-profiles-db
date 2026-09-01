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

The `profiles/` folder is a generated, centralized result of ingesting multiple data sources, including the overlays, while manual edits are possible, they are not intended.

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
