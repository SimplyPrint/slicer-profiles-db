# Dremel for Cura

Cura ships no Dremel definitions. These come from
[Cura-Dremel-Printer-Plugin](https://github.com/metalman3797/Cura-Dremel-Printer-Plugin)
at `1993b6d` (LGPL-3.0, see `LICENSE`), ported to Cura 5.13:

- ids are Cura-style (`dremel_3d45`), names match the printers ("Dremel 3D45");
- quality `setting_version` is 27, and settings Cura 5.13 no longer has are dropped
  (`support_interface_skip_height`, `speed_equalize_flow_enabled`,
  `travel_compensate_overlapping_walls_x_enabled`);
- each material quality takes its quality type's global name, as Cura displays it;
- `exclude_materials` is dropped, so generic 1.75 mm materials map to these printers.

The cloud slicer needs these definitions' formulas too: regenerate
`site-cloud-slicer/src/slicers/cura_definitions/5.13.0.json.gz` with this directory as an
extra resource root when they change.
