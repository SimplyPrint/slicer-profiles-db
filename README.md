# slicer-profiles-db
An open database of printer and filament profiles for various 3D printing slicers. Integrated directly with the SimplyPrint slicer, allowing you to use PrusaSlicer, BambuStudio and OrcaSlicer in the browser. Contribute by adding profiles here that everyone can benefit from - for users and brands alike.

## Contributing

As we are working on the process, the simplest way to add profiles is to add them as "overlays" in the `overlay/` folder, these correspond 1:1 with the format from the slicer you'd want to add the profile to, and is the simplest way to integrate the profile consistently.

The `profiles/` folder is a generated, centralized result of ingesting multiple data sources, including the overlays, while manual edits are possible, they are not intended.
