# ogim-msat
Home for all code related to OGIM database development by the EDF Science team.

Questions about the contents of this repo? Contact one of the admins: Mads O'Brien or Mark Omara


## What's in this repository?

To create a new version of the OGIM database, the scripts in this repository are run in the folowing order:

* *data_refresh* = automatic downloading of select raw data from original source providers. NOTE that not all of our data sources are able to be downloaded automatically; be sure to follow-up this step by manually downloading remaining datasets based on the notes in our Data Catalog.

* *data_integration* = scripts that "integrate" raw source data into our OGIM schema, including extensive cleaning of the raw source data. All of these scripts are run *before* the data consolidation step.

* *data_consolidation* = script(s) to assemble pieces of integrated OGIM data into a single GeoPackage and run some quality checks. Usually, one data consolidation file exists for each OGIM version.

Other folders in this repository include:

* *docs* = supporting files, such as: explanation of the attribute schema used in the database, UN Country List used for standardization, etc. (Note: As of April 2024 this folder is pretty out of date, contact the admins for more recent documentation)
* *functions* = helper functions written by the OGIM team that are called during data integration, data consolidation, or other analyses.
