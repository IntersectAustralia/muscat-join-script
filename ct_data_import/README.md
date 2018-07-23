# Canonic techniques import

Python script to import Canonic techniques records into the Muscat database. 

It does three things:
1. Imports Canonic techniques records into the canonic_techniques table
2. Imports the join records into the sources_to_canonic_techniques table
3. Adds canonic techniques marc data using the marc code "695" to the source record in the sources table.

It takes two CSV datasets:
1. The ingest for the authority Canonic Techniques table, canonic_techniques. This file has to be CSV and **must be** in the same table structure as the database table.
2. The ingest for the source_to_canonic_techniques table. This file has to be CSV and must contain the joining data from canonic_technique_id to source_id

