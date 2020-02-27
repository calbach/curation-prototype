Curation Prototypes
--

Possible approaches for a v2 curation pipeline architecture. The sample pipeline
implemented here is meant to exemplify typical work done by the curation code.

Input: BigQuery OMOP dataset, a few domain tables from 2 sites.

Pipeline stages:

1. Merge multiple tables together
1. Move data from person table elsewhere.
1. Row-level table transform
1. Retract participants by ID.
1. Group-by-participant transforms, e.g. remove duplicate measurements, observations
1. Generate derived tables, e.g. mapping
Output: BigQuery OMOP CDR dataset.
