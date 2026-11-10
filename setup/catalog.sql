-- One-time setup: the catalog that holds every object of this project.
--
-- Why this isn't a bundle resource: on Free Edition every catalog lives on Default Storage,
-- and the Unity Catalog API refuses to create those ("Please use the UI to create a catalog
-- with Default Storage"). Bundles and Terraform both go through that API. SQL works.
--
-- Run it with `databricks bundle run setup`, or paste it into the SQL editor and fill in
-- the catalog parameter.

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'databricks-football: one schema per layer (landing, bronze, silver, gold)';
