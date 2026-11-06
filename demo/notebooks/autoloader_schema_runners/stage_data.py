# Databricks notebook source
dbutils.widgets.text("uc_volume_path", "")
dbutils.widgets.dropdown("phase", "1", ["1", "2"])

uc_volume_path = dbutils.widgets.get("uc_volume_path").rstrip("/") + "/"
phase = dbutils.widgets.get("phase")

if phase not in {"1", "2"}:
    raise ValueError(f"phase must be 1 or 2, got {phase!r}")

# COMMAND ----------

landing_path = f"{uc_volume_path}data/autoloader_schema_demo/landing"
fixture_path = (
    f"{uc_volume_path}demo/resources/data/autoloader_schema_demo/"
    f"phase{phase}/events.json"
)
target_path = f"{landing_path}/phase{phase}_events.json"

if phase == "1":
    dbutils.fs.rm(landing_path, recurse=True)
dbutils.fs.mkdirs(landing_path)
dbutils.fs.cp(fixture_path, target_path)

staged = [item.name for item in dbutils.fs.ls(landing_path)]
target_name = target_path.rsplit("/", 1)[-1]
if target_name not in staged:
    raise AssertionError(
        f"Failed to stage phase {phase}: {target_path}; found {staged}"
    )

print(f"Staged Auto Loader schema demo phase {phase}:")
print(f"  source : {fixture_path}")
print(f"  target : {target_path}")
