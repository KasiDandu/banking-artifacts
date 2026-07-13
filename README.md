# banking-artifacts

Lambda + Glue application code for the banking data lake pipeline: `S3 (raw CSV landing) ->
EventBridge (CloudTrail S3 data events) -> Lambda -> Glue (PySpark)`. The Glue job validates each
source file against a config-driven schema, writes cleaned Parquet using a per-source load mode
(`truncate_load` / `append` / `upsert`), records a lineage row per run, and creates-or-refreshes
a Glue Crawler on the target.

Deployment (Terraform/Terragrunt, IAM, CloudTrail/EventBridge wiring, the "live" config JSON
files) lives in the sibling `banking-infra` repo, which consumes the artifacts this repo builds.

## Layout

- `banking-data/lambda/lambda_function.py` -- EventBridge/CloudTrail event handler; resolves the
  landed file's source config and starts the Glue job run.
- `banking-data/glue/glue_script.py` -- the Glue Spark job entry point.
- `banking-data/common/` -- shared, unit-testable logic used by both:
  - `config_loader.py` -- structural validation of a source's config JSON.
  - `validation.py` -- schema/dtype/nullability/allowed-values validation of a Spark DataFrame.
  - `load_modes.py` -- `truncate_load` / `append` / `upsert` write logic.
  - `crawler.py` -- create-or-refresh a Glue Crawler.
  - `lineage.py` -- build + write the per-run audit/lineage record.
- `tests/` -- pytest suite (local `SparkSession`, no AWS calls).
- `.github/workflows/built-release.yaml` -- CI: see "Build & release" below.

## Local development

```bash
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements-dev.txt
pytest
```

Requires a local JDK (Spark dependency) -- any JDK 8/11/17 on PATH works with pyspark 3.3.x.

## Build & release (`built-release.yaml`)

- **Any branch push**: zips the Lambda handler + `common/` into `lambda.zip`, and packages
  `common/` separately as `glue-common.zip` for the Glue job's `--extra-py-files`. Both are
  uploaded content-addressed by `sha256` under `s3://$ARTIFACT_BUCKET/{lambda,glue}/sha256/<hash>/`
  -- immutable, safe to reference from any branch/PR without collisions.
- **Push to `main`**: additionally computes the next semantic version from Conventional Commits
  (`(MAJOR)` / `(MINOR)` markers in commit messages, patch otherwise), tags the commit, creates a
  GitHub Release, copies the same artifacts to `s3://$ARTIFACT_BUCKET/{lambda,glue}/v<semver>/`,
  and writes `manifest/v<semver>.json` + `manifest/latest.json` describing exactly what was
  published (hashes, S3 keys, commit SHA, build time).

`banking-infra` pins a deployed version by editing `terraform/live/banking-data/lambda.json` /
`glue.json` (`artifact_version` + `s3_key`) and re-running `terragrunt apply` -- no coupling
between an artifact release and a Terraform code change.

### Required repo configuration (GitHub Actions Variables)

Set under Settings -> Secrets and variables -> Actions -> Variables (none of these are secrets;
auth is via OIDC, not static keys):

| Variable | Purpose |
|---|---|
| `AWS_REGION` | Region containing the artifact bucket. |
| `ARTIFACT_BUCKET` | S3 bucket the workflow publishes to (created by `banking-infra`). |
| `AWS_ARTIFACT_PUBLISH_ROLE_ARN` | IAM role (trust policy scoped to this repo via GitHub OIDC) with `s3:PutObject` on `ARTIFACT_BUCKET` only. Created by `banking-infra` (`modules/banking-data/iam.tf`). |
