## Plan: Azure Functions deployment via GitHub Actions (uv-first)

### 1) Decide Function model + trigger
- Confirm desired trigger (Timer vs HTTP) and schedule (cron) for the job.
- Choose Azure Functions Python model:
  - **v1 model**: `TimerFunction/` folder with `__init__.py` + `function.json` (best with `scm-do-build-during-deployment: false`).
  - **v2 model**: `function_app.py` with decorators (requires build step to generate `function.json` or enable Oryx build).

### 2) Add Azure Functions entrypoint
- Create a function entry file that calls the existing async pipeline in `main.py`:
  - Option A (v1): `TimerFunction/__init__.py` wraps `asyncio.run(main())`.
  - Option B (v2): `function_app.py` defines `app = func.FunctionApp()` and `@app.timer_trigger(...)` calls `await main()`.
- Add the correct `function.json` if using v1 model.

### 3) Dependency + runtime alignment
- Add `azure-functions` to `pyproject.toml` dependencies.
- Decide which current dependencies are runtime vs dev-only; move dev tools (`ruff`, `basedpyright`, `ty`) to a dev group if needed.
- Ensure Python version in workflow matches Functions runtime (currently 3.12; verify that target Function App runtime supports it).

### 4) Workflow packaging changes (uv-first)
- Update `.github/workflows/deploy-function.yml`:
  - Replace `TimerFunction` copy with the actual function folder(s) you add.
  - Use `uv export --no-dev --format requirements-txt` to generate `requirements.txt`.
  - Vendor deps with `uv pip install -r requirements.txt -t function_package/.python_packages/lib/site-packages` (no `pip install`).
  - Keep `scm-do-build-during-deployment: false` and `enable-oryx-build: false` for prebuilt zip.

### 5) Local development + settings
- Add `local.settings.json` template (or doc) for local secrets; ensure `.funcignore` keeps it out of deployments.
- Document local run commands (`func start`) and env variables required by `MarketplacerGateway`, `AzureBlobClient`, etc.

### 6) Verification
- Run a local Functions host (if available) and test trigger.
- Validate package contents in CI (`unzip -l function.zip`) to ensure `host.json`, function folder(s), `src/`, and `.python_packages/` are present.

### 7) Secrets + deployment config
- Confirm `AZURE_FUNCTIONAPP_PUBLISH_PROFILE_PROD` exists in GitHub secrets.
- Verify app name `influ-police-production-func` matches the target Function App.
