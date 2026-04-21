"""
main.py
-------
FastAPI wrapper for the Seed-and-Seek pipeline.

Endpoints:
  POST /pipeline/run        — Upload a seed file and run the full pipeline
  GET  /pipeline/{job_id}   — Get the status and results of a job
  GET  /pipeline/{job_id}/metrics   — Get evaluation metrics
  GET  /pipeline/{job_id}/download  — Download the augmented dataset

Usage:
  python src/main.py

Then open http://localhost:8000/docs for the interactive API docs.
"""

import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Seed-and-Seek API",
    description="Universal Web Dataset Discovery, Integration and Augmentation from a Seed Dataset",
    version="1.0.0",
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Root of the project (one level up from src/)
ROOT = Path(__file__).parent.parent
SRC  = ROOT / "src"

# All job outputs are stored under output/jobs/<job_id>/
JOBS_DIR = ROOT / "output" / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# Supported seed file extensions
SUPPORTED_EXTENSIONS = {".csv", ".json", ".xlsx", ".db"}

# ---------------------------------------------------------------------------
# Job helpers
# ---------------------------------------------------------------------------

def _job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _write_status(job_id: str, status: str, message: str = "") -> None:
    status_file = _job_dir(job_id) / "status.json"
    status_file.write_text(
        json.dumps({"job_id": job_id, "status": status, "message": message}),
        encoding="utf-8",
    )


def _read_status(job_id: str) -> dict:
    status_file = _job_dir(job_id) / "status.json"
    if not status_file.exists():
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return json.loads(status_file.read_text(encoding="utf-8"))


def _run_step(cmd: list[str], job_id: str, step_name: str) -> None:
    """Run a pipeline step as a subprocess. Raises RuntimeError on failure."""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    # Write step log
    log_file = _job_dir(job_id) / f"{step_name}.log"
    log_file.write_text(result.stdout + result.stderr, encoding="utf-8")

    if result.returncode != 0:
        raise RuntimeError(
            f"Step '{step_name}' failed.\n{result.stderr[-1000:]}"
        )


def _run_pipeline(job_id: str, seed_path: Path) -> None:
    """
    Runs all pipeline stages sequentially for a given job.
    Updates status.json at each step.
    """
    job  = _job_dir(job_id)
    py   = sys.executable  # use the same Python that's running FastAPI

    try:
        # Paths for intermediate outputs
        signature_path    = job / "seed_signature.json"
        queries_path      = job / "queries.json"
        discovery_path    = job / "discovery_results.json"
        ranked_path       = job / "ranked_results.json"
        integrated_csv    = job / "integrated_data.csv"
        integration_report= job / "integration_report.json"
        evaluation_path   = job / "evaluation_report.json"

        # ── Step 1: Profiling ──────────────────────────────────────────────
        _write_status(job_id, "running", "Step 1/6: Profiling seed dataset...")
        _run_step([
            py, str(SRC / "profiler.py"),
            "--input",  str(seed_path),
            "--output", str(signature_path),
        ], job_id, "1_profiler")

        # ── Step 2: Query Generation ───────────────────────────────────────
        _write_status(job_id, "running", "Step 2/6: Generating queries...")
        _run_step([
            py, str(SRC / "query_generator.py"),
            "--input",  str(signature_path),
            "--output", str(queries_path),
        ], job_id, "2_query_generator")

        # ── Step 3: Web Discovery ──────────────────────────────────────────
        _write_status(job_id, "running", "Step 3/6: Discovering datasets on the web...")
        _run_step([
            py, str(SRC / "web_discovery.py"),
            "--input",     str(queries_path),
            "--output",    str(discovery_path),
            "--signature", str(signature_path),
        ], job_id, "3_web_discovery")

        # ── Step 4: Ranking ────────────────────────────────────────────────
        _write_status(job_id, "running", "Step 4/6: Ranking candidates...")
        _run_step([
            py, str(SRC / "ranking.py"),
            "--input",  str(discovery_path),
            "--output", str(ranked_path),
        ], job_id, "4_ranking")

        # ── Step 5: Integration ────────────────────────────────────────────
        _write_status(job_id, "running", "Step 5/6: Integrating datasets...")
        _run_step([
            py, str(SRC / "integration.py"),
            "--seed",          str(seed_path),
            "--ranked",        str(ranked_path),
            "--signature",     str(signature_path),
            "--output-data",   str(integrated_csv),
            "--output-report", str(integration_report),
        ], job_id, "5_integration")

        # ── Step 6: Evaluation ─────────────────────────────────────────────
        _write_status(job_id, "running", "Step 6/6: Evaluating results...")
        _run_step([
            py, str(SRC / "evaluate.py"),
            "--ranked",  str(ranked_path),
            "--report",  str(integration_report),
            "--output",  str(evaluation_path),
            "--k", "5", "10", "20",
        ], job_id, "6_evaluate")

        _write_status(job_id, "done", "Pipeline completed successfully.")

    except RuntimeError as e:
        _write_status(job_id, "failed", str(e))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/pipeline/run", summary="Upload a seed file and run the full pipeline")
async def run_pipeline(file: UploadFile = File(...)):
    """
    Upload a seed dataset file (.csv, .json, .xlsx, .db) to start the pipeline.

    Returns a **job_id** you can use to poll status and retrieve results.
    """
    # Validate extension
    suffix = Path(file.filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Allowed: {sorted(SUPPORTED_EXTENSIONS)}",
        )

    # Create job directory
    job_id  = str(uuid.uuid4())
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)

    # Save uploaded file
    seed_path = job_dir / f"seed{suffix}"
    with open(seed_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    _write_status(job_id, "running", "Pipeline started...")

    # Run pipeline synchronously
    # (for async/background processing, replace with BackgroundTasks)
    _run_pipeline(job_id, seed_path)

    status = _read_status(job_id)
    return JSONResponse(content={"job_id": job_id, **status})


@app.get("/pipeline/{job_id}", summary="Get job status")
async def get_job_status(job_id: str):
    """
    Returns the current status of a pipeline job.

    Status values: `running`, `done`, `failed`
    """
    return JSONResponse(content=_read_status(job_id))


@app.get("/pipeline/{job_id}/metrics", summary="Get evaluation metrics")
async def get_metrics(job_id: str):
    """
    Returns the full evaluation report for a completed job.
    Includes retrieval, integration, and augmentation metrics.
    """
    _read_status(job_id)  # validates job exists

    metrics_path = _job_dir(job_id) / "evaluation_report.json"
    if not metrics_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Metrics not available. Pipeline may still be running or failed.",
        )
    return JSONResponse(content=json.loads(metrics_path.read_text(encoding="utf-8")))


@app.get("/pipeline/{job_id}/download", summary="Download the augmented dataset")
async def download_augmented(job_id: str):
    """
    Downloads the augmented dataset CSV produced by the integration step.
    """
    _read_status(job_id)  # validates job exists

    csv_path = _job_dir(job_id) / "integrated_data.csv"
    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Augmented dataset not available. Pipeline may still be running or failed.",
        )
    return FileResponse(
        path=str(csv_path),
        media_type="text/csv",
        filename=f"augmented_dataset_{job_id[:8]}.csv",
    )


@app.get("/pipeline/{job_id}/logs", summary="Get step-by-step logs")
async def get_logs(job_id: str):
    """
    Returns the stdout/stderr logs for each pipeline step.
    Useful for debugging failed jobs.
    """
    _read_status(job_id)  # validates job exists

    logs = {}
    for log_file in sorted(_job_dir(job_id).glob("*.log")):
        logs[log_file.stem] = log_file.read_text(encoding="utf-8")

    return JSONResponse(content=logs)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)