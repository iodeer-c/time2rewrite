from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from time_query_service.service import QueryPipelineService


def _normalize_answer(value: str | None) -> str:
    return (value or "").strip()


def evaluate_testset(
    *,
    csv_path: str | Path,
    service: Any | None = None,
    rewrite: bool = True,
) -> dict[str, Any]:
    input_path = Path(csv_path)
    pipeline = service or QueryPipelineService()
    results: list[dict[str, Any]] = []
    matched_cases = 0
    errored_cases = 0

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            question = (row.get("question") or "").strip()
            expected_answer = row.get("answer")
            system_date = (row.get("base_date") or "").strip()
            timezone = (row.get("timezone") or "").strip()

            try:
                pipeline_output = pipeline.process_query(
                    query=question,
                    system_date=system_date,
                    timezone=timezone,
                    rewrite=rewrite,
                )
                actual_answer = pipeline_output.get("rewritten_query")
                matches_expected_answer = _normalize_answer(actual_answer) == _normalize_answer(expected_answer)
                matched_cases += int(matches_expected_answer)
                result = {
                    "id": row.get("id"),
                    "question": question,
                    "expected_answer": expected_answer,
                    "actual_answer": actual_answer,
                    "system_date": system_date,
                    "timezone": timezone,
                    "parsed_time_expressions": pipeline_output.get("parsed_time_expressions"),
                    "resolved_time_expressions": pipeline_output.get("resolved_time_expressions"),
                    "rewritten_query": pipeline_output.get("rewritten_query"),
                    "matches_expected_answer": matches_expected_answer,
                    "error": None,
                }
            except Exception as exc:  # pragma: no cover - exercised via integration usage
                errored_cases += 1
                result = {
                    "id": row.get("id"),
                    "question": question,
                    "expected_answer": expected_answer,
                    "actual_answer": None,
                    "system_date": system_date,
                    "timezone": timezone,
                    "parsed_time_expressions": None,
                    "resolved_time_expressions": None,
                    "rewritten_query": None,
                    "matches_expected_answer": False,
                    "error": str(exc),
                }

            results.append(result)

    total_cases = len(results)
    return {
        "summary": {
            "input_csv": str(input_path),
            "total_cases": total_cases,
            "matched_cases": matched_cases,
            "mismatched_cases": total_cases - matched_cases,
            "errored_cases": errored_cases,
            "rewrite_enabled": rewrite,
        },
        "results": results,
    }


def write_evaluation_report(
    *,
    report: dict[str, Any],
    detail_output_path: str | Path,
    summary_output_path: str | Path,
) -> None:
    detail_path = Path(detail_output_path)
    summary_path = Path(summary_output_path)
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with detail_path.open("w", encoding="utf-8", newline="") as handle:
        for result in report["results"]:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")

    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "id",
                "question",
                "expected_answer",
                "actual_answer",
                "matches_expected_answer",
                "error",
                "system_date",
                "timezone",
                "parsed_time_expressions",
                "resolved_time_expressions",
                "rewritten_query",
            ],
        )
        writer.writeheader()
        for result in report["results"]:
            writer.writerow(
                {
                    "id": result.get("id"),
                    "question": result.get("question"),
                    "expected_answer": result.get("expected_answer"),
                    "actual_answer": result.get("actual_answer"),
                    "matches_expected_answer": result.get("matches_expected_answer"),
                    "error": result.get("error"),
                    "system_date": result.get("system_date"),
                    "timezone": result.get("timezone"),
                    "parsed_time_expressions": json.dumps(
                        result.get("parsed_time_expressions"),
                        ensure_ascii=False,
                    ),
                    "resolved_time_expressions": json.dumps(
                        result.get("resolved_time_expressions"),
                        ensure_ascii=False,
                    ),
                    "rewritten_query": result.get("rewritten_query"),
                }
            )


def build_output_paths(*, input_csv_path: str | Path, output_dir: str | Path | None = None) -> tuple[Path, Path]:
    input_path = Path(input_csv_path)
    root_dir = Path(output_dir) if output_dir else input_path.parent / "artifacts"
    stem = input_path.stem
    return root_dir / f"{stem}_results.jsonl", root_dir / f"{stem}_summary.csv"


def run_testset(
    *,
    input_csv_path: str | Path,
    output_dir: str | Path | None = None,
    rewrite: bool = True,
    service: Any | None = None,
) -> dict[str, Any]:
    report = evaluate_testset(csv_path=input_csv_path, service=service, rewrite=rewrite)
    detail_path, summary_path = build_output_paths(input_csv_path=input_csv_path, output_dir=output_dir)
    write_evaluation_report(
        report=report,
        detail_output_path=detail_path,
        summary_output_path=summary_path,
    )
    report["summary"]["detail_output_path"] = str(detail_path)
    report["summary"]["summary_output_path"] = str(summary_path)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the time query pipeline against a CSV test set.")
    parser.add_argument("input_csv",
                        default="/Users/td/PycharmProjects/time2rewirte/time_query_testset_30.csv",
                        help="Path to the input CSV test set.")
    parser.add_argument(
        "--output-dir",
        default="/Users/td/PycharmProjects/time2rewirte/artifacts",
        help="Directory for the jsonl detail report and csv summary report.",
    )
    parser.add_argument(
        "--no-rewrite",
        action="store_true",
        help="Skip the rewrite stage. Useful when you only want parsed and resolved outputs.",
    )
    args = parser.parse_args(argv)

    report = run_testset(
        input_csv_path=args.input_csv,
        output_dir=args.output_dir,
        rewrite=not args.no_rewrite,
    )
    summary = report["summary"]
    print(f"Input CSV: {summary['input_csv']}")
    print(f"Total cases: {summary['total_cases']}")
    print(f"Matched cases: {summary['matched_cases']}")
    print(f"Mismatched cases: {summary['mismatched_cases']}")
    print(f"Errored cases: {summary['errored_cases']}")
    print(f"Detail report: {summary['detail_output_path']}")
    print(f"Summary report: {summary['summary_output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# python -m time_query_service.evaluator /Users/td/PycharmProjects/time2rewirte/time_query_testset_30.csv
