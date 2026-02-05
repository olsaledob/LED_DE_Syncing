import os
import sys
from syncing.config import SyncConfig
from syncing.yline_check import find_yline_after_start_handshake
from syncing.logging_setup import setup_logging
from syncing.file_matching import match_led_file, extract_expected_diff_from_filename
from syncing.digital_events import DigitalEvents
from syncing.arduino_led import ArduinoLEDLogs
from syncing.handshake import HandshakeDetector
from syncing.anomalies import AnomalyHandler
from syncing.utils import trim_to_handshake_windows, trim_with_index_mapping
from syncing.fix_anomalies import AnomalyFixer
import numpy as np
import csv
import json
from datetime import datetime
import traceback

def main():
    # Load configuration
    cfg = SyncConfig.load("config.toml")

    # Setup logging globally
    logger = setup_logging(cfg.log_dir, cfg.log_level)
    logger.info("______________________")
    logger.info("Starting Sync Pipeline")

    run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    summary_rows = []

    try:
        # Iterate over MEA files
        for mea_filename in os.listdir(cfg.path_h5_dir):
            if not mea_filename.endswith(".h5"):
                continue  # skip non-HDF5 files

            mea_path = os.path.join(cfg.path_h5_dir, mea_filename)
            logger.info(f"Processing MEA file: {mea_filename}")

            row = {
                "run_id": run_id,
                "mea_file": mea_filename,
                "mea_path": mea_path,
                "led_file": None,
                "status": "started",
                "error_type": None,
                "error_message": None,
                "yline_error_found": False,
                "yline_error_count": 0,
                "yline_error_details": None,
                "note": ""
            }

            try:
                # RecID filtering
                recid_str = next((part for part in mea_filename.split("_") if "RecID" in part), None)
                if cfg.rec_id_start or cfg.rec_id_end:
                    if not recid_str:
                        logger.warning(f"No RecID in filename, skipping: {mea_filename}")
                        row["status"] = "skipped"
                        row["note"] = "Skipped: no RecID in filename."
                        summary_rows.append(row)
                        continue
                    recid_num = int(recid_str.replace("RecID", "").replace("_", ""))
                    if cfg.rec_id_start and recid_num < cfg.rec_id_start:
                        row["status"] = "skipped"
                        row["note"] = f"Skipped: RecID {recid_num} < rec_id_start {cfg.rec_id_start}."
                        summary_rows.append(row)
                        continue
                    if cfg.rec_id_end and recid_num > cfg.rec_id_end:
                        row["status"] = "skipped"
                        row["note"] = f"Skipped: RecID {recid_num} > rec_id_end {cfg.rec_id_end}."
                        summary_rows.append(row)
                        continue

                # Match LED file
                try:
                    led_path = match_led_file(mea_filename, cfg.path_led_dir)
                except Exception as e:
                    logger.error(f"Could not match LED file for {mea_filename}: {e}")
                    row["status"] = "failed"
                    row["error_type"] = type(e).__name__
                    row["error_message"] = str(e)
                    row["note"] = "Failed during LED log matching."
                    summary_rows.append(row)
                    continue

                if not led_path:
                    row["status"] = "failed"
                    row["error_type"] = "LedLogNotFoundError"
                    row["error_message"] = "match_led_file returned None (likely no RecID found in MEA filename)."
                    row["note"] = "Failed during LED log matching."
                    summary_rows.append(row)
                    continue

                row["led_file"] = os.path.basename(led_path)
                logger.info(f"Matched LED file: {os.path.basename(led_path)}")

                # Load data
                digital = DigitalEvents(mea_path).load(cfg.sync_duration_sec, cfg.post_stim_phase)
                arduino = ArduinoLEDLogs(led_path, cfg).load()

                # Detect handshakes
                detector = HandshakeDetector(cfg)
                handshake_pairs_led, start_names_led, stop_names_led = detector.find(
                    arduino.timestamps, tolerance=cfg.threshold
                )

                y_findings = find_yline_after_start_handshake(
                    arduino_linetypes=arduino.linetypes,
                    handshake_pairs=handshake_pairs_led,
                    byte_y=cfg.bytes["BYTE_Y"]
                )
                
                # Because YLineErrors only occur in full stimulus blocks and never for just a number of lines
                # after starting stimulation, this error is excepted and only logged as info!
                # This choice was made because STAs and other analyses more easily ignore the full block instead
                # of missing this information entirely.
                if y_findings:
                    row["yline_error_found"] = True
                    row["yline_error_count"] = len(y_findings)
                    row["yline_error_details"] = y_findings
                    row["note"] = (row["note"] + " " if row["note"] else "") + \
                                f"NOTE: YLineError detected after start handshake ({len(y_findings)} occurrence(s))."
                    logger.warning(
                        f"YLineError: first post-start-handshake line is Y in {len(y_findings)} case(s). "
                        f"Details: {y_findings}"
                    )

                handshake_pairs_mea, start_names_mea, stop_names_mea = detector.find(
                    digital.timestamps, tolerance=cfg.threshold
                )

                logger.info(f"LED start handshake types: {start_names_led}")
                logger.info(f"LED stop handshake types: {stop_names_led}")
                logger.info(f"MEA start handshake types: {start_names_mea}")
                logger.info(f"MEA stop handshake types: {stop_names_mea}")

                # Trim handshake windows
                stim_led_ts, trimmed_to_original_idx = trim_with_index_mapping(arduino.timestamps, handshake_pairs_led)
                stim_mea_ts = trim_to_handshake_windows(digital.timestamps, handshake_pairs_mea)
                logger.info(f"Stimulus-phase LED timestamps: {len(stim_led_ts)}")
                logger.info(f"Stimulus-phase MEA timestamps: {len(stim_mea_ts)}")

                # Determine expected diff from filename
                expected_diff = extract_expected_diff_from_filename(mea_filename)
                logger.info(f"Expected diff (µs) = {expected_diff}")

                # Detect anomalies
                handler = AnomalyHandler(expected_diff=expected_diff,
                                         threshold=cfg.threshold,
                                         pause_threshold=300_000)
                anomalies = handler.detect_anomalies(stim_led_ts)
                logger.info(f"Anomalies detected: {anomalies}")

                # Map anomalies back to original indices
                remapped_anomalies = [(trimmed_to_original_idx[trim_idx], anom_type)
                                      for trim_idx, anom_type in anomalies]
                logger.info(f"Remapped anomalies")

                # Fix anomalies
                fixer = AnomalyFixer(expected_diff=expected_diff,
                                     threshold=cfg.threshold,
                                     anomalies=remapped_anomalies)
                base_filename = os.path.splitext(mea_filename)[0]
                output_npz = os.path.join(cfg.path_to_results, f"{base_filename}_corrected.npz")
                correction_csv = os.path.join(cfg.path_to_results, f"{base_filename}_correction_log.csv")

                fixer.fix(
                    arduino_ts=arduino.timestamps,
                    arduino_patterns=arduino.patterns,
                    arduino_linetypes=arduino.linetypes,
                    mea_ts=digital.timestamps,
                    output_npz_path=output_npz,
                    correction_log_path=correction_csv,
                    handshake_pairs=handshake_pairs_led,
                    start_names=start_names_led,
                    stop_names=stop_names_led
                )

                row["status"] = "success"
                summary_rows.append(row)
                logger.info(f"Successfully processed {mea_filename}")

            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected — stopping gracefully.")
                break
            except Exception as e:
                logger.error(f"Error processing {mea_filename}: {e}")
                logger.debug(traceback.format_exc())
                row["status"] = "failed"
                row["error_type"] = type(e).__name__
                row["error_message"] = str(e)
                summary_rows.append(row)
                continue

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt detected — shutting down gracefully.")
    finally:
        os.makedirs(cfg.log_dir, exist_ok=True)
        summary_csv = os.path.join(cfg.log_dir, f"sync_summary_{run_id}.csv")
        summary_json = os.path.join(cfg.log_dir, f"sync_summary_{run_id}.json")

        if summary_rows:
            fieldnames = list(summary_rows[0].keys())
            with open(summary_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(summary_rows)

            with open(summary_json, "w", encoding="utf-8") as f:
                json.dump(summary_rows, f, indent=2)

            logger.info(f"Wrote summary CSV:  {summary_csv}")
            logger.info(f"Wrote summary JSON: {summary_json}")
        else:
            logger.warning("No summary rows collected; nothing written.")

        logger.info("_______________________")
        logger.info("Sync Pipeline Completed")


if __name__ == "__main__":
    main()