import os
import sys
from syncing.config import SyncConfig
from syncing.logging_setup import setup_logging
from syncing.file_matching import match_led_file, extract_expected_diff_from_filename
from syncing.digital_events import DigitalEvents
from syncing.arduino_led import ArduinoLEDLogs
from syncing.handshake import HandshakeDetector
from syncing.anomalies import AnomalyHandler
from syncing.utils import trim_to_handshake_windows, trim_with_index_mapping
from syncing.fix_anomalies import AnomalyFixer
import numpy as np
import traceback

def main():
    # Load configuration
    cfg = SyncConfig.load("config.toml")

    # Setup logging globally
    logger = setup_logging(cfg.log_dir, cfg.log_level)
    logger.info("______________________")
    logger.info("Starting Sync Pipeline")

    processed_files = []
    failed_files = []

    try:
        # Iterate over MEA files
        for mea_filename in os.listdir(cfg.path_h5_dir):
            if not mea_filename.endswith(".h5"):
                continue  # skip non-HDF5 files

            mea_path = os.path.join(cfg.path_h5_dir, mea_filename)
            logger.info(f"Processing MEA file: {mea_filename}")

            try:
                # RecID filtering
                recid_str = next((part for part in mea_filename.split("_") if "RecID" in part), None)
                if cfg.rec_id_start or cfg.rec_id_end:
                    if not recid_str:
                        logger.warning(f"No RecID in filename, skipping: {mea_filename}")
                        continue
                    recid_num = int(recid_str.replace("RecID", "").replace("_", ""))
                    if cfg.rec_id_start and recid_num < cfg.rec_id_start:
                        continue
                    if cfg.rec_id_end and recid_num > cfg.rec_id_end:
                        continue

                # Match LED file
                try:
                    led_path = match_led_file(mea_filename, cfg.path_led_dir)
                except Exception as e:
                    logger.error(f"Could not match LED file for {mea_filename}: {e}")
                    failed_files.append((mea_filename, str(e)))
                    continue

                logger.info(f"Matched LED file: {os.path.basename(led_path)}")

                # Load data
                digital = DigitalEvents(mea_path).load(cfg.sync_duration_sec, cfg.post_stim_phase)
                arduino = ArduinoLEDLogs(led_path, cfg).load()

                # Detect handshakes
                detector = HandshakeDetector(cfg)
                handshake_pairs_led, start_names_led, stop_names_led = detector.find(
                    arduino.timestamps, tolerance=cfg.threshold
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
                    handshake_pairs=handshake_pairs_led
                )

                processed_files.append(mea_filename)
                logger.info(f"Successfully processed {mea_filename}")

            except KeyboardInterrupt:
                logger.warning("KeyboardInterrupt detected — stopping gracefully.")
                break
            except Exception as e:
                logger.error(f"Error processing {mea_filename}: {e}")
                logger.debug(traceback.format_exc())
                failed_files.append((mea_filename, str(e)))
                continue

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt detected — shutting down gracefully.")
    finally:
        if processed_files:
            logger.info(f"Processed {len(processed_files)} files successfully:")
            for f in processed_files:
                logger.info(f"  - {f}")

        if failed_files:
            logger.warning(f"{len(failed_files)} files failed:")
            for fname, err in failed_files:
                logger.warning(f"  - {fname}: {err}")

        logger.info("_______________________")
        logger.info("Sync Pipeline Completed")


if __name__ == "__main__":
    main()