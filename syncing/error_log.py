import os
import pandas as pd
from collections import Counter

def write_error_log(log_dir: str,
                    base_name: str,
                    handshake_pairs,
                    start_names,
                    stop_names,
                    anomalies,
                    index_offset_series=None):
    
    os.makedirs(log_dir, exist_ok=True)

    # handshake summary
    hs_rows = []
    for n, ((start_range, stop_range), sname, tname) in enumerate(zip(handshake_pairs, start_names, stop_names), start=1):
        # diff-index ranges
        sb, se = start_range
        tb, te = stop_range

        # convert to timestamp-index ranges (diff i refers to ts[i] -> ts[i+1])
        start_ts_range = (sb, se + 1)
        stop_ts_range  = (tb, te + 1)

        row = {
            "handshake_id": n,
            "start_type": sname,
            "stop_type": tname,
            "start_diff_begin": sb, "start_diff_end": se,
            "stop_diff_begin": tb, "stop_diff_end": te,
            "start_ts_begin": start_ts_range[0], "start_ts_end": start_ts_range[1],
            "stop_ts_begin": stop_ts_range[0], "stop_ts_end": stop_ts_range[1],
        }

        if index_offset_series is not None:
            def corr_idx(k):
                off = index_offset_series[k] if k < len(index_offset_series) else index_offset_series[-1]
                return k + off
            row.update({
                "start_ts_begin_corrected": corr_idx(start_ts_range[0]),
                "start_ts_end_corrected": corr_idx(start_ts_range[1]),
                "stop_ts_begin_corrected": corr_idx(stop_ts_range[0]),
                "stop_ts_end_corrected": corr_idx(stop_ts_range[1]),
            })

        hs_rows.append(row)

    # anomaly summary counts
    anomaly_counts = Counter([t for _, t in anomalies])
    anomaly_rows = [{"anomaly_type": k, "count": v} for k, v in sorted(anomaly_counts.items())]

    # write files
    hs_path = os.path.join(log_dir, f"{base_name}_handshake_summary.csv")
    an_path = os.path.join(log_dir, f"{base_name}_anomaly_summary.csv")

    pd.DataFrame(hs_rows).to_csv(hs_path, index=False)
    pd.DataFrame(anomaly_rows).to_csv(an_path, index=False)

    return hs_path, an_path