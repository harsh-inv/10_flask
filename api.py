# api.py
from flask import Flask, request, jsonify
import sqlite3
import os
from org_1_2907 import DataQualityChecker  # from your existing file

app = Flask(__name__)

# Temporary folder for uploads
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Data Quality API is running"})

@app.route("/run_checks", methods=["POST"])
def run_checks():
    """
    Triggered ONLY when UI5 user clicks 'Run Checks'
    Expects: data_quality.csv, system_codes.csv, db_path (optional)
    """
    try:
        dq_file = request.files.get("data_quality")
        sc_file = request.files.get("system_codes")
        db_path = request.form.get("db_path", "test.db")  # default db

        if not dq_file or not sc_file:
            return jsonify({"error": "Both data_quality and system_codes files are required"}), 400

        dq_path = os.path.join(UPLOAD_FOLDER, "data_quality.csv")
        sc_path = os.path.join(UPLOAD_FOLDER, "system_codes.csv")
        dq_file.save(dq_path)
        sc_file.save(sc_path)

        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        dq_checker = DataQualityChecker(conn)

        # Load configs
        if not dq_checker.load_checks_config(dq_path):
            return jsonify({"error": "Failed to load data_quality config"}), 500
        if not dq_checker.load_system_codes_config(sc_path):
            return jsonify({"error": "Failed to load system_codes config"}), 500

        # Run all checks (this is the heavy operation)
        results = dq_checker.run_all_checks()
        conn.close()

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
