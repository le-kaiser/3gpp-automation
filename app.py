from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import threading
import json
import os
from main import run_spec_tracking
import logging

# Suppress Werkzeug logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["127.0.0.1", "localhost", "0.0.0.0"]}})

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(BASE_DIR, 'progress.json')
LOG_FILE = os.path.join(BASE_DIR, 'tracker.log')
RESULTS_FILE = os.path.join(BASE_DIR, 'results.json')

def run_tracker_in_background(spec_number, results_callback):
    """Function to run the spec tracking in a background thread."""
    run_spec_tracking(spec_number=spec_number, progress_callback=update_progress, results_callback=results_callback)

def update_progress(progress):
    """Callback function to update the progress file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({'progress': progress}, f)

def update_results(result):
    """Callback function to update the results file."""
    if not os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'w') as f:
            json.dump([], f)
    
    with open(RESULTS_FILE, 'r+') as f:
        data = json.load(f)
        data.append(result)
        f.seek(0)
        json.dump(data, f)

@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@app.route('/start-tracking', methods=['POST'])
def start_tracking():
    """Start the spec tracking process."""
    spec_number = request.json.get('spec_number')
    if not spec_number:
        return jsonify({'error': 'Spec number is required'}), 400

    # Initialize files
    update_progress(0)
    with open(LOG_FILE, 'w'):
        pass
    with open(RESULTS_FILE, 'w') as f:
        json.dump([], f)

    # Run the tracking in a background thread
    thread = threading.Thread(target=run_tracker_in_background, args=(spec_number, update_results))
    thread.start()

    return jsonify({'message': 'Tracking started'})

@app.route('/progress')
def progress():
    """Get the current tracking progress."""
    if not os.path.exists(PROGRESS_FILE):
        return jsonify({'progress': 0})

    with open(PROGRESS_FILE, 'r') as f:
        try:
            data = json.load(f)
            return jsonify(data)
        except json.JSONDecodeError:
            return jsonify({'progress': 0})

@app.route('/logs')
def logs():
    """Get the logs."""
    if not os.path.exists(LOG_FILE):
        return ""
    
    with open(LOG_FILE, 'r') as f:
        return f.read()

@app.route('/results')
def results():
    """Get the results."""
    if not os.path.exists(RESULTS_FILE):
        return jsonify([])

    with open(RESULTS_FILE, 'r') as f:
        try:
            data = json.load(f)
            return jsonify(data)
        except json.JSONDecodeError:
            return jsonify([])

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5002)
