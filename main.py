#!/usr/bin/env python3
"""
Flask Backend for TrainingPeaks File Processor - UPDATED WITH PACE SUPPORT

NEW FEATURES:
- Extracts pace targets from TrainingPeaks workout structure files
- Converts speed (m/s) to pace (min/km) format
- Outputs target_pace_low and target_pace_high in CSV

Requirements:
    pip install flask flask-cors fitparse --break-system-packages
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import io
import csv
import gzip
import zipfile
from datetime import datetime, timedelta
from fitparse import FitFile
import statistics

app = Flask(__name__)
CORS(app)

# ============================================================================
# FIT PARSING FUNCTIONS
# ============================================================================

def calculate_hr_drift(records, start_time, duration_seconds):
    """Calculate HR drift for laps > 5 minutes"""
    if duration_seconds < 300:
        return None
    
    lap_hr_values = []
    end_time = start_time + timedelta(seconds=duration_seconds)
    for record in records:
        if 'timestamp' in record and 'heart_rate' in record:
            if record['timestamp'] >= start_time and record['timestamp'] < end_time:
                if record['heart_rate'] is not None:
                    lap_hr_values.append((record['timestamp'], record['heart_rate']))
    
    if len(lap_hr_values) < 10:
        return None
    
    mid_point = len(lap_hr_values) // 2
    first_half = [hr for _, hr in lap_hr_values[:mid_point]]
    second_half = [hr for _, hr in lap_hr_values[mid_point:]]
    
    if not first_half or not second_half:
        return None
    
    first_half_avg = statistics.mean(first_half)
    second_half_avg = statistics.mean(second_half)
    
    if first_half_avg == 0:
        return None
    
    drift = ((second_half_avg - first_half_avg) / first_half_avg) * 100
    return round(drift, 2)

def seconds_to_pace(seconds_per_meter):
    """Convert seconds per meter to min/km pace format"""
    if seconds_per_meter is None or seconds_per_meter == 0:
        return None
    seconds_per_km = seconds_per_meter * 1000
    minutes = int(seconds_per_km // 60)
    seconds = int(seconds_per_km % 60)
    return f"{minutes}:{seconds:02d}"

def parse_fit_file(fit_data):
    """Extract session, lap, and record data from FIT file bytes"""
    fitfile = FitFile(io.BytesIO(fit_data))
    
    session_data = {}
    lap_data = []
    record_data = []
    
    for record in fitfile.get_messages():
        if record.name == 'session':
            for field in record.fields:
                if field.value is not None or field.name not in session_data:
                    session_data[field.name] = field.value
        elif record.name == 'lap':
            lap = {}
            for field in record.fields:
                if field.value is not None or field.name not in lap:
                    lap[field.name] = field.value
            lap_data.append(lap)
        elif record.name == 'record':
            record_dict = {}
            for field in record.fields:
                record_dict[field.name] = field.value
            record_data.append(record_dict)
    
    return session_data, lap_data, record_data

def create_lap_data_csv_content(lap_data, record_data):
    """Create lap data CSV content as string"""
    output = io.StringIO()
    fieldnames = [
        'lap_number', 'lap_name', 'start_time', 'duration_seconds',
        'distance_meters', 'avg_heart_rate', 'max_heart_rate',
        'avg_pace', 'avg_cadence', 'avg_power', 'hr_drift'
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for idx, lap in enumerate(lap_data, 1):
        start_time = lap.get('start_time')
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else ''
        
        duration = lap.get('total_elapsed_time')
        distance = lap.get('total_distance')
        avg_hr = lap.get('avg_heart_rate')
        max_hr = lap.get('max_heart_rate')
        
        # Use enhanced_avg_speed for pace
        avg_speed = lap.get('enhanced_avg_speed') or lap.get('avg_speed')
        avg_pace = None
        if avg_speed and avg_speed > 0:
            avg_pace = seconds_to_pace(1.0 / avg_speed)
        
        # Use avg_running_cadence × 2 for steps/min
        avg_cadence = lap.get('avg_running_cadence') or lap.get('avg_cadence')
        avg_fractional = lap.get('avg_fractional_cadence', 0)
        if avg_cadence:
            avg_cadence = round((avg_cadence + avg_fractional) * 2)
        
        avg_power = lap.get('avg_power')
        
        lap_name = ''
        intensity = lap.get('intensity')
        if intensity:
            lap_name = str(intensity)
        
        hr_drift = None
        if start_time and duration:
            hr_drift = calculate_hr_drift(record_data, start_time, duration)
        
        writer.writerow({
            'lap_number': idx,
            'lap_name': lap_name,
            'start_time': start_time_str,
            'duration_seconds': duration or '',
            'distance_meters': distance or '',
            'avg_heart_rate': avg_hr or '',
            'max_heart_rate': max_hr or '',
            'avg_pace': avg_pace or '',
            'avg_cadence': avg_cadence or '',
            'avg_power': avg_power or '',
            'hr_drift': hr_drift if hr_drift is not None else ''
        })
    
    return output.getvalue()

# ============================================================================
# STRUCTURE PARSING WITH PACE SUPPORT (UPDATED)
# ============================================================================

def convert_hr(encoded_value):
    """Convert encoded HR value to actual BPM (subtract 100)"""
    if encoded_value is None:
        return None
    return encoded_value - 100

def speed_to_pace_seconds(speed_ms):
    """Convert speed (m/s) to pace (seconds per km)"""
    if speed_ms is None or speed_ms == 0:
        return None
    seconds_per_km = 1000 / speed_ms
    return round(seconds_per_km)

def pace_seconds_to_string(seconds_per_km):
    """Convert pace in seconds per km to M:SS format"""
    if seconds_per_km is None:
        return None
    minutes = int(seconds_per_km // 60)
    seconds = int(seconds_per_km % 60)
    return f"{minutes}:{seconds:02d}"

def get_zone_name(hr_low, hr_high):
    """Determine zone name based on HR ranges"""
    if hr_low is None or hr_high is None:
        return ""
    
    if hr_high <= 130:
        return "Zone 1: Recovery"
    elif hr_high <= 145:
        if hr_low < 130:
            return "Zone 1: Recovery - Zone 2: Aerobic"
        return "Zone 2: Aerobic"
    elif hr_high <= 160:
        if hr_low < 145:
            return "Zone 2: Aerobic - Zone 3: Tempo"
        return "Zone 3: Tempo"
    elif hr_high <= 175:
        return "Zone 4: Threshold"
    else:
        return "Zone 5: VO2 Max"

def parse_workout_structure(fit_data):
    """Parse workout structure FIT file"""
    fitfile = FitFile(io.BytesIO(fit_data))
    
    workout_info = {}
    workout_steps = []
    
    for record in fitfile.get_messages():
        if record.name == 'workout':
            for field in record:
                workout_info[field.name] = field.value
        elif record.name == 'workout_step':
            step_data = {}
            for field in record:
                step_data[field.name] = field.value
            workout_steps.append(step_data)
    
    return workout_info, workout_steps

def create_structure_csv_content(workout_steps):
    """Create structure CSV content with HR zones AND pace targets"""
    output = io.StringIO()
    fieldnames = [
        'step_number', 'step_name', 'duration_seconds', 
        'target_zone_low', 'target_zone_high', 'target_zone_name',
        'target_pace_low', 'target_pace_high',
        'step_type', 'notes'
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for idx, step in enumerate(workout_steps, start=1):
        step_name = step.get('wkt_step_name', '')
        duration_type = step.get('duration_type', '')
        duration_time = step.get('duration_time')
        intensity = step.get('intensity', '')
        
        # Extract HR zones
        hr_low_encoded = step.get('custom_target_heart_rate_low')
        hr_high_encoded = step.get('custom_target_heart_rate_high')
        hr_low = convert_hr(hr_low_encoded)
        hr_high = convert_hr(hr_high_encoded)
        
        zone_name = get_zone_name(hr_low, hr_high)
        
        # Extract pace targets (NEW)
        speed_low_ms = step.get('custom_target_speed_low')
        speed_high_ms = step.get('custom_target_speed_high')
        
        # Convert speed to pace (inverted)
        pace_low_sec = speed_to_pace_seconds(speed_high_ms)
        pace_high_sec = speed_to_pace_seconds(speed_low_ms)
        
        pace_low = pace_seconds_to_string(pace_low_sec) if pace_low_sec else ''
        pace_high = pace_seconds_to_string(pace_high_sec) if pace_high_sec else ''
        
        # Determine step type
        if duration_type == 'repeat_until_steps_cmplt':
            step_type = 'repeat'
        elif intensity == 'warmup':
            step_type = 'warmup'
        elif intensity == 'cooldown':
            step_type = 'cooldown'
        elif intensity == 'rest':
            step_type = 'rest'
        elif intensity == 'active':
            step_type = 'active'
        elif intensity == 'recovery':
            step_type = 'recovery'
        else:
            step_type = intensity or ''
        
        # Parse duration and notes
        duration_seconds = None
        notes = ''
        
        if duration_type == 'time' and duration_time:
            duration_seconds = int(duration_time)
        elif duration_type == 'open':
            notes = 'Open duration (until lap button pressed)'
        elif duration_type == 'repeat_until_steps_cmplt':
            repeat_steps = step.get('repeat_steps', 0)
            duration_step = step.get('duration_step', 0)
            notes = f'Repeat previous {repeat_steps} step(s) until step {duration_step} completes'
        
        writer.writerow({
            'step_number': idx,
            'step_name': step_name,
            'duration_seconds': duration_seconds if duration_seconds else '',
            'target_zone_low': hr_low if hr_low is not None else '',
            'target_zone_high': hr_high if hr_high is not None else '',
            'target_zone_name': zone_name,
            'target_pace_low': pace_low,
            'target_pace_high': pace_high,
            'step_type': step_type,
            'notes': notes
        })
    
    return output.getvalue()

# ============================================================================
# FLASK API ENDPOINTS
# ============================================================================

@app.route('/parse-lap-data', methods=['POST'])
def parse_lap_data():
    """Parse lap data FIT file (.fit or .fit.gz)"""
    try:
        file = request.files['file']
        file_data = file.read()
        
        if file.filename.endswith('.gz'):
            file_data = gzip.decompress(file_data)
        
        session_data, lap_data, record_data = parse_fit_file(file_data)
        csv_content = create_lap_data_csv_content(lap_data, record_data)
        
        return jsonify({
            'success': True,
            'csv': csv_content,
            'lap_count': len(lap_data)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/parse-structure', methods=['POST'])
def parse_structure():
    """Parse workout structure FIT file - NOW INCLUDES PACE TARGETS"""
    try:
        file = request.files['file']
        file_data = file.read()
        
        workout_info, workout_steps = parse_workout_structure(file_data)
        csv_content = create_structure_csv_content(workout_steps)
        
        return jsonify({
            'success': True,
            'csv': csv_content,
            'step_count': len(workout_steps)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/unzip', methods=['POST'])
def unzip_file():
    """Unzip a .zip file and return the CSV content"""
    try:
        file = request.files['file']
        
        with zipfile.ZipFile(io.BytesIO(file.read())) as zip_ref:
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            
            if not csv_files:
                return jsonify({
                    'success': False,
                    'error': 'No CSV file found in zip'
                }), 400
            
            csv_content = zip_ref.read(csv_files[0]).decode('utf-8')
            
            return jsonify({
                'success': True,
                'csv': csv_content,
                'filename': csv_files[0]
            })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'TrainingPeaks FIT Parser API v2.0 (with PACE support)'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("=" * 70)
    print("TrainingPeaks File Processor Backend v2.0 - WITH PACE SUPPORT")
    print("=" * 70)
    print(f"\nStarting Flask server on 0.0.0.0:{port}")
    print("\nEndpoints:")
    print("  POST /parse-lap-data    - Parse lap data FIT files")
    print("  POST /parse-structure   - Parse workout structure (HR + PACE)")
    print("  POST /unzip             - Unzip and extract CSV")
    print("  GET  /health            - Health check")
    print("\nNEW: Extracts pace targets from TrainingPeaks workouts")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=port, debug=False)
