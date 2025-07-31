import datetime
import os
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from hdbcli import dbapi
from hdbcli.dbapi import Error as HdbError
from collections import deque
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler

# --- Basic App Setup ---
load_dotenv()
app = Flask(__name__)

# --- Database and Scheduler Configuration ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///hana_dashboard.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SCHEDULER_API_ENABLED'] = True

db = SQLAlchemy(app)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# --- In-Memory Alert Log ---
alert_history = deque(maxlen=50)

# --- Database Model for Historical Data ---
class KpiHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    cpu_usage = db.Column(db.Float)
    memory_usage = db.Column(db.Float)

# --- HANA Connection for On-Premise ---
def get_hana_connection():
    """Establishes a connection, now compatible with on-premise HANA Express."""
    try:
        use_encrypt = os.getenv("HANA_ENCRYPT", "false").lower() == "true"
        
        return dbapi.connect(
            address=os.getenv("HANA_ADDRESS"),
            port=int(os.getenv("HANA_PORT", 39015)),
            user=os.getenv("HANA_USER"),
            password=os.getenv("HANA_PASSWORD"),
            encrypt=use_encrypt,
            sslValidateCertificate=False # Important for self-signed certs
        )
    except HdbError as e:
        print(f"HANA Connection Error: {e}")
        return None

# --- Core Data Fetching & Logic for On-Premise (DEFINITIVE FINAL VERSION) ---
def get_hana_kpis():
    """Fetches real-time KPIs, with individual error handling for each query."""
    kpis = {
        'cpu_usage': 'N/A',
        'memory_usage': {'used': 'N/A', 'total': 'N/A'},
        'active_sessions': 'N/A',
        'error': None
    }
    conn = get_hana_connection()
    if not conn:
        kpis['error'] = "Database connection failed. Please check .env file and network."
        return kpis
    
    try:
        with conn.cursor() as cursor:
            # --- KPI 1: CPU Usage (Reverted to the only working version) ---
            try:
                # Using M_SYSTEM_OVERVIEW with the name 'CPU' is the most basic approach
                cursor.execute("SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE NAME = 'CPU'")
                cpu_result = cursor.fetchone()
                if cpu_result and cpu_result[0] is not None:
                    # Parse the 'Available ##, Used ##' format
                    parts = cpu_result[0].split(',')
                    available_cpu = float(parts[0].split()[-1])
                    used_cpu = float(parts[1].split()[-1])
                    kpis['cpu_usage'] = round((used_cpu / available_cpu) * 100, 2) if available_cpu > 0 else 0
            except HdbError as e:
                print(f"ERROR fetching CPU: {e}")
                kpis['cpu_usage'] = 'Error'

            # --- KPI 2: Memory Usage (Reverted to the correct, modern query) ---
            try:
                cursor.execute("SELECT ROUND(SUM(TOTAL_MEMORY_USED_SIZE) / 1024/1024/1024, 2), ROUND(MAX(EFFECTIVE_ALLOCATION_LIMIT) / 1024/1024/1024, 2) FROM M_SERVICE_MEMORY")
                mem_result = cursor.fetchone()
                if mem_result and mem_result[0] is not None:
                    kpis['memory_usage'] = {'used': mem_result[0], 'total': mem_result[1]}
            except HdbError as e:
                print(f"ERROR fetching Memory: {e}")
                kpis['memory_usage'] = {'used': 'Error', 'total': 'Error'}

            # --- KPI 3: Active Sessions ---
            try:
                cursor.execute("SELECT COUNT(*) FROM M_CONNECTIONS WHERE CONNECTION_STATUS = 'RUNNING'")
                session_count = cursor.fetchone()
                if session_count:
                    kpis['active_sessions'] = session_count[0]
            except HdbError as e:
                print(f"ERROR fetching Sessions: {e}")
                kpis['active_sessions'] = 'Error'
                
    except HdbError as e:
        kpis['error'] = f"A critical SQL error occurred: {e}"
    finally:
        if conn: conn.close()
    return kpis

# --- Background Job to Log KPIs ---
@scheduler.task('interval', id='log_kpi_job', seconds=60, misfire_grace_time=900)
def log_kpi_job():
    with app.app_context():
        print("Running scheduled KPI logging job...")
        kpis = get_hana_kpis()
        if not kpis.get('error') and isinstance(kpis.get('cpu_usage'), (int, float)) and isinstance(kpis.get('memory_usage', {}).get('used'), (int, float)):
            new_log = KpiHistory(
                cpu_usage=kpis['cpu_usage'],
                memory_usage=kpis['memory_usage']['used']
            )
            db.session.add(new_log)
            db.session.commit()
            print("KPIs successfully logged to database.")

# --- Page Rendering Routes ---
@app.route('/')
@app.route('/dashboard')
def dashboard(): return render_template('dashboard.html')

@app.route('/script-generator')
def script_generator_page(): return render_template('script_generator.html')

@app.route('/alerts')
def alerts_page(): return render_template('alerts.html', alerts=list(alert_history))

@app.route('/sessions')
def sessions_page(): return render_template('sessions.html')

@app.route('/table-explorer')
def table_explorer_page(): return render_template('table_explorer.html')

@app.route('/history')
def history_page(): return render_template('history.html')

@app.route('/health-check')
def health_check_page(): return render_template('health_check.html')


# --- API Endpoints ---
@app.route('/api/kpis')
def api_kpis(): return jsonify(get_hana_kpis())

@app.route('/api/execute-script', methods=['POST'])
def execute_script():
    script = request.json.get('script')
    conn = get_hana_connection()
    if not conn: return jsonify({'error': 'Database connection failed.'}), 500
    try:
        with conn.cursor() as cursor:
            cursor.execute(script)
            if script.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return jsonify({'success': True, 'columns': columns, 'rows': rows})
            else:
                conn.commit()
                return jsonify({'success': True, 'message': f'{cursor.rowcount} rows affected.'})
    except HdbError as e:
        return jsonify({'success': False, 'error': f'Execution failed: {e}'}), 500

@app.route('/api/sessions')
def api_sessions():
    conn = get_hana_connection()
    if not conn: return jsonify({'error': 'DB connection failed'}), 500
    try:
        with conn.cursor() as cursor:
            # Reverted to the only compatible version for your system
            cursor.execute("SELECT CONNECTION_ID, CLIENT_HOST, CLIENT_IP, CONNECTION_STATUS FROM M_CONNECTIONS WHERE CONNECTION_STATUS='RUNNING' ORDER BY CONNECTION_ID")
            columns = [desc[0] for desc in cursor.description]
            sessions = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return jsonify(sessions)
    except HdbError as e:
        return jsonify({'error': f'Failed to fetch sessions: {e}'}), 500

@app.route('/api/tables')
def api_tables():
    conn = get_hana_connection()
    if not conn: return jsonify({'error': 'DB connection failed'}), 500
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT TOP 100 SCHEMA_NAME, TABLE_NAME, RECORD_COUNT, ROUND(MEMORY_SIZE_IN_TOTAL / 1024 / 1024, 2) AS MEMORY_MB FROM M_CS_TABLES ORDER BY MEMORY_SIZE_IN_TOTAL DESC")
            columns = [desc[0] for desc in cursor.description]
            tables = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return jsonify(tables)
    except HdbError as e:
        return jsonify({'error': f'Failed to fetch tables: {e}'}), 500

@app.route('/api/historical-kpis')
def api_historical_kpis():
    one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    data = KpiHistory.query.filter(KpiHistory.timestamp >= one_day_ago).order_by(KpiHistory.timestamp).all()
    return jsonify({
        'labels': [d.timestamp.strftime('%H:%M') for d in data],
        'cpu': [d.cpu_usage for d in data],
        'memory': [d.memory_usage for d in data]
    })
    
@app.route('/api/health-check')
def api_health_check():
    checks = [
        {"name": "Last Successful Data Backup", "query": "SELECT TOP 1 SYS_START_TIME, ENTRY_TYPE_NAME, STATE_NAME FROM M_BACKUP_CATALOG WHERE ENTRY_TYPE_NAME = 'complete data backup' and STATE_NAME = 'successful' ORDER BY SYS_START_TIME DESC"},
        {"name": "Active Transactions", "query": "SELECT HOST, PORT, CONNECTION_ID, TRANSACTION_ID FROM M_TRANSACTIONS WHERE TRANSACTION_STATUS = 'ACTIVE'"},
    ]
    report = []
    conn = get_hana_connection()
    if not conn: return jsonify([{'name': 'Connection', 'status': 'Error', 'details': 'Could not connect to HANA DB.'}])
    
    with conn.cursor() as cursor:
        for check in checks:
            try:
                cursor.execute(check['query'])
                rows = cursor.fetchall()
                if rows:
                    report.append({'name': check['name'], 'status': 'Warning', 'details': f'{len(rows)} issue(s) found. First row: {rows[0]}'})
                else:
                    report.append({'name': check['name'], 'status': 'OK', 'details': 'No issues found.'})
            except HdbError as e:
                report.append({'name': check['name'], 'status': 'Error', 'details': f'Query failed: {e}'})
    conn.close()
    return jsonify(report)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, use_reloader=False)