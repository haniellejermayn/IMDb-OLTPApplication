from flask import Flask, request, jsonify
from flask_cors import CORS
from db_manager import DatabaseManager
from replication_manager import ReplicationManager
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = DatabaseManager()
replication_manager = ReplicationManager(db_manager)

@app.route('/health', methods=['GET'])
def health_check():
    """Check node health status"""
    return jsonify(db_manager.check_all_nodes())

@app.route('/titles', methods=['GET'])
def get_titles():
    """Get titles with pagination"""
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    title_type = request.args.get('type', None)
    
    result = db_manager.get_titles(page, limit, title_type)
    return jsonify(result)

@app.route('/title/<tconst>', methods=['GET'])
def get_title(tconst):
    """Get single title by ID"""
    result = db_manager.get_title_by_id(tconst)
    return jsonify(result)

@app.route('/title', methods=['POST'])
def create_title():
    """Create new title"""
    data = request.json
    result = replication_manager.insert_title(data)
    return jsonify(result), 201 if result['success'] else 500

@app.route('/title/<tconst>', methods=['PUT'])
def update_title(tconst):
    """Update existing title"""
    data = request.json
    isolation_level = request.args.get('isolation', 'READ-COMMITTED')
    result = replication_manager.update_title(tconst, data, isolation_level)
    return jsonify(result)

@app.route('/title/<tconst>', methods=['DELETE'])
def delete_title(tconst):
    """Delete title"""
    result = replication_manager.delete_title(tconst)
    return jsonify(result)

@app.route('/simulate/concurrent-read', methods=['POST'])
def simulate_concurrent_read():
    """Test Case 1: Concurrent reads"""
    tconst = request.json.get('tconst')
    result = replication_manager.test_concurrent_reads(tconst)
    return jsonify(result)

@app.route('/simulate/concurrent-write', methods=['POST'])
def simulate_concurrent_write():
    """Test Case 2 & 3: Concurrent writes"""
    data = request.json
    result = replication_manager.test_concurrent_writes(data)
    return jsonify(result)

@app.route('/simulate/node-failure', methods=['POST'])
def simulate_node_failure():
    """Test Cases 1-4: Node failure scenarios"""
    scenario = request.json.get('scenario')
    result = replication_manager.simulate_failure(scenario)
    return jsonify(result)

@app.route('/logs', methods=['GET'])
def get_transaction_logs():
    """Get transaction logs"""
    limit = int(request.args.get('limit', 50))
    result = db_manager.get_transaction_logs(limit)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)