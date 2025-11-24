import logging
from datetime import datetime
import threading

logger = logging.getLogger(__name__)

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_transaction(self, node_id, operation, tconst, status, error_message=None):
        """Log transaction to transaction_log table"""
        query = """
            INSERT INTO transaction_log (node_id, operation, tconst, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.db.execute_query('node1', query, (node_id, operation, tconst, status, error_message))
    
    def insert_title(self, data):
        """Insert title with replication to appropriate nodes"""
        tconst = data.get('tconst')
        title_type = data.get('title_type')
        
        # Determine target nodes based on title_type
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        query = """
            INSERT INTO titles (tconst, title_type, primary_title, start_year, runtime_minutes, genres)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            tconst,
            title_type,
            data.get('primary_title'),
            data.get('start_year'),
            data.get('runtime_minutes'),
            data.get('genres')
        )
        
        results = {}
        
        # Insert to Node 1 (central)
        result1 = self.db.execute_query('node1', query, params)
        results['node1'] = result1
        self.log_transaction('node1', 'INSERT', tconst, 'SUCCESS' if result1['success'] else 'FAILED', 
                           result1.get('error'))
        
        # Insert to fragment node (Node 2 or 3)
        result_frag = self.db.execute_query(target_node, query, params)
        results[target_node] = result_frag
        self.log_transaction(target_node, 'INSERT', tconst, 'SUCCESS' if result_frag['success'] else 'FAILED',
                           result_frag.get('error'))
        
        return {
            'success': result1['success'] and result_frag['success'],
            'results': results
        }
    
    def update_title(self, tconst, data, isolation_level='READ-COMMITTED'):
        """Update title with replication"""
        # First, determine which fragment node has this record
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        # Build update query
        set_clauses = []
        params = []
        
        for key, value in data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        params.append(tconst)
        
        query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
        
        results = {}
        
        # Update Node 1
        result1 = self.db.execute_query('node1', query, params, isolation_level)
        results['node1'] = result1
        self.log_transaction('node1', 'UPDATE', tconst, 'SUCCESS' if result1['success'] else 'FAILED',
                           result1.get('error'))
        
        # Update fragment node
        result_frag = self.db.execute_query(target_node, query, params, isolation_level)
        results[target_node] = result_frag
        self.log_transaction(target_node, 'UPDATE', tconst, 'SUCCESS' if result_frag['success'] else 'FAILED',
                           result_frag.get('error'))
        
        return {
            'success': result1['success'] and result_frag['success'],
            'results': results
        }
    
    def delete_title(self, tconst):
        """Delete title with replication"""
        # Get title info first
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        query = "DELETE FROM titles WHERE tconst = %s"
        
        results = {}
        
        # Delete from Node 1
        result1 = self.db.execute_query('node1', query, (tconst,))
        results['node1'] = result1
        self.log_transaction('node1', 'DELETE', tconst, 'SUCCESS' if result1['success'] else 'FAILED',
                           result1.get('error'))
        
        # Delete from fragment node
        result_frag = self.db.execute_query(target_node, query, (tconst,))
        results[target_node] = result_frag
        self.log_transaction(target_node, 'DELETE', tconst, 'SUCCESS' if result_frag['success'] else 'FAILED',
                           result_frag.get('error'))
        
        return {
            'success': result1['success'] and result_frag['success'],
            'results': results
        }
    
    def test_concurrent_reads(self, tconst):
        """Test Case 1: Concurrent reads on same data"""
        # Simulate multiple nodes reading same data simultaneously
        results = {}
        threads = []
        
        def read_from_node(node_name):
            conn = self.db.get_connection(node_name)
            if conn:
                try:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data = cursor.fetchone()
                    results[node_name] = {'success': True, 'data': data}
                except Exception as e:
                    results[node_name] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
        
        for node in ['node1', 'node2', 'node3']:
            t = threading.Thread(target=read_from_node, args=(node,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        return {'test': 'concurrent_read', 'results': results}
    
    def test_concurrent_writes(self, data):
        """Test Case 2 & 3: Concurrent writes"""
        # Implementation for concurrent write testing
        return {'test': 'concurrent_write', 'message': 'To be implemented'}
    
    def simulate_failure(self, scenario):
        """Simulate node failure scenarios"""
        # Implementation for failure simulation
        return {'scenario': scenario, 'message': 'To be implemented'}