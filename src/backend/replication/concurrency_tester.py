import logging
import threading
import time

logger = logging.getLogger(__name__)

# TODO: make use of ReplicationManager's methods for consistency

class ConcurrencyTester:
    def __init__(self, db_manager, replication_manager):
        self.db = db_manager
        self.replication_manager = replication_manager
    
    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        """
        Test Case 1: Concurrent reads on same data
        Multiple nodes reading simultaneously
        """
        results = {}
        threads = []
        lock = threading.Lock()
        
        def read_from_node(node_name):
            conn = self.db.get_connection(node_name, isolation_level)
            if conn:
                try:
                    start_time = time.time()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data = cursor.fetchone()
                    end_time = time.time()
                    
                    with lock:
                        results[node_name] = {
                            'success': True,
                            'data': data,
                            'read_time': end_time - start_time,
                            'isolation_level': isolation_level
                        }
                except Exception as e:
                    with lock:
                        results[node_name] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
            else:
                with lock:
                    results[node_name] = {'success': False, 'error': 'Connection failed'}
        
        # start concurrent reads
        for node in ['node1', 'node2', 'node3']:
            t = threading.Thread(target=read_from_node, args=(node,))
            threads.append(t)
            t.start()
        
        # wait for all reads to complete
        for t in threads:
            t.join()
        
        # check consistency
        data_values = [r['data'] for r in results.values() if r.get('success') and r.get('data')]
        consistent = len(set(str(d) for d in data_values)) <= 1 if data_values else True
        
        return {
            'test': 'concurrent_read',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'results': results,
            'consistent': consistent
        }
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        """
        Test Case 2 & 3: Concurrent writes
        Multiple updates happening simultaneously on same/different data
        """
        results = {}
        threads = []
        lock = threading.Lock()
        
        def write_to_nodes(update_data):
            tconst = update_data['tconst']
            new_data = update_data['data']
            
            try:
                start_time = time.time()
                result = self.replication_manager.update_title(tconst, new_data, isolation_level)
                end_time = time.time()
                
                with lock:
                    results[tconst] = {
                        'success': result['success'],
                        'write_time': end_time - start_time,
                        'results': result['results']
                    }
            except Exception as e:
                with lock:
                    results[tconst] = {'success': False, 'error': str(e)}
        
        # start concurrent writes
        for update in updates:
            t = threading.Thread(target=write_to_nodes, args=(update,))
            threads.append(t)
            t.start()
        
        # wait for all writes to complete
        for t in threads:
            t.join()
        
        return {
            'test': 'concurrent_write',
            'isolation_level': isolation_level,
            'results': results
        }
    
    def simulate_failure(self, scenario):
        """
        Guide for simulating failure scenarios
        These require manual intervention (stopping containers)
        """
        if scenario == 'fragment_to_central':
            return {
                'scenario': 'Case #1: Central node failure during replication',
                'steps': [
                    '1. Stop node1 container: docker stop node1-central',
                    '2. Insert a new title via POST /title',
                    '3. Check pending queue: GET /recovery/status',
                    '4. Restart node1: docker start node1-central',
                    '5. Trigger recovery: POST /test/failure/central-recovery'
                ],
                'expected': 'Insert succeeds on fragment, queued for central',
                'current_queue_size': self.replication_manager.recovery_handler.get_pending_count()
            }
        
        elif scenario == 'central_recovery':
            return {
                'scenario': 'Case #2: Central node recovery',
                'action': 'Triggering recovery for node1...',
                'result': self.replication_manager.recover_node('node1')
            }
        
        elif scenario == 'central_to_fragment':
            return {
                'scenario': 'Case #3: Fragment node failure during replication',
                'steps': [
                    '1. Stop node2 container: docker stop node2-movies',
                    '2. Update a movie via PUT /title/tt0000001',
                    '3. Check pending queue: GET /recovery/status',
                    '4. Restart node2: docker start node2-movies',
                    '5. Trigger recovery: POST /test/failure/fragment-recovery with {"node": "node2"}'
                ],
                'expected': 'Update succeeds on central, queued for fragment',
                'current_queue_size': self.replication_manager.recovery_handler.get_pending_count()
            }
        
        elif scenario == 'fragment_recovery':
            node = request.json.get('node', 'node2') if hasattr(self, 'request') else 'node2'
            return {
                'scenario': f'Case #4: Fragment node ({node}) recovery',
                'action': f'Triggering recovery for {node}...',
                'result': self.replication_manager.recover_node(node)
            }
        
        else:
            return {
                'error': 'Unknown scenario',
                'valid_scenarios': [
                    'fragment_to_central',
                    'central_recovery',
                    'central_to_fragment',
                    'fragment_recovery'
                ]
            }