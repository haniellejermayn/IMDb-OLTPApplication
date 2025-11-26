import logging
from .transaction_logger import TransactionLogger
from .recovery_handler import RecoveryHandler
from .concurrency_tester import ConcurrencyTester

logger = logging.getLogger(__name__)

# TODO: Optional lightweight conflict detection
# Compare existing DB values with intended update before writing
# Log a warning if there’s a difference to track potential concurrent updates

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.transaction_logger = TransactionLogger(db_manager)
        self.recovery_handler = RecoveryHandler(db_manager, self.transaction_logger)
        self.concurrency_tester = ConcurrencyTester(db_manager, self)
    
    def insert_title(self, data):
        """Insert title"""
        tconst = data.get('tconst')
        title_type = data.get('title_type')
        
        # node2 if movie, node3 if non-movie
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
        
        # insert to central
        result_central = self.db.execute_query('node1', query, params)
        results['node1'] = result_central
        
        if result_central['success']:
            self.transaction_logger.log_transaction('node1', 'INSERT', tconst, 'SUCCESS', None)
            
            # replicate to fragment
            result_frag = self.db.execute_query(target_node, query, params)
            results[target_node] = result_frag
            
            if result_frag['success']:
                self.transaction_logger.log_transaction(target_node, 'INSERT', tconst, 'SUCCESS', None)
            else:
                # Case #1: Failed to replicate to fragment (queue for recovery)
                self.transaction_logger.log_transaction(target_node, 'INSERT', tconst, 'FAILED', result_frag.get('error'))
                self.recovery_handler.queue_failed_replication(target_node, 'INSERT', query, params, tconst)
                logger.warning(f"Failed to replicate INSERT to {target_node} for {tconst}, queued for retry")
        else:
            self.transaction_logger.log_transaction('node1', 'INSERT', tconst, 'FAILED', result_central.get('error'))
        
        return {
            'success': result_central['success'],
            'replicated_to_fragment': results.get(target_node, {}).get('success', False),
            'results': results
        }
    
    def update_title(self, tconst, data, isolation_level='READ COMMITTED'):
        """Update title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        # update query
        set_clauses = []
        params = []
        
        for key, value in data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        params.append(tconst)
        
        query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
        
        results = {}
        
        # update central first (node1)
        result1 = self.db.execute_query('node1', query, params, isolation_level)
        results['node1'] = result1
        
        if result1['success']:
            self.transaction_logger.log_transaction('node1', 'UPDATE', tconst, 'SUCCESS', None)
            
            # replicate to fragment
            result_frag = self.db.execute_query(target_node, query, params, isolation_level)
            results[target_node] = result_frag
            
            if result_frag['success']:
                self.transaction_logger.log_transaction(target_node, 'UPDATE', tconst, 'SUCCESS', None)
            else:
                # Case #3: Failed to replicate to fragment node
                self.transaction_logger.log_transaction(target_node, 'UPDATE', tconst, 'FAILED', result_frag.get('error'))
                self.recovery_handler.queue_failed_replication(target_node, 'UPDATE', query, params, tconst)
                logger.warning(f"Failed to replicate UPDATE to {target_node} for {tconst}, queued for retry")
        else:
            self.transaction_logger.log_transaction('node1', 'UPDATE', tconst, 'FAILED', result1.get('error'))
        
        return {
            'success': result1['success'],
            'replicated_to_fragment': results.get(target_node, {}).get('success', False),
            'results': results
        }
    
    def delete_title(self, tconst):
        """Delete title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        query = "DELETE FROM titles WHERE tconst = %s"
        
        results = {}
        
        # delete from central first (node1)
        result1 = self.db.execute_query('node1', query, (tconst,))
        results['node1'] = result1
        
        if result1['success']:
            self.transaction_logger.log_transaction('node1', 'DELETE', tconst, 'SUCCESS', None)
            
            # replicate deletion to fragment
            result_frag = self.db.execute_query(target_node, query, (tconst,))
            results[target_node] = result_frag
            
            if result_frag['success']:
                self.transaction_logger.log_transaction(target_node, 'DELETE', tconst, 'SUCCESS', None)
            else:
                self.transaction_logger.log_transaction(target_node, 'DELETE', tconst, 'FAILED', result_frag.get('error'))
                self.recovery_handler.queue_failed_replication(target_node, 'DELETE', query, (tconst,), tconst)
        else:
            self.transaction_logger.log_transaction('node1', 'DELETE', tconst, 'FAILED', result1.get('error'))
        
        return {
            'success': result1['success'],
            'replicated_to_fragment': results.get(target_node, {}).get('success', False),
            'results': results
        }
    
    # ==== delegation methods for recovery and concurrency testing ====

    def recover_node(self, node_name):
        return self.recovery_handler.recover_node(node_name)
    
    def get_pending_replications(self):
        return {
            'pending_count': self.recovery_handler.get_pending_count(),
            'message': 'Transactions waiting for node recovery'
        }

    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        """Delegate to concurrency tester"""
        return self.concurrency_tester.test_concurrent_reads(tconst, isolation_level)
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        """Delegate to concurrency tester"""
        return self.concurrency_tester.test_concurrent_writes(updates, isolation_level)
    
    def simulate_failure(self, scenario):
        """Delegate to concurrency tester"""
        return self.concurrency_tester.simulate_failure(scenario)
    