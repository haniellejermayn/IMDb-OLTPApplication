import logging
import json
from .transaction_logger import TransactionLogger
from .recovery_handler import RecoveryHandler
from .concurrency_tester import ConcurrencyTester

logger = logging.getLogger(__name__)

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.transaction_logger = TransactionLogger(db_manager)
        self.recovery_handler = RecoveryHandler(db_manager, self.transaction_logger)
        self.concurrency_tester = ConcurrencyTester(db_manager, self)
    
    def _get_primary_node(self, title_type):
        return 'node2' if title_type == 'movie' else 'node3'
    
    def insert_title(self, data):
        """
        Insert flow:
        1. Write to primary fragment (Node 2 or 3)
        2. Replicate to central (Node 1)
        3. Log everything to source node's transaction_log
        """
        tconst = data.get('tconst')
        title_type = data.get('title_type')
        
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
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
        
        # Write to primary fragment
        result_primary = self.db.execute_query(primary_node, query, params)
        results[primary_node] = result_primary
        
        if not result_primary['success']:
            # Primary write failed - REAL failure
            logger.error(
                f"✗ PRIMARY WRITE FAILED on {primary_node} for {tconst}: "
                f"{result_primary.get('error')}"
            )
            return {
                'success': False,
                'error': result_primary.get('error'),
                'message': f'Insert failed at primary node {primary_node}',
                'results': results
            }
        
        logger.info(f"✓ INSERT to PRIMARY {primary_node} succeeded for {tconst}")
        
        # Write to central node
        result_central = self.db.execute_query(central_node, query, params)
        results[central_node] = result_central
        
        if result_central['success']:
            # Success - log as completed
            self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='INSERT',
                record_id=tconst,
                query=query,
                params=params,
                status='SUCCESS'
            )
            logger.info(f"✓ INSERT replicated to CENTRAL {central_node} for {tconst}")
            
            return {
                'success': True,
                'primary_node': primary_node,
                'replicated_to_central': True,
                'results': results,
                'message': f'Insert committed to {primary_node} and replicated to {central_node}'
            }
        else:
            # Replication failed - log as PENDING for retry
            transaction_id = self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='INSERT',
                record_id=tconst,
                query=query,
                params=params,
                status='PENDING',
                error_msg=result_central.get('error')
            )
            
            logger.warning(
                f"⚠ REPLICATION FAILED: {primary_node} → {central_node} for {tconst}. "
                f"Transaction {transaction_id} logged as PENDING for automatic retry."
            )
            
            return {
                'success': True,  # Primary succeeded
                'primary_node': primary_node,
                'replicated_to_central': False,
                'transaction_id': transaction_id,
                'results': results,
                'message': (
                    f'Insert committed to {primary_node}. '
                    f'Replication to {central_node} failed but queued for retry.'
                )
            }
    
    def update_title(self, tconst, data, isolation_level='READ COMMITTED'):
        """Update title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        # Build query
        set_clauses = []
        params = []
        
        for key, value in data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        params.append(tconst)
        query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
        
        results = {}
        
        # Update primary fragment
        result_primary = self.db.execute_query(primary_node, query, tuple(params), isolation_level)
        results[primary_node] = result_primary
        
        if not result_primary['success']:
            logger.error(f"✗ PRIMARY UPDATE FAILED on {primary_node} for {tconst}")
            return {
                'success': False,
                'error': result_primary.get('error'),
                'message': f'Update failed at primary node {primary_node}',
                'results': results
            }
        
        logger.info(f"✓ UPDATE to PRIMARY {primary_node} succeeded for {tconst}")
        
        # Replicate to central
        result_central = self.db.execute_query(central_node, query, tuple(params), isolation_level)
        results[central_node] = result_central
        
        if result_central['success']:
            self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='UPDATE',
                record_id=tconst,
                query=query,
                params=tuple(params),
                status='SUCCESS'
            )
            logger.info(f"✓ UPDATE replicated to CENTRAL {central_node} for {tconst}")
            
            return {
                'success': True,
                'primary_node': primary_node,
                'replicated_to_central': True,
                'results': results,
                'message': f'Update committed and replicated'
            }
        else:
            transaction_id = self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='UPDATE',
                record_id=tconst,
                query=query,
                params=tuple(params),
                status='PENDING',
                error_msg=result_central.get('error')
            )
            
            logger.warning(
                f"⚠ UPDATE REPLICATION FAILED for {tconst}. "
                f"Transaction {transaction_id} queued for retry."
            )
            
            return {
                'success': True,
                'primary_node': primary_node,
                'replicated_to_central': False,
                'transaction_id': transaction_id,
                'results': results,
                'message': f'Update committed to {primary_node}, replication queued'
            }
    
    def delete_title(self, tconst):
        """Delete title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        primary_node = self._get_primary_node(title_type)
        central_node = 'node1'
        
        query = "DELETE FROM titles WHERE tconst = %s"
        params = (tconst,)
        
        results = {}
        
        # Delete from primary
        result_primary = self.db.execute_query(primary_node, query, params)
        results[primary_node] = result_primary
        
        if not result_primary['success']:
            logger.error(f"✗ PRIMARY DELETE FAILED on {primary_node} for {tconst}")
            return {
                'success': False,
                'error': result_primary.get('error'),
                'message': f'Delete failed at primary node {primary_node}',
                'results': results
            }
        
        logger.info(f"✓ DELETE from PRIMARY {primary_node} succeeded for {tconst}")
        
        # Replicate deletion to central
        result_central = self.db.execute_query(central_node, query, params)
        results[central_node] = result_central
        
        if result_central['success']:
            self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='DELETE',
                record_id=tconst,
                query=query,
                params=params,
                status='SUCCESS'
            )
            logger.info(f"✓ DELETE replicated to CENTRAL {central_node} for {tconst}")
            
            return {
                'success': True,
                'primary_node': primary_node,
                'replicated_to_central': True,
                'results': results,
                'message': f'Delete committed and replicated'
            }
        else:
            transaction_id = self.transaction_logger.log_replication(
                source_node=primary_node,
                target_node=central_node,
                operation_type='DELETE',
                record_id=tconst,
                query=query,
                params=params,
                status='PENDING',
                error_msg=result_central.get('error')
            )
            
            logger.warning(
                f"⚠ DELETE REPLICATION FAILED for {tconst}. "
                f"Transaction {transaction_id} queued for retry."
            )
            
            return {
                'success': True,
                'primary_node': primary_node,
                'replicated_to_central': False,
                'transaction_id': transaction_id,
                'results': results,
                'message': f'Delete committed to {primary_node}, replication queued'
            }
    
    # === Delegation methods for recovery and concurrency testing ===
    def recover_node(self, node_name):
        return self.recovery_handler.recover_node(node_name)
    
    def get_pending_replications(self):
        return self.recovery_handler.get_pending_summary()
    
    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        return self.concurrency_tester.test_concurrent_reads(tconst, isolation_level)
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        return self.concurrency_tester.test_concurrent_writes(updates, isolation_level)
    
    def simulate_failure(self, scenario):
        return self.concurrency_tester.simulate_failure(scenario)